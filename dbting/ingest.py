#!/usr/bin/env python3

import os
import os.path
import csv
import sys
import click
import json
from typing import Dict
from openpyxl import load_workbook  # type: ignore
from .external_tables import create_external_tables, drop_external_tables, repair_external_tables
from .utils import load_mapping, run, Config, TargetTables
from .xlsx_to_json_mapping import xlsx_to_json_mapping, MappingException
from sharepointcli.cli import main as spo  # type: ignore


def index_xlsx_to_csv(source: str, target: str, worksheet: int = 0) -> None:
    "Convert an xlsx file into a csv"
    wb = load_workbook(source)
    ws = wb.worksheets[worksheet]
    with open(target, "w") as f:
        for row in ws.values:
            if len(row) > 1:
                flow = (row[0] or "").strip().lower()
                path = (row[1] or "").strip()
                f.write("{flow};{path}\n".format(flow=flow, path=path))


def get_flow_index(sharepoint_url: str, config: Config) -> Dict[str, str]:
    "Download the flow index"
    index_xlsx = os.path.join(config["INGESTION_PATH"], config["FLOW_INDEX_XLSX"].replace(" ", "_"))
    index_csv = os.path.join(
        config["INGESTION_PATH"],
        config["FLOW_INDEX_XLSX"].replace(" ", "_").replace(".xlsx", ".csv"),
    )

    # Download and convert flows index
    index_url = os.path.join(sharepoint_url, config["FLOW_INDEX_XLSX"])
    os.makedirs(config["INGESTION_PATH"], exist_ok=True)
    if spo(["cp", index_url, index_xlsx]) != 0:
        click.secho("error downloading flow index", fg="red")
        sys.exit(1)
    index_xlsx_to_csv(index_xlsx, index_csv)
    os.unlink(index_xlsx)

    # Read flows index
    urls: Dict[str, str] = {}
    with open(index_csv) as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for i, row in enumerate(reader):
            if i > 0 and len(row) >= 2:
                urls[row[0]] = row[1]
    return urls


def get_flow_url(sharepoint_url: str, flow: str, config: Config) -> str:
    "Get the url of the mapping file for a given flow"
    urls = get_flow_index(sharepoint_url, config)
    flow_url = urls.get(flow)
    if not flow_url:
        click.secho("{flow} - Not found".format(flow=flow), fg="red")
        sys.exit(1)
    return config["SHAREPOINT__BASE_URL"] + flow_url  # type: ignore


def get_flow_xlsx(flow: str, download: bool, config: Config) -> str:
    "Return the mapping file path for a given flow"
    # Sharepoint download
    flow_xlsx = os.path.join(config["INGESTION_PATH"], flow + ".xlsx")
    sharepoint_url = config.get("SHAREPOINT__BASE_URL")
    if download and sharepoint_url:
        flow_url = get_flow_url(sharepoint_url, flow, config)
        if spo(["cp", flow_url, flow_xlsx]) != 0:
            click.secho("error downloading flow mapping", fg="red")
            sys.exit(1)
    # Check if flow xlsx exists
    if not os.path.exists(flow_xlsx):
        click.secho("{flow_xlsx} does not exists".format(flow_xlsx=flow_xlsx), fg="red")
        sys.exit(1)
    return flow_xlsx


def ingest_model(
    flow: str,
    create_tables: bool,
    repair_tables: bool,
    download: bool,
    field_delimiter: str,
    include_target_tables: TargetTables,
    config: Config,
    dry_run: bool = False,
    debug: bool = False,
) -> None:
    "Parse flow mapping and generate source models"
    flow = flow.lower()

    # Get flow XLSX
    flow_xlsx = get_flow_xlsx(flow=flow, download=download, config=config)

    # XLS parsing
    click.secho("{flow} - Parse XLSX".format(flow=flow))
    try:
        xlsx_to_json_mapping(
            filename=flow_xlsx,
            flow=flow,
            config=config,
            field_delimiter=field_delimiter,
        )
    except MappingException as ex:
        click.secho("Error: %s" % ex, fg="red")
        sys.exit(1)

    if create_tables:
        if True:  # Athena TODO
            click.secho("{flow} - Create external tables".format(flow=flow))
            drop_external_tables(
                flow=flow,
                athena_location=config["S3__ATHENA_LOCATION"],
                include_target_tables=include_target_tables,
                dry_run=dry_run,
                debug=debug,
            )
            create_external_tables(
                flow=flow,
                athena_location=config["S3__ATHENA_LOCATION"],
                include_target_tables=include_target_tables,
                dry_run=dry_run,
                debug=debug,
            )
        else:
            # Check tables argument
            tables = load_mapping(flow, include_target_tables)
            # Create dbt external tables
            click.secho("{flow} - Create external tables".format(flow=flow))
            args = {"flow": flow, "test": True}
            if include_target_tables:
                source_node_names = []
                for table in tables.values():
                    source_node_names.append("{batch_profile}__{source_table}".format(**table))
                    source_node_names.append("{datalake_profile}__{target_table}".format(**table))
                args["source_node_names"] = source_node_names
            if not dry_run:
                run(
                    [
                        "dbt",
                        "--partial-parse",
                        "run-operation",
                        "create_external_tables",
                        "--args",
                        json.dumps(args),
                    ],
                    debug=debug,
                )
        if repair_tables:
            # Repair external tables
            click.secho("{flow} - Repair external tables".format(flow=flow))
            repair_external_tables(
                flow=flow,
                athena_location=config["S3__ATHENA_LOCATION"],
                include_target_tables=include_target_tables,
                dry_run=dry_run,
                debug=debug,
            )
