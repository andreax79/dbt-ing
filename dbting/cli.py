#!/usr/bin/env python3

import os
import os.path
import csv
import sys
import click
import json
from openpyxl import load_workbook
from .external_tables import create_external_tables, drop_external_tables, repair_external_tables
from .utils import load_mapping, run, load_config, decrypt_config_passwords, write_config_file, VERSION
from .xlsx_to_json_mapping import xlsx_to_json_mapping, MappingException
from .batch_to_datalake import batch_to_datalake, BatchToDatalakeException
from sharepointcli.cli import main as spo

__all__ = ["cli"]


config = load_config()


@click.group()
@click.option("--debug/--no-debug", default=False)
@click.option("--batch_profile", default=config["DBT__BATCH_PROFILE"])
@click.option("--datalake_profile", default=config["DBT__DATALAKE_PROFILE"])
@click.option("--batch_location", default=config["S3__BATCH_LOCATION"])
@click.option("--datalake_location", default=config["S3__DATALAKE_LOCATION"])
@click.option("--source_schema", default=config["DB__SOURCE_SCHEMA"])
@click.option("--target_schema", default=config["DB__TARGET_SCHEMA"])
@click.option("--sharepoint_url", default=config["SHAREPOINT__BASE_URL"])
@click.version_option(version=VERSION)
@click.pass_context
def cli(
    ctx,
    debug,
    batch_profile,
    datalake_profile,
    batch_location,
    datalake_location,
    source_schema,
    target_schema,
    sharepoint_url,
):
    ctx.obj = {
        "debug": debug,
        "batch_profile": batch_profile,
        "datalake_profile": datalake_profile,
        "batch_location": batch_location,
        "datalake_location": datalake_location,
        "source_schema": source_schema,
        "target_schema": target_schema,
        "sharepoint_url": sharepoint_url,
    }


# -----------------------------------------------------------------------------
# Test


@cli.command()
@click.option("--flow", required=True)
@click.option("--source", required=False)
@click.option("--year", required=False)
@click.option("--month", required=False)
@click.option("--day", required=False)
@click.option("--hour", required=False)
@click.pass_context
def test(ctx, flow, source, year, month, day, hour):
    "Run flow test"
    flow = flow.lower()
    where = {
        "year": "%04d" % int(year),
        "month": "%02d" % int(month),
        "day": "%02d" % int(day),
    }
    if source is not None:
        where["source"] = source
    if hour is not None:
        where["hour"] = "%02d" % int(hour)
    run(
        [
            "dbt",
            "test",
            "--models",
            "models/sources/datalake/%s" % flow,
            "--vars",
            json.dumps(where),
        ]
    )


# -----------------------------------------------------------------------------
# Upload


@cli.command(context_settings={"ignore_unknown_options": True})
@click.option("--flow", required=True)
@click.option("--table", required=True)
@click.argument("files", required=True, nargs=-1, type=click.Path())
@click.option("--year", required=False)
@click.option("--month", required=False)
@click.option("--day", required=False)
@click.option("--hour", required=False)
@click.pass_context
def upload(ctx, flow, table, files, **kargs):
    "Upload raw files to batch location"
    flow = flow.lower()
    try:
        mapping = load_mapping(flow)
    except:
        click.secho("flow {} not found".format(flow), fg="red")
        sys.exit(1)
    try:
        table_mapping = mapping[table]
    except KeyError:
        click.secho("table {} not found".format(table), fg="red")
        sys.exit(1)
    source_location = table_mapping.get("source_location")
    if not source_location:
        source_location = os.path.join(table_mapping.get("batch_location"), flow)
    path = source_location + "/".join(["{}={}".format(k, v) for k, v in kargs.items() if v]) + "/"
    for filename in files:
        run(["aws", "s3", "cp", filename, path])


# -----------------------------------------------------------------------------
# Ingest


def index_xlsx_to_csv(source, target, worksheet=0):
    wb = load_workbook(source)
    ws = wb.worksheets[worksheet]
    with open(target, "w") as f:
        for row in ws.values:
            if len(row) > 1:
                flow = (row[0] or "").strip().lower()
                path = (row[1] or "").strip()
                f.write("{flow};{path}\n".format(flow=flow, path=path))


def get_flow_index(sharepoint_url: str):
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
    urls = {}
    with open(index_csv) as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for i, row in enumerate(reader):
            if i > 0 and len(row) >= 2:
                urls[row[0]] = row[1]
    return urls


def get_flow_url(sharepoint_url: str, flow: str) -> str:
    urls = get_flow_index(sharepoint_url)
    flow_url = urls.get(flow)
    if not flow_url:
        click.secho("{flow} - Not found".format(flow=flow), fg="red")
        sys.exit(1)
    return config["SHAREPOINT__BASE_URL"] + flow_url


def get_flow_xlsx(sharepoint_url: str, flow: str, download: bool) -> str:
    # Sharepoint download
    flow_xlsx = os.path.join(config["INGESTION_PATH"], flow + ".xlsx")
    if download and sharepoint_url:
        flow_url = get_flow_url(sharepoint_url, flow)
        if spo(["cp", flow_url, flow_xlsx]) != 0:
            click.secho("error downloading flow mapping", fg="red")
            sys.exit(1)
    # Check if flow xlsx exists
    if not os.path.exists(flow_xlsx):
        click.secho("{flow_xlsx} does not exists".format(flow_xlsx=flow_xlsx), fg="red")
        sys.exit(1)
    return flow_xlsx


@cli.command()
@click.option("--flow", required=True)
@click.option("--create-tables/--no-create-tables", default=False)
@click.option("--repair-tables/--no-repair-tables", default=True)
@click.option("--download/--no-download", default=True)
@click.option("--field_delimiter", default="|")
@click.option("--table", "-t", "include_target_tables", multiple=True, help="Tables to be created/repaired")
@click.option("--dry-run", is_flag=True, help="Perform a dry run", default=False)
@click.pass_context
def ingest(
    ctx,
    flow,
    create_tables,
    repair_tables,
    download,
    field_delimiter,
    include_target_tables,
    dry_run,
):
    "Parse flow mapping and generate source models"
    flow = flow.lower()

    # Get flow XLSX
    flow_xlsx = get_flow_xlsx(sharepoint_url=ctx.obj["sharepoint_url"], flow=flow, download=download)

    # XLS parsing
    click.echo("{flow} - Parse XLSX".format(flow=flow))
    try:
        xlsx_to_json_mapping(
            filename=flow_xlsx,
            flow=flow,
            batch_profile=ctx.obj["batch_profile"],
            datalake_profile=ctx.obj["datalake_profile"],
            batch_location=ctx.obj["batch_location"],
            datalake_location=ctx.obj["datalake_location"],
            source_schema=ctx.obj["source_schema"],
            target_schema=ctx.obj["target_schema"],
            field_delimiter=field_delimiter,
        )
    except MappingException as ex:
        click.secho("Error: %s" % ex, fg="red")
        sys.exit(1)

    if create_tables:
        if True:  # Athena TODO
            click.echo("{flow} - Create external tables".format(flow=flow))
            drop_external_tables(
                flow=flow,
                athena_location=config["S3__ATHENA_LOCATION"],
                include_target_tables=include_target_tables,
                dry_run=dry_run,
            )
            create_external_tables(
                flow=flow,
                athena_location=config["S3__ATHENA_LOCATION"],
                include_target_tables=include_target_tables,
                dry_run=dry_run,
            )
        else:
            # Check tables argument
            tables = load_mapping(flow, include_target_tables)
            # Create dbt external tables
            click.echo("{flow} - Create external tables".format(flow=flow))
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
                    ]
                )
        if repair_tables:
            # Repair external tables
            click.echo("{flow} - Repair external tables".format(flow=flow))
            repair_external_tables(
                flow=flow,
                athena_location=config["S3__ATHENA_LOCATION"],
                include_target_tables=include_target_tables,
                dry_run=dry_run,
            )


# -----------------------------------------------------------------------------


@cli.command()
@click.option("--flow", required=True)
@click.option("--table", required=False)
@click.option("--source", required=False)
@click.option("--year", required=False)
@click.option("--month", required=False)
@click.option("--day", required=False)
@click.option("--hour", required=False)
@click.option("--datetime_format", required=False)
@click.option("--date_format", required=False)
@click.option("--decimal_format", required=False)
@click.option("--where_condition", required=False, help="SQL where condition")
@click.option(
    "--add-source-partition/--no-add-source-partition",
    help="Add/don't add source partition",
    default=True,
)
@click.option("--dry-run", is_flag=True, help="Perform a dry run", default=False)
@click.pass_context
def datalake(
    ctx,
    flow,
    table,
    source,
    year,
    month,
    day,
    hour,
    datetime_format,
    date_format,
    decimal_format,
    where_condition,
    add_source_partition,
    dry_run,
):
    "Load data from batch to datalake"
    flow = flow.lower()

    try:
        batch_to_datalake(
            flow=flow,
            table=table,
            athena_location=config["S3__ATHENA_LOCATION"],
            partitions={
                "source": source if source is not None else None,
                "year": ("%04d" % int(year)) if year is not None else None,
                "month": ("%02d" % int(month)) if month is not None else None,
                "day": ("%02d" % int(day)) if day is not None else None,
                "hour": ("%02d" % int(hour)) if hour is not None else None,
            },
            datetime_format=datetime_format,
            date_format=date_format,
            decimal_format=decimal_format,
            where_condition=where_condition,
            add_source_partition=add_source_partition,
            dry_run=dry_run,
            debug=ctx.obj["debug"],
        )
    except BatchToDatalakeException as ex:
        click.secho("Error: %s" % ex, fg="red")
        sys.exit(1)


# -----------------------------------------------------------------------------
# Generate doc


@cli.command()
@click.option("--upload/--no-upload", default=True, help="Upload the generated documentation to S3 doc bucket")
@click.pass_context
def doc(ctx, upload):
    "Generate and upload documentation"
    # Generate doc
    run(["dbt", "docs", "generate"])
    if upload and config.get("S3__DOC_LOCATION"):
        # Upload documentation to S3 bucket
        run(["aws", "s3", "sync", "--delete", "--acl", "public-read", "target/", config["S3__DOC_LOCATION"]])


# -----------------------------------------------------------------------------
# Setup


@cli.command()
@click.option("--update/--no-update", default=True, help="Update dbt dependencies")
@click.option("--auth/--no-auth", default=True)
@click.option("--target", default="dev", help="Default dbt environment")
@click.pass_context
def setup(ctx, update, auth, target):
    "Setup"
    config["TARGET"] = target
    config["CWD"] = os.getcwd()
    # Update dependencies
    if update:
        if os.path.exists("./bin/activate"):  # Virtual env
            run(["pip3", "install", "--upgrade", "-r", "requirements.txt"])
        else:
            run(["pip3", "install", "--user", "--upgrade", "-r", "requirements.txt"])
    # Decrypt encrypted config passwords
    decrypt_config_passwords(config)
    if ctx.obj["debug"]:
        for k, v in sorted(config.items()):
            click.echo("{k}: {v}".format(k=k, v=v))
    # Render dbt config
    write_config_file(config, config["DBT_PROFILES"])
    # Render Sharepoint config
    if config["SHAREPOINT__HOST"]:
        write_config_file(config, config["SPO_CREDENTIALS"])
    # Update dbt dependencies
    if update:
        run(["dbt", "deps"])
    # Sharepoint authentication
    if auth and config["SHAREPOINT__HOST"]:
        spo(["authenticate", config["SHAREPOINT__HOST"]])


if __name__ == "__main__":
    cli()
