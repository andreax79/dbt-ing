#!/usr/bin/env python3

import os
import os.path
import sys
import json
import click
from typing import Dict, List, Optional
from .utils import (
    click_partition_options,
    prepare_partitions,
    load_mapping,
    run,
    load_config,
    decrypt_config_passwords,
    write_config_file,
    VERSION,
)
from .batch_to_datalake import batch_to_datalake, BatchToDatalakeException
from .ingest import ingest_model
from sharepointcli.cli import main as spo  # type: ignore

__all__ = ["cli"]


config = load_config()


@click.group()
@click.option("--debug/--no-debug", default=False)
@click.version_option(version=VERSION)
@click.pass_context
def cli(
    ctx: click.Context,
    debug: bool,
) -> None:
    ctx.obj = {
        "debug": debug,
    }


# -----------------------------------------------------------------------------
# Test


@cli.command()
@click.option("--flow", required=True)
@click_partition_options(config)
@click.pass_context
def test(ctx: click.Context, flow: str, **partitions_args: Dict[str, str]) -> None:
    "Run flow test"
    flow = flow.lower()
    partitions = prepare_partitions(partitions_args, config)
    run(
        [
            "dbt",
            "test",
            "--models",
            "models/sources/datalake/%s" % flow,
            "--vars",
            json.dumps(partitions),
        ],
        debug=ctx.obj["debug"],
    )


# -----------------------------------------------------------------------------
# Upload


@cli.command(context_settings={"ignore_unknown_options": True})
@click.option("--flow", required=True)
@click.option("--table", required=True)
@click.argument("files", required=True, nargs=-1, type=click.Path())
@click_partition_options(config)
@click.pass_context
def upload(ctx: click.Context, flow: str, table: str, files: List[str], **partitions_args: Dict[str, str]) -> None:
    "Upload raw files to batch location"
    flow = flow.lower()
    try:
        mapping = load_mapping(flow)
    except Exception:
        click.secho("flow {} not found".format(flow), fg="red")
        sys.exit(1)
    try:
        table_mapping = mapping[table]
    except KeyError:
        click.secho("table {} not found".format(table), fg="red")
        sys.exit(1)
    partitions = prepare_partitions(partitions_args, config)
    source_location = table_mapping.get("source_location")
    if not source_location:
        source_location = os.path.join(table_mapping.get("batch_location"), flow)  # type: ignore
    path = source_location + "/".join(["{}={}".format(k, v) for k, v in partitions.items() if v]) + "/"
    for filename in files:
        run(["aws", "s3", "cp", filename, path], debug=ctx.obj["debug"])


# -----------------------------------------------------------------------------
# Ingest


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
    ctx: click.Context,
    flow: str,
    create_tables: bool,
    repair_tables: bool,
    download: bool,
    field_delimiter: str,
    include_target_tables: List[str],
    dry_run: bool,
) -> None:
    ingest_model(
        flow=flow,
        create_tables=create_tables,
        repair_tables=repair_tables,
        download=download,
        field_delimiter=field_delimiter,
        include_target_tables=include_target_tables,
        dry_run=dry_run,
        debug=ctx.obj["debug"],
        config=config,
    )


# -----------------------------------------------------------------------------


@cli.command()
@click.option("--flow", required=True)
@click.option("--table", required=False)
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
@click_partition_options(config)
@click.pass_context
def datalake(
    ctx: click.Context,
    flow: str,
    table: str,
    datetime_format: Optional[str],
    date_format: Optional[str],
    decimal_format: Optional[str],
    where_condition: Optional[str],
    add_source_partition: bool,
    dry_run: bool,
    **partitions_args: Dict[str, str],
) -> None:
    "Convert raw data to columnar formats (batch to datalake)"
    flow = flow.lower()

    try:
        batch_to_datalake(
            flow=flow,
            table=table,
            athena_location=config["S3__ATHENA_LOCATION"],
            default_partitions=config["DEFAULT_PARTITIONS"],
            partitions=prepare_partitions(partitions_args, config),
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
def doc(ctx: click.Context, upload: bool) -> None:
    "Generate and upload documentation"
    # Generate doc
    run(["dbt", "docs", "generate"])
    if upload and config.get("S3__DOC_LOCATION"):
        # Upload documentation to S3 bucket
        run(
            ["aws", "s3", "sync", "--delete", "--acl", "public-read", "target/", config["S3__DOC_LOCATION"]],
            debug=ctx.obj["debug"],
        )


# -----------------------------------------------------------------------------
# Setup


@cli.command()
@click.option("--update/--no-update", default=True, help="Update dbt dependencies")
@click.option("--auth/--no-auth", default=True)
@click.option("--target", default="dev", help="Default dbt environment")
@click.pass_context
def setup(ctx: click.Context, update: bool, auth: bool, target: str) -> None:
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
            click.secho("{k}: {v}".format(k=k, v=v))
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
