#!/usr/bin/env python

import boto3
import botocore
import sys
import click
from pkg_resources import resource_string
from jinja2 import Template
from .utils import load_mapping
from .qm import QueryManager

__all__ = ["create_external_tables", "drop_external_tables", "repair_external_tables"]


def repair_external_tables(flow, athena_location, include_target_tables=None, dry_run=False):
    "include_target_tables = list of target tables to be included, include all if empty"
    tables = set()
    mapping = load_mapping(flow, include_target_tables)
    for item in mapping.values():
        tables.add("{source_schema}.{source_table}".format(**item))  # source table
        tables.add("{target_schema}.{target_table}".format(**item))  # target table
    qm = QueryManager(athena_location=athena_location, dry_run=dry_run)
    for table in tables:
        context = {"Database": table.split(".")[0]}
        sql = "msck repair table {}".format(table)
        qm.execute_query(sql, context)
    qm.wait_executions()


def drop_external_tables(flow, athena_location, include_target_tables=None, dry_run=False):
    "Drop external tables include_target_tables = list of target tables to be included, include all if empty"
    mapping = load_mapping(flow, include_target_tables)
    qm = QueryManager(athena_location=athena_location, dry_run=dry_run)
    for table in mapping.values():
        if not table.get("source_location"):
            table["source_location"] = os.path.join(table.get("batch_location"), flow)
        # Batch
        context = {"Database": table["source_schema"]}
        qm.execute_template("drop_batch_table.sql", context, table)
        # Datalake
        context = {"Database": table["target_schema"]}
        qm.execute_template("drop_datalake_table.sql", context, table)
    qm.wait_executions()


def create_external_tables(flow, athena_location, include_target_tables=None, dry_run=False):
    "Create external tables include_target_tables = list of target tables to be included, include all if empty"
    mapping = load_mapping(flow, include_target_tables)
    qm = QueryManager(athena_location=athena_location, dry_run=dry_run)
    for table in mapping.values():
        if not table.get("source_location"):
            table["source_location"] = os.path.join(table.get("batch_location"), flow)
        # Batch
        context = {"Database": table["source_schema"]}
        qm.execute_template("create_batch_table.sql", context, table)
        # Datalake
        context = {"Database": table["target_schema"]}
        qm.execute_template("create_datalake_table.sql", context, table)
    qm.wait_executions()
