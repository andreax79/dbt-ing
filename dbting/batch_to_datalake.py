#!/usr/bin/env python

import os
import pprint
import click
from pkg_resources import resource_string
from typing import Optional
from jinja2 import Template
from .utils import (
    load_mapping,
    run,
    DEFAULT_DATETIME_FORMAT,
    DEFAULT_DATE_FORMAT,
    DEFAULT_PARTITIONS,
)
from .qm import QueryManager


__all__ = ["batch_to_datalake", "BatchToDatalakeException"]


class BatchToDatalakeException(Exception):
    pass


class BatchToDatalake(object):
    def __init__(
        self,
        flow: str,
        athena_location: str,
        excluded_tables=None,
        datetime_format: Optional[str] = None,
        date_format: Optional[str] = None,
        decimal_format: Optional[str] = None,
        where_condition: Optional[str] = None,
        dry_run: bool = False,
        debug: bool = False,
    ) -> None:
        self.flow = flow
        self.athena_location = athena_location
        self.mapping = load_mapping(flow)
        self.excluded_tables = excluded_tables or []
        self.datetime_format = datetime_format or DEFAULT_DATETIME_FORMAT
        self.date_format = date_format or DEFAULT_DATE_FORMAT
        self.decimal_format = decimal_format or None
        self.where_condition = where_condition or None
        self.dry_run = dry_run
        self.debug = debug

    @property
    def table_names(self):
        return sorted(x for x in self.mapping.keys() if x not in self.excluded_tables)

    def table_columns(self, table):
        return self.mapping[table]["columns"]

    def render(self, template, table, **kargs):
        table_def = self.mapping[table]
        columns = self.table_columns(table)
        data = {
            "flow": self.flow,
            "columns": columns,
            "datetime_format": table_def.get("datetime_format") or self.datetime_format,
            "date_format": table_def.get("date_format") or self.date_format,
            "decimal_format": table_def.get("decimal_format") or self.decimal_format,
            "source_table": table_def["source_table"],
            "source_schema": table_def["source_schema"],
            "target_table": table_def["target_table"],
            "target_schema": table_def["target_schema"],
            "source_location": table_def["source_location"].rstrip("/"),
            "target_location": table_def["target_location"].rstrip("/"),
            "where_condition": table_def.get("where_condition") or self.where_condition,
            "remove_quotes": table_def.get("remove_quotes") or None,
        }
        data.update(kargs)
        return Template(template).render(**data)

    def sql_batch_to_datalake(self, table: str, partitions, source_format: str) -> None:
        partitions = dict((k, v.replace("'", "''")) for k, v in partitions.items() if v)
        template = resource_string("dbting.templates", "sql_batch_to_datalake.sql").decode("utf-8")
        return self.render(template, table=table, partitions=partitions)

    def prepare_location(self, location, partitions):
        # Concatenate the location and partitions, return an s3 path
        return location + "/".join(["{}={}".format(k, v) for k, v in partitions.items() if v])

    def delete_from_s3_datalake(self, table, partitions):
        # Clean the target datalakeChiappopeloso
        table_def = self.mapping[table]
        table_partitions = table_def.get("partitions", DEFAULT_PARTITIONS)
        path = self.prepare_location(table_def["target_location"], partitions if table_partitions else {})
        click.echo("Delete {}".format(path))
        if not self.dry_run:
            run(["aws", "s3", "rm", "--recursive", path])

    def add_source_partition(self, table, partitions):
        # Add source partion
        table_def = self.mapping[table]
        # Check partition parameters
        table_partitions = table_def.get("partitions", DEFAULT_PARTITIONS)
        if isinstance(table_partitions, str):
            table_partitions = [x.strip() for x in table_partitions.split(",")]
        for partition in table_partitions:
            if partitions.get(partition) is None:
                raise Exception("Missing partition {}".format(partition))
        if not any(partitions.values()):
            return
        # Add partition
        command = [
            "gluettalax",
            "add_partition",
            table_def["source_schema"],
            table_def["source_table"],
        ]
        command.extend(["--{}={}".format(k, v) for k, v in partitions.items() if v is not None])
        click.echo(command)
        if not self.dry_run:
            run(command)

    def athena_batch_to_datalake(self, table, partitions):
        table_def = self.mapping[table]
        table_partitions = table_def.get("partitions", DEFAULT_PARTITIONS)
        partitions = partitions if table_partitions else {}
        sql = self.sql_batch_to_datalake(
            table=table,
            partitions=partitions,
            source_format=table_def.get("source_format"),
        )
        if self.debug:
            click.echo("-" * 80)
            click.echo(sql)
            click.echo("-" * 80)
        if not self.dry_run:
            context = {"Database": table_def["target_schema"]}
            qm = QueryManager(athena_location=self.athena_location)
            qm.execute_query(sql, context)
            errors = qm.wait_executions()
            if errors:
                raise BatchToDatalakeException(errors[0])


def batch_to_datalake(
    flow: str,
    table: str,
    athena_location: str,
    partitions,
    datetime_format: Optional[str] = None,
    date_format: Optional[str] = None,
    decimal_format: Optional[str] = None,
    where_condition: Optional[str] = None,
    add_source_partition: bool = True,
    dry_run: bool = False,
    debug: bool = False,
) -> None:
    if table is None:
        tables = load_mapping(flow).keys()
        for table in tables:
            batch_to_datalake(
                flow=flow,
                table=table,
                athena_location=athena_location,
                partitions=partitions,
                datetime_format=datetime_format,
                date_format=date_format,
                decimal_format=decimal_format,
                where_condition=where_condition,
                add_source_partition=add_source_partition,
                dry_run=dry_run,
                debug=debug,
            )
    else:
        b2d = BatchToDatalake(
            flow=flow,
            athena_location=athena_location,
            datetime_format=datetime_format,
            date_format=date_format,
            decimal_format=decimal_format,
            where_condition=where_condition,
            dry_run=dry_run,
            debug=debug,
        )
        if add_source_partition:
            b2d.add_source_partition(table, partitions)
        b2d.delete_from_s3_datalake(table, partitions)
        b2d.athena_batch_to_datalake(table, partitions)
