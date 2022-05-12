#!/usr/bin/env python

import click
from pkg_resources import resource_string
from typing import Any, Dict, List, Optional
from jinja2 import Template  # type: ignore
from .utils import (
    load_mapping,
    run,
    Partitions,
    TargetTables,
    DEFAULT_DATETIME_FORMAT,
    DEFAULT_DATE_FORMAT,
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
        default_partitions: str,
        excluded_tables: TargetTables = None,
        datetime_format: Optional[str] = None,
        date_format: Optional[str] = None,
        decimal_format: Optional[str] = None,
        where_condition: Optional[str] = None,
        dry_run: bool = False,
        debug: bool = False,
    ) -> None:
        self.flow = flow
        self.athena_location = athena_location
        self.default_partitions = default_partitions
        self.mapping = load_mapping(flow)
        self.excluded_tables = excluded_tables or []
        self.datetime_format = datetime_format or DEFAULT_DATETIME_FORMAT
        self.date_format = date_format or DEFAULT_DATE_FORMAT
        self.decimal_format = decimal_format or None
        self.where_condition = where_condition or None
        self.dry_run = dry_run
        self.debug = debug

    @property
    def table_names(self) -> List[str]:
        return sorted(x for x in self.mapping.keys() if x not in self.excluded_tables)

    def table_columns(self, table: str) -> List[str]:
        return self.mapping[table]["columns"]  # type: ignore

    def render(self, template: str, table: str, **kargs: Dict[str, Any]) -> str:
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
        return Template(template).render(**data)  # type: ignore

    def sql_batch_to_datalake(self, table: str, partitions: Partitions) -> str:
        partitions = dict((k, v.replace("'", "''")) for k, v in partitions.items() if v)
        template = resource_string("dbting.templates", "sql_batch_to_datalake.sql").decode("utf-8")
        return self.render(template, table=table, partitions=partitions)

    def prepare_location(self, location: str, partitions: Partitions) -> str:
        # Concatenate the location and partitions, return an s3 path
        return location + "/".join(["{}={}".format(k, v) for k, v in partitions.items() if v])

    def delete_from_s3_datalake(self, table: str, partitions: Partitions) -> None:
        # Clean the target datalake
        table_def = self.mapping[table]
        table_partitions = table_def.get("partitions", self.default_partitions)
        path = self.prepare_location(table_def["target_location"], partitions if table_partitions else {})
        click.secho("Delete {}".format(path))
        if not self.dry_run:
            run(["aws", "s3", "rm", "--recursive", path], debug=self.debug)

    def add_source_partition(self, table: str, partitions: Partitions) -> None:
        # Add source partion
        table_def = self.mapping[table]
        # Check partition parameters
        table_partitions = table_def.get("partitions", self.default_partitions)
        if isinstance(table_partitions, str):
            table_partitions = [x.strip() for x in table_partitions.split(",")]
        for partition in table_partitions:
            if partitions.get(partition) is None:
                raise BatchToDatalakeException("Missing partition {}".format(partition))
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
        if not self.dry_run:
            run(command, debug=self.debug)

    def athena_batch_to_datalake(self, table: str, partitions: Partitions) -> None:
        table_def = self.mapping[table]
        table_partitions = table_def.get("partitions", self.default_partitions)
        partitions = partitions if table_partitions else {}
        sql = self.sql_batch_to_datalake(table=table, partitions=partitions)
        context = {"Database": table_def["target_schema"]}
        qm = QueryManager(athena_location=self.athena_location, dry_run=self.dry_run, debug=self.debug)
        qm.execute_query(sql, context)
        errors = qm.wait_executions()
        if errors:
            raise BatchToDatalakeException(errors[0])


def batch_to_datalake(
    flow: str,
    table: str,
    athena_location: str,
    default_partitions: str,
    partitions: Partitions,
    datetime_format: Optional[str] = None,
    date_format: Optional[str] = None,
    decimal_format: Optional[str] = None,
    where_condition: Optional[str] = None,
    add_source_partition: bool = True,
    dry_run: bool = False,
    debug: bool = False,
) -> None:
    "Convert raw data to columnar formats"
    if table is None:
        tables = load_mapping(flow).keys()
        for table in tables:
            batch_to_datalake(
                flow=flow,
                table=table,
                athena_location=athena_location,
                default_partitions=default_partitions,
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
            default_partitions=default_partitions,
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
