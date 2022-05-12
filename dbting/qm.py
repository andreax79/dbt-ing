#!/usr/bin/env python

import os
import boto3  # type: ignore
import botocore  # type: ignore
import time
import click
from typing import Set, List
from pkg_resources import resource_string
from jinja2 import Template  # type: ignore
from .utils import AthenaContext, Table

__all__ = ["QueryManager"]


class TooManyRequestsException(Exception):
    pass


class DryRunException(Exception):
    pass


class QueryManager:
    def __init__(self, athena_location: str, dry_run: bool = False, debug: bool = False):
        self.config = {"OutputLocation": athena_location}
        self.debug = debug
        # Athena client setup
        if dry_run:
            self.client = None
        elif "AWS_REGION" in os.environ:
            self.client = boto3.client("athena", os.environ["AWS_REGION"])
        else:
            self.client = boto3.client("athena")
        self.execution_ids: Set[str] = set()

    def start_query_execution(self, sql: str, context: AthenaContext) -> str:
        try:
            if self.debug:
                click.secho(sql, fg="cyan")
            if self.client is None:
                raise DryRunException
            r = self.client.start_query_execution(
                QueryString=sql, QueryExecutionContext=context, ResultConfiguration=self.config
            )
            return r["QueryExecutionId"]  # type: ignore
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] == "TooManyRequestsException":
                raise TooManyRequestsException()
            else:
                raise ex

    def execute_query(self, sql: str, context: AthenaContext, sleep_seconds: int = 1) -> None:
        "Execute an SQL statement on Athena"
        try:
            self.execution_ids.add(self.start_query_execution(sql, context))
            time.sleep(sleep_seconds)
        except TooManyRequestsException:
            self.wait_executions()
            time.sleep(sleep_seconds)
            self.execution_ids.add(self.start_query_execution(sql, context))
        except DryRunException:
            pass

    def execute_template(self, template: str, context: AthenaContext, data: Table) -> None:
        "Render an execute an SQL template on Athena"
        template_str = resource_string("dbting.templates", template).decode("utf-8")
        sql = Template(template_str).render(**data)
        return self.execute_query(sql, context)

    def wait_executions(self, sleep_seconds: int = 5) -> List[str]:
        errors: List[str] = []
        if self.client is None:
            return errors
        while self.execution_ids:
            time.sleep(sleep_seconds)
            for execution_id in list(self.execution_ids):
                r = self.client.get_query_execution(QueryExecutionId=execution_id)
                state = r["QueryExecution"]["Status"]["State"]
                if state in ["QUEUED", "RUNNING"]:
                    click.secho("{} {}".format(state, r["QueryExecution"]["Query"]))
                else:
                    if state == "FAILED":
                        error = r["QueryExecution"]["Status"]["StateChangeReason"]
                        errors.append(error)
                        click.secho(error, fg="red")
                    self.execution_ids.remove(execution_id)
        return errors
