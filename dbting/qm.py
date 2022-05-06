#!/usr/bin/env python

import os
import boto3
import botocore
import time
import click
from pkg_resources import resource_string
from jinja2 import Template

__all__ = ["QueryManager"]


class TooManyRequestsException(Exception):
    pass


class QueryManager:
    def __init__(self, athena_location, client=None):
        self.config = {"OutputLocation": athena_location}
        if client:
            self.client = client
        elif "AWS_REGION" in os.environ:
            self.client = boto3.client("athena", os.environ["AWS_REGION"])
        else:
            self.client = boto3.client("athena")
        self.execution_ids = set()

    def start_query_execution(self, sql, context):
        try:
            click.echo(sql)
            r = self.client.start_query_execution(
                QueryString=sql, QueryExecutionContext=context, ResultConfiguration=self.config
            )
            return r["QueryExecutionId"]
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] == "TooManyRequestsException":
                raise TooManyRequestsException()
            else:
                raise ex

    def execute_query(self, sql, context, sleep_seconds=1):
        try:
            self.execution_ids.add(self.start_query_execution(sql, context))
            time.sleep(sleep_seconds)
        except TooManyRequestsException:
            self.wait_executions()
            time.sleep(sleep_seconds)
            self.execution_ids.add(self.start_query_execution(sql, context))

    def execute_template(self, template, context, data):
        template_str = resource_string("dbting.templates", template).decode("utf-8")
        sql = Template(template_str).render(**data)
        return self.execute_query(sql, context)

    def wait_executions(self, sleep_seconds=5):
        errors = []
        while self.execution_ids:
            time.sleep(sleep_seconds)
            for execution_id in list(self.execution_ids):
                r = self.client.get_query_execution(QueryExecutionId=execution_id)
                state = r["QueryExecution"]["Status"]["State"]
                if state in ["QUEUED", "RUNNING"]:
                    click.echo(state, r["QueryExecution"]["Query"])
                else:
                    if state == "FAILED":
                        error = r["QueryExecution"]["Status"]["StateChangeReason"]
                        errors.append(error)
                        click.secho(error, fg="red")
                    self.execution_ids.remove(execution_id)
        return errors
