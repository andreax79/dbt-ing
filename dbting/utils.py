#!/usr/bin/env python

import os
import os.path
import json
import sys
import click
import yaml
import subprocess
from cryptography.fernet import Fernet
from jinja2 import Template

__all__ = [
    "decrypt_config_passwords",
    "load_mapping",
    "load_config",
    "run",
    "to_bool",
    "write_config_file",
    "DEFAULT_DATETIME_FORMAT",
    "DEFAULT_DATE_FORMAT",
    "DEFAULT_PARTITIONS",
    "DEFAULT_FIELD_DELIMITER",
    "DEFAULT_SOURCE_FORMAT",
    "VERSION",
]


_BOOL_LOOKUP = {
    "yes": True,
    "y": True,
    "true": True,
    "t": True,
    "si": True,
    "1": True,
    True: True,
    1: True,
}

DEFAULT_DATETIME_FORMAT = "YYYYMMdd HH.mm.ss"
DEFAULT_DATE_FORMAT = "YYYYMMdd"
DEFAULT_PARTITIONS = ["year", "month", "day"]
DEFAULT_FIELD_DELIMITER = "|"
DEFAULT_SOURCE_FORMAT = "csv"
DEFAULT_CONFIG = {
    "CONFIG": "config/dbting.yml",
    "SPO_CREDENTIALS": "~/.spo/credentials",
    "DBT_PROFILES": "~/.dbt/profiles.yml",
    "INGESTION_PATH": "ingestion",
    "FLOW_INDEX_XLSX": "flow_index.xlsx",
    "SHAREPOINT__BASE_URL": None,
    "SHAREPOINT__HOST": None,
}

VERSION_FILE = os.path.join(os.path.dirname(__file__), "VERSION")

with open(VERSION_FILE) as f:
    VERSION = f.read().strip()


def to_bool(value) -> bool:
    "Convert a string to a bool value"
    if not value:
        return None
    if isinstance(value, str):
        value = value.lower()
    try:
        return _BOOL_LOOKUP[value]
    except KeyError:
        return False


def load_mapping(flow: str, include_target_tables=None) -> None:
    "Return mapping configuration for a given flow"
    ingestion_mapping_path = os.path.join("flows", "{flow}.cfg.json".format(flow=flow))
    with open(ingestion_mapping_path, "r") as f:
        mapping = json.load(f)
    return filter_tables(mapping, include_target_tables)


def filter_tables(mapping, include_target_tables=None):
    if not include_target_tables:
        return mapping
    result = {}
    for table in include_target_tables:
        if table not in mapping.keys():
            click.secho("table {table} not found".format(table=table), fg="red")
            sys.exit(1)
        else:
            result[table] = mapping[table]
    return result


def run(command) -> None:
    "Execute a command and exit if the command fails"
    if isinstance(command, str):
        if os.system(command) != 0:
            sys.exit(1)
    else:
        if subprocess.run(command).returncode != 0:
            sys.exit(1)


def load_config():
    "Load the configuration"
    config = dict(DEFAULT_CONFIG)
    with open(config["CONFIG"], "r") as f:
        config.update(yaml.safe_load(f))
    # Convert the keys to uppercase
    t = dict((k.upper(), v) for k, v in config.items())
    result = {}
    for k, v in t.items():
        if isinstance(v, dict):
            result.update(dict(("%s__%s" % (k.upper(), kk.upper()), vv) for kk, vv in v.items()))
        else:
            result[k.upper()] = v
    return result


def decrypt_config_passwords(config) -> None:
    "Decrypt the encrypted passwords (ending with _ENC)"
    if not config.get("S3__KEY_LOCATION"):
        return
    # Download the key
    key_path = os.path.basename(config["S3__KEY_LOCATION"])
    run(["aws", "s3", "cp", config["S3__KEY_LOCATION"], key_path])
    with open(key_path, "rb") as f:
        key = Fernet(f.read())
    # Decrypt the passwords
    for k, v in list(config.items()):
        if k.endswith("_ENC"):
            k = k[:-4]
            v = key.decrypt(bytes(v, "ascii")).decode("ascii")
            config[k] = v
    # click.echo(key.encrypt(bytes('key', 'ascii')).decode('ascii'))


def write_config_file(config, filename: str) -> None:
    "Render a config template"
    basename = os.path.basename(filename)
    with open(os.path.join("config", basename + ".tmpl")) as f:
        template = Template(f.read())
        data = template.render(**config)
    os.makedirs(os.path.expanduser(os.path.dirname(filename)), exist_ok=True)
    with open(os.path.expanduser(filename), "w") as f:
        f.write(data)
