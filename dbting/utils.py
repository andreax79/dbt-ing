#!/usr/bin/env python

import os
import os.path
import json
import sys
import click
import yaml
import subprocess
from cryptography.fernet import Fernet  # type: ignore
from jinja2 import Template  # type: ignore
from typing import Any, Callable, Dict, List, Optional, Union

__all__ = [
    "click_partition_options",
    "decrypt_config_passwords",
    "load_mapping",
    "load_config",
    "prepare_partitions",
    "run",
    "to_bool",
    "write_config_file",
    "AthenaContext",
    "Config",
    "Table",
    "Tables",
    "TargetTables",
    "Partitions",
    "DEFAULT_DATETIME_FORMAT",
    "DEFAULT_DATE_FORMAT",
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
PARTITIONS_TYPES = {"varchar": str, "char": str, "int": int}

VERSION_FILE = os.path.join(os.path.dirname(__file__), "VERSION")

with open(VERSION_FILE) as f:
    VERSION = f.read().strip()

# Typing
AthenaContext = Dict[str, str]
Config = Dict[str, Any]
Table = Dict[str, Any]
Tables = Dict[str, Table]
TargetTables = Optional[List[str]]
Partitions = Dict[str, str]


def to_bool(value: Any) -> bool:
    "Convert a string to a bool value"
    if not value:
        return False
    if isinstance(value, str):
        value = value.lower()
    try:
        return _BOOL_LOOKUP[value]
    except KeyError:
        return False


def load_mapping(flow: str, include_target_tables: TargetTables = None) -> Tables:
    "Return mapping configuration for a given flow"
    ingestion_mapping_path = os.path.join("flows", "{flow}.cfg.json".format(flow=flow))
    with open(ingestion_mapping_path, "r") as f:
        mapping = json.load(f)
    return filter_tables(mapping, include_target_tables)


def filter_tables(mapping: Tables, include_target_tables: TargetTables = None) -> Tables:
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


def run(command: Union[str, List[str]], debug: bool = False) -> None:
    "Execute a command and exit if the command fails"
    if isinstance(command, str):
        if debug:
            click.secho(command, fg="cyan")
        if os.system(command) != 0:
            sys.exit(1)
    else:
        if debug:
            click.secho(" ".join(command), fg="cyan")
        if subprocess.run(command).returncode != 0:
            sys.exit(1)


def load_config() -> Config:
    "Load the configuration"
    config = dict(DEFAULT_CONFIG)
    with open(config["CONFIG"], "r") as f:  # type: ignore
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


def decrypt_config_passwords(config: Config) -> None:
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
    # click.secho(key.encrypt(bytes('key', 'ascii')).decode('ascii'))


def write_config_file(config: Config, filename: str) -> None:
    "Render a config template"
    basename = os.path.basename(filename)
    with open(os.path.join("config", basename + ".tmpl")) as f:
        template = Template(f.read())
        data = template.render(**config)
    os.makedirs(os.path.expanduser(os.path.dirname(filename)), exist_ok=True)
    with open(os.path.expanduser(filename), "w") as f:
        f.write(data)


def click_partition_options(config: Config) -> Callable[[Any], Any]:
    "Add partition options to click commands"

    def decorator(f: Callable[[Any], Any]) -> Callable[[Any], Any]:
        for partition in reversed(config.get("PARTITIONS", [])):
            click.option(
                "--" + partition["name"],
                partition["name"],
                required=to_bool(partition.get("required")),
                type=PARTITIONS_TYPES[partition.get("type", "varchar").lower()],
            )(f)
        return f

    return decorator


def prepare_partitions(partitions_args: Dict[str, Any], config: Config) -> Partitions:
    result: Partitions = {}
    for partition in config.get("PARTITIONS", []):
        name = partition["name"]
        value = partitions_args.get(name)
        if value is not None:
            if partition["type"] == "int":
                value = "{number:0{width}d}".format(width=partition["length"], number=value)
            result[name] = value  # type: ignore
    return result
