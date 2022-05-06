#!/usr/bin/env python

import re
import os
import os.path
import sys
import json
import yaml
import shutil
from openpyxl import load_workbook
from .utils import (
    to_bool,
    DEFAULT_PARTITIONS,
    DEFAULT_FIELD_DELIMITER,
    DEFAULT_SOURCE_FORMAT,
)

__all__ = [
    "xlsx_to_json_mapping",
    "MappingException",
]


def represent_ordereddict(dumper, data):
    value = []
    for item_key, item_value in data.items():
        node_key = dumper.represent_data(item_key)
        node_value = dumper.represent_data(item_value)
        value.append((node_key, node_value))
    return yaml.nodes.MappingNode("tag:yaml.org,2002:map", value)


yaml.add_representer(dict, represent_ordereddict)


class quoted(str):
    pass


# define a custom representer for strings
def quoted_presenter(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')


yaml.add_representer(quoted, quoted_presenter)


def filename_to_source_table(filename: str, flow: str) -> str:
    "Convert filename into source table name"
    t = re.sub("\..*$", "", filename)  # remove extension
    t = re.sub("{{[^}]*}}", "", t).replace("-", "_").strip("_ . -")  # remove jinja macro
    t = t.replace("*", "").replace("?", "").strip("_")
    t = t.lower()
    if not t.startswith(flow + "_"):
        t = flow + "_" + t
    return t


def get_target_table_name(target_table: str, flow: str) -> str:
    result = target_table.lower()
    if not result.startswith(flow + "_"):
        result = flow + "_" + result
    return result


def table_to_path(table: str, flow: str) -> str:
    return flow + "/" + table[len(flow) + 1 :]


class MappingException(Exception):
    pass


class Empty(Exception):
    pass


def parse_row_config(table_def, columns, row):
    try:
        table_def[row[0].strip().lower()] = row[1].strip() if row[1] else ""
    except Exception as ex:
        raise MappingException("Error parsing row {}: {}".format(row, ex))


def parse_row_where(table_def, columns, row):
    where_condition = (row[0] or "").strip()
    if where_condition:
        table_def["where_condition"] = where_condition


def parse_row_select_header(table_def, columns, row):
    table_def["header"] = dict((h.lower(), i) for i, h in enumerate(row) if h is not None)
    table_def["parse_row"] = parse_row_select


def parse_row_select(table_def, columns, row):
    item = {}
    for h, i in table_def["header"].items():
        try:
            value = row[i]
            if isinstance(value, str):
                value = value.strip()
        except Exception:
            value = None
        item[h] = value
    columns.append(item)


def parse_xlsx_legacy(
    ws,
    flow: str,
    batch_profile: str,
    datalake_profile: str,
    batch_location: str,
    datalake_location: str,
    source_schema: str,
    target_schema: str,
    field_delimiter: str,
):
    flow = flow.lower()
    header = None
    rows = []
    for row in ws.values:
        if header is None:
            header = dict((h.lower(), i) for i, h in enumerate(row))
        else:
            item = {}
            for h, i in header.items():
                try:
                    value = row[i]
                    if isinstance(value, str):
                        value = value.strip()
                except Exception:
                    value = None
                item[h] = value
            rows.append(item)

    tables = {}
    for row_number, item in enumerate(rows, start=1):
        if not item.get("file_name"):
            continue
        target_table = get_target_table_name(item["target_table"], flow)
        table_def = tables.get(target_table)
        if table_def is None:
            table_def = {
                "flow": flow,
                "batch_profile": batch_profile,
                "datalake_profile": datalake_profile,
                "batch_location": batch_location,
                "datalake_location": datalake_location,
                "source_schema": source_schema,
                "target_schema": target_schema,
                "field_delimiter": field_delimiter,
                "partitions": DEFAULT_PARTITIONS,
                "columns": [],
            }
            tables[target_table] = table_def
        table_def["columns"].append(item)
    return tables


def parse_worsheet(
    ws,
    flow: str,
    tables,
    batch_profile: str,
    datalake_profile: str,
    batch_location: str,
    datalake_location: str,
    source_schema: str,
    target_schema: str,
    field_delimiter: str,
):
    flow = flow.lower()
    columns = []
    if ws["A1"].value != "TYPE":
        print("- skip worksheet {}".format(ws.title))
        return
    table_def = {
        "parse_row": None,
        "type": ws["A2"].value,
        "flow": flow,
        "batch_profile": batch_profile,
        "datalake_profile": datalake_profile,
        "batch_location": batch_location,
        "datalake_location": datalake_location,
        "source_schema": source_schema,
        "target_schema": target_schema,
        "field_delimiter": field_delimiter,
        "partitions": DEFAULT_PARTITIONS,
        "columns": columns,
    }
    if table_def["type"] != "#INGESTION":
        raise MappingException("#{} invalid type {}".format(0, table_def["type"]))
    for i, row in enumerate(ws.values, start=1):
        if i > 2:
            if row[0] == "#CONFIG":
                table_def["parse_row"] = parse_row_config
            elif row[0] == "#WHERE":
                table_def["parse_row"] = parse_row_where
            elif row[0] == "#SELECT":
                table_def["parse_row"] = parse_row_select_header
            else:
                table_def["parse_row"](table_def, columns, row)
    del table_def["parse_row"]
    del table_def["header"]
    target_table = get_target_table_name(columns[0]["target_table"], flow)
    print("- worksheet {} table {}".format(ws.title, target_table))
    if target_table in tables:
        raise MappingException("Duplicated target table {}".format(target_table))
    tables[target_table] = table_def


def parse_xlsx(
    filename: str,
    flow: str,
    batch_profile: str,
    datalake_profile: str,
    batch_location: str,
    datalake_location: str,
    source_schema: str,
    target_schema: str,
    field_delimiter: str = DEFAULT_FIELD_DELIMITER,
    worksheet: int = 0,
):
    # Read xlsx
    flow = flow.lower()
    wb = load_workbook(filename, read_only=True)
    try:
        ws = wb.worksheets[worksheet]
        if ws["A1"].value != "TYPE":  # legacy
            return parse_xlsx_legacy(
                ws=ws,
                flow=flow,
                batch_profile=batch_profile,
                datalake_profile=datalake_profile,
                batch_location=batch_location,
                datalake_location=datalake_location,
                source_schema=source_schema,
                target_schema=target_schema,
                field_delimiter=field_delimiter,
            )
        else:
            tables = {}
            for ws in wb.worksheets:
                parse_worsheet(
                    ws=ws,
                    flow=flow,
                    tables=tables,
                    batch_profile=batch_profile,
                    datalake_profile=datalake_profile,
                    batch_location=batch_location,
                    datalake_location=datalake_location,
                    source_schema=source_schema,
                    target_schema=target_schema,
                    field_delimiter=field_delimiter,
                )
            return tables
    finally:
        # Close the workbook after reading
        wb.close()


def parse_rows(tables, flow):
    flow = flow.lower()
    data = []
    for target_table, table_def in list(tables.items()):
        if to_bool(table_def.get("exclude_table")):
            print("- skip target table %s" % target_table)
            del tables[target_table]
            continue
        print("- %s" % target_table)
        columns = []
        table_def["col_number"] = 0
        for row_number, item in enumerate(table_def["columns"], start=1):
            try:
                if not item.get("file_name"):
                    raise Empty
                st = target_table
                col = {}
                source_format = table_def.get("source_format")
                col["flow"] = flow
                col["source_schema"] = table_def["source_schema"]
                col["filename"] = item["file_name"]
                try:
                    col["source_table"] = filename_to_source_table(col["filename"], flow)
                except:
                    raise MappingException("#{} invalid filename {}".format(row_number, item["filename"]))
                col["source_location"] = table_def.get("source_location") or (
                    os.path.join(
                        table_def["batch_location"],
                        table_to_path(col["source_table"], flow),
                    )
                    + "/"
                )

                for k in ["source_table", "source_location", "filename"]:
                    if k not in table_def:
                        table_def[k] = col[k]

                if col["source_schema"] != table_def["source_schema"]:
                    raise MappingException("#{} source schema mismatch {}".format(row_number, col["source_schema"]))
                if col["source_table"] != table_def["source_table"]:
                    raise MappingException("#{} source table mismatch {}".format(row_number, col["source_table"]))
                if col["source_location"] != table_def["source_location"]:
                    raise MappingException("#{} source location mismatch {}".format(row_number, col["source_location"]))

                col_number = table_def["col_number"] + 1
                table_def["col_number"] = col_number
                table_def["columns"].append(col)
                if source_format == "json":
                    col["source_column"] = item["source_name"].lower()
                else:
                    col["source_column"] = "col{}".format(col_number)
                    if to_bool(table_def.get("use_source_name")):
                        col["source_formula"] = item["source_name"]
                col["target_schema"] = table_def["target_schema"]
                col["target_table"] = st
                col["target_location"] = table_def.get("target_location") or (
                    os.path.join(
                        table_def["datalake_location"],
                        table_to_path(col["target_table"], flow),
                    )
                    + "/"
                )
                tables[st]["target_schema"] = col["target_schema"]
                tables[st]["target_table"] = col["target_table"]
                tables[st]["target_location"] = col["target_location"]
                if not item.get("new_name"):
                    raise MappingException("#{} invalid target_column {}".format(row_number, "<empty>"))
                col["target_column"] = item["new_name"].lower()
                if "." in col["target_column"] or " " in col["target_column"] or "-" in col["target_column"]:
                    raise MappingException("#{} invalid target_column {}".format(row_number, col["target_column"]))
                col["description"] = item["description"] or ""
                col["data_type"] = item["data_type"].lower()
                col["key"] = to_bool(item["key"])
                col["index"] = to_bool(item["index"])
                col["nullable"] = to_bool(item["nullable"])
                if not col["data_type"]:
                    raise MappingException("#{} missing data_type in row {}".format(row_number, str(item)))
                elif col["data_type"] in ("char", "varchar"):
                    col["data_type"] = "{}({})".format(col["data_type"], item.get("length") or 255)
                elif col["data_type"] in ("decimal"):
                    col["data_type"] = "{}({})".format(
                        col["data_type"],
                        (str(item.get("length") or "18,2")).replace(".", ","),
                    )
                elif col["data_type"] in ("float"):
                    col["data_type"] = "real"
                elif col["data_type"] in ("double"):
                    col["data_type"] = "double precision"
                elif col["data_type"] in ("date", "timestamp"):
                    col["format"] = item.get("length")  # length column is used for format for the date/timestamp
                elif col["data_type"] in (
                    "boolean",
                    "tinyint",
                    "smallint",
                    "integer",
                    "bigint",
                    "float",
                    "double precision",
                ):
                    pass
                elif col["data_type"] in ("array<char>", "array<varchar>"):
                    col["data_type"] = "{}({})>".format(col["data_type"].rstrip(">"), item.get("length") or 255)
                else:
                    raise MappingException("#{} invalid data_type in row {}".format(row_number, str(item)))
                columns.append(col)
            except Empty:
                # print('#{} empty line'.format(row_number))
                pass

        # Add partition columns
        try:
            partitions = table_def["partitions"]
            if partitions == "":
                partitions = []
            elif isinstance(partitions, str):
                partitions = [x.strip() for x in table_def["partitions"].split(",")]
        except:
            partitions = DEFAULT_PARTITIONS
        table_def["partitions"] = partitions
        for k in partitions:
            if k == "year":
                data_type = "char(4)"
            elif k in ("day", "month", "hour", "minute"):
                data_type = "char(2)"
            else:
                data_type = "varchar(20)"
            column = {
                "source_schema": table_def["source_schema"],
                "source_table": table_def["source_table"],
                "source_location": table_def["source_location"],
                "source_column": k,
                "target_schema": table_def["target_schema"],
                "target_table": table_def["target_table"],
                "target_location": table_def["target_location"],
                "target_column": k,
                "description": "",
                "partition": "yes",
                "data_type": data_type,
            }
            columns.append(column)
        table_def["columns"] = columns
        data.extend(columns)

    # Write json
    os.makedirs("flows", exist_ok=True)
    with open(os.path.join("flows", flow + ".json"), "w") as f:
        # Sort columns by schema and table
        data = sorted(data, key=lambda x: (x["source_schema"], x["source_table"]))
        f.write(json.dumps(data, indent=2))
    with open(os.path.join("flows", flow + ".cfg.json"), "w") as f:
        f.write(json.dumps(tables, indent=2))
    return tables


def generate_column(column, kind, table_def, forced_type=None, has_compound_key=False, table_format=None):
    if kind == "batch" and not column.get("meta", {}).get("partition") and not table_format == "json":
        forced_type = "varchar(65535)" if not column.get("meta", {}).get("partition") else None
    result = {
        "name": column["source_column"] if kind == "batch" else column["target_column"],
        "description": column["description"],
        "data_type": forced_type or column["data_type"],
        "tests": [],
        "meta": {},
    }
    if column.get("partition") == "yes":
        result["meta"]["partition"] = "yes"
    if kind == "datalake":
        if column.get("key") and not has_compound_key:
            result["tests"].append(
                {
                    "unique_where": {
                        "where": quoted(
                            " and ".join("{f}='{{{{ var('{f}') }}}}'".format(f=f) for f in table_def["partitions"])
                        )
                    }
                }
            )
    if not column.get("nullable") and column.get("partition") != "yes":
        result["tests"].append(
            {
                "not_null": {
                    "where": quoted(
                        " and ".join("{f}='{{{{ var('{f}') }}}}'".format(f=f) for f in table_def["partitions"])
                    )
                }
            }
        )
    if not result["meta"]:
        del result["meta"]
    if not result["tests"]:
        del result["tests"]
    return result


def generate_sources(tables, kind, flow):
    sources_path = "./models/sources/{kind}/{flow}".format(kind=kind, flow=flow)
    shutil.rmtree(sources_path, ignore_errors=True)
    os.makedirs(sources_path, exist_ok=True)

    for table_def in tables.values():
        if kind == "batch":
            name = table_def["batch_profile"] + "__" + table_def["source_table"]
            source_format = table_def.get("source_format") or DEFAULT_SOURCE_FORMAT
            table_properties = {
                "CrawlerSchemaDeserializerVersion": "1.0",
                "CrawlerSchemaSerializerVersion": "1.0",
                "typeOfData": "file",
                "compressionType": table_def.get("compression_type", "none"),
            }
            if source_format == "json":
                table_properties["classification"] = "json"
            else:
                table_properties["areColumnsQuoted"] = "false"
                table_properties["classification"] = "csv"
                table_properties["columnsOrdered"] = "true"
                table_properties["delimiter"] = table_def["field_delimiter"]
            doc = {
                "version": 2,
                "sources": [
                    {
                        "name": table_def["batch_profile"],
                        "tables": [
                            {
                                "name": name,
                                "description": table_def.get("description")
                                or "{flow} - {name}".format(flow=flow, name=name),
                                "identifier": table_def["source_table"],
                                "meta": {
                                    "flow": table_def["flow"],
                                    "field_delimiter": table_def["field_delimiter"],
                                    "file_format": "textfile",
                                    "location": table_def["source_location"],
                                    "format": source_format,
                                    "table_properties": table_properties,
                                },
                                "columns": [
                                    generate_column(
                                        column,
                                        kind,
                                        table_def,
                                        table_format=source_format,
                                    )
                                    for column in table_def["columns"]
                                ],
                            }
                        ],
                    }
                ],
            }
        else:
            name = table_def["datalake_profile"] + "__" + table_def["target_table"]
            key_columns = [column["target_column"] for column in table_def["columns"] if column.get("key")]
            has_compound_key = len(key_columns) > 1
            if not has_compound_key:
                tests = None
            else:
                tests = [
                    {
                        "unique_columns_where": {
                            "combination_of_columns": key_columns,
                            "where": quoted(
                                " and ".join("{f}='{{{{ var('{f}') }}}}'".format(f=f) for f in table_def["partitions"])
                            ),
                        }
                    }
                ]
            tags = table_def.get("tags").split() if table_def.get("tags") else None
            doc = {
                "version": 2,
                "sources": [
                    {
                        "name": table_def["datalake_profile"],
                        "tables": [
                            {
                                "name": name,
                                "description": table_def.get("description")
                                or "{flow} - {name}".format(flow=flow, name=name),
                                "identifier": table_def["target_table"],
                                "meta": {
                                    "flow": table_def["flow"],
                                    "file_format": "parquet",
                                    "location": table_def["target_location"],
                                    "table_properties": {
                                        "classification": "parquet",
                                    },
                                },
                                "tests": tests,
                                "tags": tags,
                                "columns": [
                                    generate_column(
                                        column,
                                        kind,
                                        table_def,
                                        has_compound_key=has_compound_key,
                                    )
                                    for column in table_def["columns"]
                                ],
                            }
                        ],
                    }
                ],
            }
            for k in ["tests", "tags"]:
                if not doc["sources"][0]["tables"][0][k]:
                    del doc["sources"][0]["tables"][0][k]
        with open(os.path.join(sources_path, name + ".yml"), "w") as f:
            f.write(yaml.dump(doc, width=128))


def check(tables):
    has_duplicates = False
    for table in tables.values():
        cols = set()
        for column in table["columns"]:
            if column["target_column"] in cols:
                print(
                    "Duplicate column '{column}' in table '{table}'".format(
                        column=column["target_column"], table=table["target_table"]
                    )
                )
                has_duplicates = True
            cols.add(column["target_column"])
    if has_duplicates:
        raise MappingException("Duplicated columns")


def xlsx_to_json_mapping(
    filename,
    flow,
    batch_profile,
    datalake_profile,
    batch_location,
    datalake_location,
    source_schema,
    target_schema,
    field_delimiter=DEFAULT_FIELD_DELIMITER,
    worksheet=0,
):
    tables = parse_xlsx(
        filename=filename,
        flow=flow,
        batch_profile=batch_profile,
        datalake_profile=datalake_profile,
        batch_location=batch_location,
        datalake_location=datalake_location,
        source_schema=source_schema,
        target_schema=target_schema,
        field_delimiter=field_delimiter,
        worksheet=worksheet,
    )
    tables = parse_rows(tables, flow)
    check(tables)
    generate_sources(tables, "batch", flow)
    generate_sources(tables, "datalake", flow)
