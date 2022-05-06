#!/usr/bin/env bash
cd "$(dirname "$0")" || exit

# Modules to be uninstalled before upgrade
MODULES="dbt dbt-bigquery dbt-core dbt-extractor dbt-postgres dbt-redshift dbt-snowflake dbt-sqlite"

if env | grep -q C9_HOSTNAME; then
    #  AWS Cloud9
    pip3 uninstall -y $MODULES
    rm -rf dbt_modules
    pip3 install --user -r requirements.txt
else
    # Prepare virtual environment
    virtualenv .
    source ./bin/activate
    pip3 uninstall -y $MODULES
    rm -rf dbt_modules
    pip3 install -r requirements.txt
fi

# python3 dbting.py setup
