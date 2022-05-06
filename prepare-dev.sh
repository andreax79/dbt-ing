#!/usr/bin/env bash
cd "$(dirname "$0")" || exit

if env | grep -q C9_HOSTNAME; then
    #  AWS Cloud9
    pip3 install --user -r requirements.txt -r requirements-dev.txt
else
    # Prepare virtual environment
    virtualenv .
    source ./bin/activate
    pip3 install -r requirements.txt -r requirements-dev.txt
fi

pre-commit install
