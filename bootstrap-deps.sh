#!/usr/bin/env bash

PY_DIR="./env"
if [[ ! -d "$PY_DIR" ]]; then
  python3 -m venv ./env
fi

source "$PY_DIR/bin/activate"
pip install --upgrade pip
pip install -r ./requirements.txt

