#!/usr/bin/env bash

PY_DIR="./env-pypy"
if [[ ! -d "$PY_DIR" ]]; then
  virtualenv --python=pypy3 "$PY_DIR"
fi

source "$PY_DIR/bin/activate"
pip install --upgrade pip
pypy3 -m pip install -r ./requirements.txt
