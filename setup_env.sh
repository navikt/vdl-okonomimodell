#!/bin/bash

if [ ! -f .venv/bin/pip ]; then
  make install
fi

if [ -f .venv/bin/pip ]; then
  source .venv/bin/activate
  dependency_diff=$(grep -Fvf <(pip freeze) requirements-lock.txt)
fi

if [[ ! -z "$dependency_diff" ]]; then
  echo deps diff: $dependency_diff
  make install
fi

#Snowbird
. ./infrastructure/auth.sh

#DBT
. ./dbt/auth.sh

code .
