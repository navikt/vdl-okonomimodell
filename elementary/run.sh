#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
source $SCRIPTPATH/.venv/bin/activate

TOKEN=
CHANNEL=virksomhetsdatalaget-info-test
DBT_PATH=$(builtin cd $SCRIPTPATH/../dbt; pwd)

edr send-report \
  --slack-token $TOKEN \
  --slack-channel-name $CHANNEL \
  --project-dir $DBT_PATH \
  --profiles-dir $DBT_PATH \
  --disable-samples true

edr monitor \
  --slack-token $TOKEN \
  --slack-channel-name $CHANNEL \
  --project-dir $DBT_PATH \
  --profiles-dir $DBT_PATH \
  --disable-samples true \
  --test true
