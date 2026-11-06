#!/usr/bin/env bash
set -u
INTERVAL=${BATCH_INTERVAL_SEC:-60}

while true; do
  echo "$(date -u +%FT%TZ) pipeline meter_cdc"
  spark-pipelines --driver-memory 1g run --spec /work/pipelines/meter_cdc/spark-pipeline.yml 2>&1 \
    | grep -E "Flow .* (COMPLETED|FAILED)|Run is|Error|Exception" | grep -v "^\s*at "
  echo "$(date -u +%FT%TZ) batch features"
  spark-submit --driver-memory 1g /work/jobs/batch_features.py 2>&1 | grep -vE "WARN|^\s+at |log4j|NativeCode"
  sleep "$INTERVAL"
done
