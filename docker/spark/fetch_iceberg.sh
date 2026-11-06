#!/usr/bin/env bash
set -euo pipefail

ARTIFACT="iceberg-spark-runtime-${ICEBERG_SPARK}_2.13"
TARGET=/opt/spark/jars/iceberg-spark-runtime.jar

if [[ "$ICEBERG_VERSION" != *-SNAPSHOT ]]; then
  URL="https://repo1.maven.org/maven2/org/apache/iceberg/${ARTIFACT}/${ICEBERG_VERSION}/${ARTIFACT}-${ICEBERG_VERSION}.jar"
  curl -sSfL -o "$TARGET" "$URL"
  echo "iceberg: $URL"
  exit 0
fi

BASE="https://repository.apache.org/content/repositories/snapshots/org/apache/iceberg/${ARTIFACT}/${ICEBERG_VERSION}"
PINNED="${BASE}/${ARTIFACT}-${ICEBERG_SNAPSHOT_STAMP}.jar"
if curl -sSfL -o "$TARGET" "$PINNED"; then
  echo "iceberg: pinned snapshot $PINNED"
  exit 0
fi

STAMP=$(curl -sSfL "${BASE}/maven-metadata.xml" | grep -m1 -o '<value>[^<]*</value>' | sed 's/<[^>]*>//g')
curl -sSfL -o "$TARGET" "${BASE}/${ARTIFACT}-${STAMP}.jar"
echo "iceberg: pinned snapshot no longer published, using latest ${STAMP}"
