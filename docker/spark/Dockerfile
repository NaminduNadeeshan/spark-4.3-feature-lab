FROM apache/spark:4.3.0

USER root

ARG ICEBERG_SPARK=4.2
ARG ICEBERG_VERSION=1.13.0-SNAPSHOT
ARG ICEBERG_SNAPSHOT_STAMP=1.13.0-20261106.001914-50

ENV PYSPARK_PYTHON=python3 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/opt/spark/bin:$PATH \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/work/lib

RUN pip install --no-cache-dir \
      jupyterlab==4.4.* nbconvert ipykernel \
      pyyaml grpcio grpcio-status protobuf googleapis-common-protos zstandard \
      pandas pyarrow numpy matplotlib \
      psycopg[binary] \
      kafka-python-ng

COPY resolve_jars.py /tmp/resolve_jars.py
RUN python3 /tmp/resolve_jars.py \
      org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0,org.postgresql:postgresql:42.7.7 /opt/spark/jars \
      spark-sql-kafka,spark-token-provider-kafka,kafka-clients,commons-pool2,postgresql \
 && rm -rf /root/.ivy2* /tmp/resolve_jars.py

COPY fetch_iceberg.sh /tmp/fetch_iceberg.sh
RUN ICEBERG_SPARK=$ICEBERG_SPARK ICEBERG_VERSION=$ICEBERG_VERSION ICEBERG_SNAPSHOT_STAMP=$ICEBERG_SNAPSHOT_STAMP \
      bash /tmp/fetch_iceberg.sh && rm /tmp/fetch_iceberg.sh

COPY catalog /tmp/catalog
RUN mkdir -p /tmp/catalog/out \
 && /opt/java/openjdk/bin/javac -nowarn -d /tmp/catalog/out -cp "/opt/spark/jars/*" /tmp/catalog/IcebergChangelogCatalog.java \
 && /opt/java/openjdk/bin/jar cf /opt/spark/jars/lab-iceberg-changelog.jar -C /tmp/catalog/out . \
 && rm -rf /tmp/catalog

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN mkdir -p /home/spark /work /data && chown -R spark:spark /home/spark /work /data
ENV HOME=/home/spark

USER spark
WORKDIR /work
