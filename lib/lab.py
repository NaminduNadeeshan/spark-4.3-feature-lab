import os
from contextlib import contextmanager

import psycopg
from pyspark.sql import SparkSession

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "energy")
PG_USER = os.getenv("PG_USER", "lab")
PG_PASSWORD = os.getenv("PG_PASSWORD", "lab")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/{PG_DB}?currentSchema=energy&reWriteBatchedInserts=true"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

READ_SCHEMA = "meter_id string, region_id string, read_ts timestamp, power_kw double, kwh double, voltage double"
PRICE_SCHEMA = "region_id string, interval_start timestamp, interval_end timestamp, rrp double, source string"
CHANGE_SCHEMA = ("meter_id string, region_id string, customer string, segment string, tariff string, status string, "
                 "lat double, lon double, op string, seq long, changed_at timestamp")
INTERVAL_SCHEMA = "meter_id string, interval_start timestamp, interval_minutes int, kwh double, quality string"
ALERT_SCHEMA = "meter_id string, event_ts timestamp, source_kafka_ts timestamp, kind string, value double"
RTM_WINDOW_SCHEMA = "region_id string, window_start timestamp, reads long, kwh double"


def spark_session(app, cores="2", **conf):
    builder = (SparkSession.builder.appName(app).master(f"local[{cores}]")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.join.asofJoin.enabled", "true")
               .config("spark.ui.showConsoleProgress", "false"))
    for k, v in conf.items():
        builder = builder.config(k.replace("__", "."), v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kafka_batch(spark, topic, schema):
    return (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", topic).option("startingOffsets", "earliest").load()
            .selectExpr(f"from_json(cast(value as string), '{schema}') as r", "timestamp as kafka_ts")
            .select("r.*", "kafka_ts"))


def kafka_stream(spark, topic, schema, starting="earliest"):
    return (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", topic).option("startingOffsets", starting).load()
            .selectExpr(f"from_json(cast(value as string), '{schema}') as r", "timestamp as kafka_ts")
            .select("r.*", "kafka_ts"))


@contextmanager
def pg():
    with psycopg.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
                         options="-c search_path=energy") as conn:
        yield conn


def jdbc_read(spark, table_or_query):
    src = table_or_query if " " not in table_or_query.strip() else f"({table_or_query}) q"
    return spark.read.jdbc(JDBC_URL, src, properties=JDBC_PROPS)


def upsert(df, table, keys, cols=None):
    cols = cols or df.columns
    rows = [tuple(r[c] for c in cols) for r in df.select(*cols).collect()]
    if not rows:
        return 0
    update = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in keys)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
           f"ON CONFLICT ({', '.join(keys)}) DO " + (f"UPDATE SET {update}" if update else "NOTHING"))
    with pg() as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def replace(df, table, cols=None):
    cols = cols or df.columns
    rows = [tuple(r[c] for c in cols) for r in df.select(*cols).collect()]
    with pg() as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table}")
        if rows:
            cur.executemany(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})", rows)
    return len(rows)


def log_run(job, started_at, status, detail=""):
    with pg() as conn:
        conn.execute("INSERT INTO pipeline_run (job, started_at, finished_at, status, detail) VALUES (%s, %s, now(), %s, %s)",
                     (job, started_at, status, detail[:2000]))
