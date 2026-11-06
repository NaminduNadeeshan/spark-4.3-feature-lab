from pyspark.sql import functions as F

from lab import (ALERT_SCHEMA, JDBC_PROPS, JDBC_URL, PRICE_SCHEMA, READ_SCHEMA, RTM_WINDOW_SCHEMA, jdbc_read,
                 kafka_stream, pg, spark_session, upsert)

spark = spark_session("stream-to-postgres", cores="4")
CHECKPOINT = "/data/checkpoints/stream"


def write_prices(df, _):
    upsert(df.dropDuplicates(["region_id", "interval_start"]), "fact_price_5min", ["region_id", "interval_start"],
           ["region_id", "interval_start", "interval_end", "rrp", "source"])


def write_reads(df, _):
    if df.isEmpty():
        return
    s = df.sparkSession
    df.createOrReplaceTempView("batch_reads")
    jdbc_read(s, "fact_price_5min").createOrReplaceTempView("prices")
    priced = s.sql("""
        SELECT r.meter_id, r.read_ts, r.region_id, r.kwh, r.power_kw, r.voltage,
               p.rrp, p.interval_start AS price_interval,
               r.kwh * p.rrp / 1000.0 AS cost_aud
        FROM batch_reads r
        LEFT ASOF JOIN prices p
          MATCH_CONDITION (r.read_ts >= p.interval_start)
          ON r.region_id = p.region_id
    """).cache()
    priced.write.jdbc(JDBC_URL, "fact_meter_read", mode="append", properties=JDBC_PROPS)
    minute = (priced.groupBy("meter_id", F.date_trunc("minute", "read_ts").alias("minute"))
              .agg(F.count("*").cast("int").alias("reads"), F.sum("kwh").alias("kwh"),
                   F.max("power_kw").alias("max_kw"), F.sum("cost_aud").alias("cost_aud")))
    rows = [tuple(r) for r in minute.collect()]
    with pg() as conn, conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO fact_usage_1m (meter_id, minute, reads, kwh, max_kw, cost_aud) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (meter_id, minute) DO UPDATE SET
              reads = fact_usage_1m.reads + excluded.reads,
              kwh = fact_usage_1m.kwh + excluded.kwh,
              max_kw = greatest(fact_usage_1m.max_kw, excluded.max_kw),
              cost_aud = coalesce(fact_usage_1m.cost_aud, 0) + coalesce(excluded.cost_aud, 0)
        """, rows)
    priced.unpersist()


def write_alerts(df, _):
    upsert(df.dropDuplicates(["meter_id", "event_ts", "kind"]), "fact_alert", ["meter_id", "event_ts", "kind"],
           ["meter_id", "event_ts", "alert_ts", "kind", "value", "latency_ms"])


def write_windows(df, _):
    latest = (df.groupBy("region_id", "window_start")
              .agg(F.max_by(F.struct("reads", "kwh", "emitted_at"), "emitted_at").alias("x"))
              .select("region_id", "window_start", "x.*"))
    upsert(latest, "fact_rtm_window", ["region_id", "window_start"])


prices = kafka_stream(spark, "prices", PRICE_SCHEMA)
(prices.writeStream.foreachBatch(write_prices).option("checkpointLocation", f"{CHECKPOINT}/prices")
 .trigger(processingTime="10 seconds").queryName("prices").start())

reads = (kafka_stream(spark, "meter_reads", READ_SCHEMA, starting="latest")
         .withWatermark("read_ts", "30 seconds")
         .dropDuplicatesWithinWatermark(["meter_id", "read_ts"]))
(reads.writeStream.foreachBatch(write_reads).option("checkpointLocation", f"{CHECKPOINT}/reads")
 .trigger(processingTime="5 seconds").queryName("reads").start())

alerts = (kafka_stream(spark, "alerts", ALERT_SCHEMA)
          .selectExpr("meter_id", "event_ts", "kafka_ts AS alert_ts", "kind", "value",
                      "(unix_micros(kafka_ts) - unix_micros(source_kafka_ts)) / 1000.0 AS latency_ms"))
(alerts.writeStream.foreachBatch(write_alerts).option("checkpointLocation", f"{CHECKPOINT}/alerts")
 .trigger(processingTime="5 seconds").queryName("alerts").start())

windows = kafka_stream(spark, "rtm_region_kwh", RTM_WINDOW_SCHEMA).selectExpr(
    "region_id", "window_start", "reads", "kwh", "kafka_ts AS emitted_at")
(windows.writeStream.foreachBatch(write_windows).option("checkpointLocation", f"{CHECKPOINT}/rtm_windows")
 .trigger(processingTime="5 seconds").queryName("rtm_windows").start())

spark.streams.awaitAnyTermination()
