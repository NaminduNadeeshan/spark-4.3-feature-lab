from pyspark.sql import functions as F

from lab import KAFKA, READ_SCHEMA, kafka_stream, spark_session

spark = spark_session("real-time-mode", cores="12", spark__sql__shuffle__partitions="2")
CHECKPOINT = "/data/checkpoints/rtm"

reads = kafka_stream(spark, "meter_reads", READ_SCHEMA, starting="latest")

alerts = reads.where("voltage > 253 OR power_kw > 28").selectExpr(
    "meter_id AS key",
    """to_json(named_struct(
         'meter_id', meter_id,
         'event_ts', read_ts,
         'source_kafka_ts', kafka_ts,
         'kind', CASE WHEN voltage > 253 THEN 'over_voltage' ELSE 'power_spike' END,
         'value', CASE WHEN voltage > 253 THEN voltage ELSE power_kw END)) AS value""")

(alerts.writeStream.format("kafka")
 .option("kafka.bootstrap.servers", KAFKA)
 .option("topic", "alerts")
 .option("checkpointLocation", f"{CHECKPOINT}/alerts")
 .outputMode("update")
 .trigger(realTime="1 minute")
 .queryName("rtm_stateless_alerts")
 .start())

region_kwh = (reads
              .withWatermark("read_ts", "10 seconds")
              .dropDuplicates(["meter_id", "read_ts"])
              .groupBy("region_id", F.window("read_ts", "10 seconds").alias("w"))
              .agg(F.count("*").alias("reads"), F.sum("kwh").alias("kwh"))
              .selectExpr("region_id AS key",
                          "to_json(named_struct('region_id', region_id, 'window_start', w.start, "
                          "'reads', reads, 'kwh', kwh)) AS value"))

(region_kwh.writeStream.format("kafka")
 .option("kafka.bootstrap.servers", KAFKA)
 .option("topic", "rtm_region_kwh")
 .option("checkpointLocation", f"{CHECKPOINT}/region_kwh")
 .outputMode("update")
 .trigger(realTime="1 minute")
 .queryName("rtm_stateful_region_kwh")
 .start())

spark.streams.awaitAnyTermination()
