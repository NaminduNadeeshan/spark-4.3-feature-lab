import struct
import traceback
from datetime import datetime, timezone

from pyspark.sql import functions as F

from lab import INTERVAL_SCHEMA, KAFKA, jdbc_read, kafka_batch, log_run, pg, replace, spark_session, upsert

spark = spark_session("batch-features", cores="4", spark__sql__warehouse__dir="/data/warehouse")
HAVERSINE_KM = """2 * 6371 * asin(sqrt(
    pow(sin(radians(s.lat - m.lat) / 2), 2) +
    cos(radians(m.lat)) * cos(radians(s.lat)) * pow(sin(radians(s.lon - m.lon) / 2), 2)))"""


@F.udf("binary")
def point_wkb(lon, lat):
    return struct.pack("<BIdd", 1, 1, lon, lat)


def load_dimensions():
    meters = (spark.read.table("lake.energy.dim_meter")
              .withColumn("wkb", point_wkb("lon", "lat"))
              .withColumn("geog", F.expr("st_geogfromwkb(wkb)")))
    meters.write.mode("overwrite").parquet("/data/warehouse/meter_geo")
    meters = spark.read.parquet("/data/warehouse/meter_geo").selectExpr(
        "*", "st_srid(geog) AS srid", "st_asbinary(geog) AS geog_wkb", "seq AS last_seq")
    n1 = replace(meters, "dim_meter", ["meter_id", "region_id", "customer", "segment", "tariff", "status",
                                       "lat", "lon", "geog_wkb", "last_seq"])
    history = spark.read.table("lake.energy.dim_meter_history").selectExpr(
        "meter_id", "region_id", "customer", "segment", "tariff", "status", "lat", "lon",
        "__START_AT AS start_seq", "__END_AT AS end_seq",
        "timestamp_micros(__START_AT) AS valid_from", "timestamp_micros(__END_AT) AS valid_to",
        "__END_AT IS NULL AS is_current")
    n2 = replace(history, "dim_meter_history")
    return f"dim_meter={n1} dim_meter_history={n2}"


def dimension_changes():
    with pg() as conn:
        last = conn.execute("SELECT coalesce(max(commit_version), 0) FROM dim_meter_change_log").fetchone()[0]
    changes = spark.sql(f"""
        SELECT _commit_version AS commit_version, _commit_timestamp AS commit_ts, _change_type AS change_type,
               meter_id, region_id, customer, segment, tariff, status
        FROM lake.energy.dim_meter CHANGES FROM VERSION {last} EXCLUSIVE
        WITH ('computeUpdates' = 'true')
    """)
    n = upsert(changes, "dim_meter_change_log", ["commit_version", "meter_id", "change_type"])
    return f"dim_meter_change_log=+{n} after version {last}"


def nearest_substations():
    spark.read.parquet("/data/warehouse/meter_geo").createOrReplaceTempView("m")
    jdbc_read(spark, "dim_substation").createOrReplaceTempView("s")
    nearest = spark.sql(f"""
        SELECT m.meter_id, s.substation_id, {HAVERSINE_KM} AS distance_km
        FROM m JOIN s EXACT NEAREST 2 BY DISTANCE {HAVERSINE_KM}
    """).withColumn("rank", F.expr("row_number() OVER (PARTITION BY meter_id ORDER BY distance_km)").cast("int"))
    return f"bridge_meter_substation={replace(nearest, 'bridge_meter_substation')}"


def interval_reads():
    intervals = kafka_batch(spark, "meter_intervals", INTERVAL_SCHEMA).dropDuplicates(["meter_id", "interval_start"])
    intervals.createOrReplaceTempView("intervals")
    spark.read.parquet("/data/warehouse/meter_geo").select("meter_id", "region_id").createOrReplaceTempView("meters")
    jdbc_read(spark, "fact_price_5min").createOrReplaceTempView("prices")
    priced = spark.sql("""
        SELECT i.meter_id, i.interval_start, m.region_id, i.kwh, p.rrp, i.kwh * p.rrp / 1000.0 AS cost_aud
        FROM intervals i
        JOIN meters m USING (meter_id)
        LEFT ASOF JOIN prices p
          MATCH_CONDITION (i.interval_start >= p.interval_start)
          ON m.region_id = p.region_id
    """)
    n = upsert(priced, "fact_meter_interval", ["meter_id", "interval_start"])
    return f"fact_meter_interval={n}"


def load_profiles():
    profiles = spark.sql("""
        WITH hourly AS (
            SELECT meter_id, hour(interval_start + INTERVAL 10 HOURS) AS h, sum(kwh) AS kwh
            FROM intervals GROUP BY ALL
        )
        SELECT meter_id,
               vector_normalize(transform(sort_array(collect_list(struct(h, kwh))), x -> cast(x.kwh AS float))) AS profile
        FROM hourly GROUP BY meter_id HAVING count(*) = 24
    """).join(spark.read.parquet("/data/warehouse/meter_geo").select("meter_id", "segment"), "meter_id")
    profiles.cache().createOrReplaceTempView("profiles")
    similar = spark.sql("""
        SELECT a.meter_id, b.meter_id AS similar_meter_id, vector_cosine_similarity(a.profile, b.profile) AS similarity
        FROM profiles a JOIN (SELECT * FROM profiles) b
          EXACT NEAREST 4 BY SIMILARITY vector_cosine_similarity(a.profile, b.profile)
    """).where("meter_id <> similar_meter_id").withColumn(
        "rank", F.expr("row_number() OVER (PARTITION BY meter_id ORDER BY similarity DESC)").cast("int")).where("rank <= 3")
    out = profiles.selectExpr("meter_id", "transform(profile, x -> cast(x AS double)) AS profile",
                              "current_timestamp() AS updated_at")
    n1 = replace(out, "meter_profile")
    n2 = replace(similar, "meter_similarity", ["meter_id", "rank", "similar_meter_id", "similarity"])
    return f"meter_profile={n1} meter_similarity={n2}"


def device_diagnostics():
    raw = (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", "device_events")
           .option("startingOffsets", "earliest").load()
           .selectExpr("parse_json(cast(value AS string)) AS v"))
    raw.createOrReplaceTempView("raw_events")
    diag = spark.sql("""
        WITH cleaned AS (
            SELECT variant_set(variant_delete(v, '$.debug'), '$.pipeline', 'spark-4.3-batch') AS v FROM raw_events
        )
        SELECT variant_get(c.v, '$.device.id', 'string') AS device_id,
               variant_get(c.v, '$.device.meter_id', 'string') AS meter_id,
               variant_get(c.v, '$.ts', 'timestamp') AS event_ts,
               variant_get(c.v, '$.device.firmware', 'string') AS firmware,
               t.ordinal, t.component, t.temp_c, t.efficiency, coalesce(t.has_fault, false) AS has_fault
        FROM cleaned c,
        LATERAL JSON_TABLE(to_json(c.v), '$.readings[*]' COLUMNS (
            ordinal    FOR ORDINALITY,
            component  STRING  PATH '$.component',
            temp_c     DOUBLE  PATH '$.metrics.temp_c',
            efficiency DOUBLE  PATH '$.metrics.efficiency',
            has_fault  BOOLEAN PATH '$.fault'
        )) AS t
    """).dropDuplicates(["device_id", "event_ts", "ordinal"])
    return f"fact_device_diagnostic={upsert(diag, 'fact_device_diagnostic', ['device_id', 'event_ts', 'ordinal'])}"


def region_kpis():
    reads = jdbc_read(spark, "SELECT meter_id, region_id, kwh, rrp, cost_aud FROM fact_meter_read "
                             "WHERE read_ts > now() - interval '1 hour'")
    reads.write.mode("overwrite").parquet("/data/warehouse/reads_last_hour")
    spark.sql("CREATE TABLE IF NOT EXISTS reads_last_hour USING parquet LOCATION '/data/warehouse/reads_last_hour'")
    spark.sql("REFRESH TABLE reads_last_hour")
    spark.sql("DROP VIEW IF EXISTS region_metrics")
    spark.sql("""
        CREATE VIEW region_metrics WITH METRICS LANGUAGE YAML AS $$
        version: "0.1"
        source: reads_last_hour
        dimensions:
          - name: region_id
            expr: region_id
        measures:
          - name: meters
            expr: count(DISTINCT meter_id)
          - name: kwh
            expr: sum(kwh)
          - name: cost_aud
            expr: sum(cost_aud)
          - name: avg_rrp
            expr: avg(rrp)
        $$
    """)
    kpis = spark.sql("""
        SELECT region_id, MEASURE(meters) AS meters, MEASURE(kwh) AS kwh, MEASURE(cost_aud) AS cost_aud,
               MEASURE(avg_rrp) AS avg_rrp, current_timestamp() AS computed_at
        FROM region_metrics GROUP BY region_id
    """)
    return f"kpi_region={upsert(kpis, 'kpi_region', ['region_id'])}"


for step in [load_dimensions, dimension_changes, nearest_substations, interval_reads, load_profiles, device_diagnostics, region_kpis]:
    started = datetime.now(timezone.utc)
    try:
        detail = step()
        log_run(step.__name__, started, "ok", detail)
        print(step.__name__, "ok", detail, flush=True)
    except Exception:
        log_run(step.__name__, started, "failed", traceback.format_exc())
        traceback.print_exc()
