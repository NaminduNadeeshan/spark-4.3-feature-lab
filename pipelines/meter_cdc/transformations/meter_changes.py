import os

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

CHANGE_SCHEMA = ("meter_id string, region_id string, customer string, segment string, tariff string, status string, "
                 "lat double, lon double, op string, seq long, changed_at timestamp")


@dp.temporary_view
def meter_changes() -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
            .option("subscribe", "meter_changes")
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr(f"from_json(cast(value as string), '{CHANGE_SCHEMA}') AS r")
            .select("r.*"))


dp.create_streaming_table(
    "lake.energy.dim_meter",
    table_properties={"changelog.row-id": "meter_id"},
)

dp.create_auto_cdc_flow(
    target="lake.energy.dim_meter",
    source="meter_changes",
    keys=["meter_id"],
    sequence_by="seq",
    apply_as_deletes="op = 'DELETE'",
    except_column_list=["op", "changed_at"],
    stored_as_scd_type=1,
)
