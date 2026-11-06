CREATE STREAMING TABLE lake.energy.dim_meter_history;

CREATE FLOW dim_meter_history_cdc AS AUTO CDC INTO lake.energy.dim_meter_history
FROM STREAM(meter_changes)
KEYS (meter_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY seq
COLUMNS * EXCEPT (op, changed_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON (tariff, status, customer, lat, lon);
