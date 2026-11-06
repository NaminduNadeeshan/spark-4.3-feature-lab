CREATE SCHEMA IF NOT EXISTS energy;
SET search_path TO energy;

CREATE TABLE dim_region (
    region_id   text PRIMARY KEY,
    region_name text NOT NULL,
    state       text NOT NULL
);

INSERT INTO dim_region VALUES
    ('NSW1', 'New South Wales', 'NSW'),
    ('QLD1', 'Queensland', 'QLD'),
    ('VIC1', 'Victoria', 'VIC'),
    ('SA1',  'South Australia', 'SA'),
    ('TAS1', 'Tasmania', 'TAS');

CREATE TABLE dim_meter (
    meter_id     text PRIMARY KEY,
    region_id    text REFERENCES dim_region,
    customer     text,
    segment      text,
    tariff       text,
    status       text,
    lat          double precision,
    lon          double precision,
    geog_wkb     bytea,
    last_seq     bigint,
    loaded_at    timestamptz DEFAULT now()
);

CREATE TABLE dim_meter_history (
    meter_id     text NOT NULL,
    region_id    text,
    customer     text,
    segment      text,
    tariff       text,
    status       text,
    lat          double precision,
    lon          double precision,
    start_seq    bigint NOT NULL,
    end_seq      bigint,
    valid_from   timestamptz NOT NULL,
    valid_to     timestamptz,
    is_current   boolean NOT NULL,
    loaded_at    timestamptz DEFAULT now(),
    PRIMARY KEY (meter_id, start_seq)
);

CREATE TABLE dim_meter_change_log (
    commit_version bigint NOT NULL,
    commit_ts      timestamptz NOT NULL,
    change_type    text NOT NULL,
    meter_id       text NOT NULL,
    region_id      text,
    customer       text,
    segment        text,
    tariff         text,
    status         text,
    loaded_at      timestamptz DEFAULT now(),
    PRIMARY KEY (commit_version, meter_id, change_type)
);

CREATE TABLE dim_substation (
    substation_id text PRIMARY KEY,
    name          text,
    region_id     text REFERENCES dim_region,
    lat           double precision,
    lon           double precision
);

INSERT INTO dim_substation VALUES
    ('SUB-NSW-01', 'Sydney CBD',        'NSW1', -33.870, 151.205),
    ('SUB-NSW-02', 'Parramatta',        'NSW1', -33.815, 151.003),
    ('SUB-NSW-03', 'Sutherland',        'NSW1', -34.031, 151.058),
    ('SUB-QLD-01', 'Brisbane North',    'QLD1', -27.380, 153.030),
    ('SUB-QLD-02', 'Logan',             'QLD1', -27.640, 153.110),
    ('SUB-VIC-01', 'Melbourne CBD',     'VIC1', -37.814, 144.963),
    ('SUB-VIC-02', 'Dandenong',         'VIC1', -37.987, 145.215),
    ('SUB-VIC-03', 'Sunshine',          'VIC1', -37.788, 144.832),
    ('SUB-SA-01',  'Adelaide CBD',      'SA1',  -34.928, 138.600),
    ('SUB-SA-02',  'Salisbury',         'SA1',  -34.760, 138.640),
    ('SUB-TAS-01', 'Hobart',            'TAS1', -42.882, 147.327),
    ('SUB-TAS-02', 'Glenorchy',         'TAS1', -42.833, 147.276);

CREATE TABLE bridge_meter_substation (
    meter_id      text,
    substation_id text,
    rank          int,
    distance_km   double precision,
    computed_at   timestamptz DEFAULT now(),
    PRIMARY KEY (meter_id, rank)
);

CREATE TABLE fact_price_5min (
    region_id      text REFERENCES dim_region,
    interval_start timestamptz,
    interval_end   timestamptz,
    rrp            double precision,
    source         text,
    PRIMARY KEY (region_id, interval_start)
);

CREATE TABLE fact_meter_read (
    meter_id        text,
    read_ts         timestamptz,
    region_id       text,
    kwh             double precision,
    power_kw        double precision,
    voltage         double precision,
    rrp             double precision,
    price_interval  timestamptz,
    cost_aud        double precision,
    ingested_at     timestamptz DEFAULT now()
);
CREATE INDEX ON fact_meter_read (read_ts DESC);
CREATE INDEX ON fact_meter_read (meter_id, read_ts DESC);

CREATE TABLE fact_meter_interval (
    meter_id        text,
    interval_start  timestamptz,
    region_id       text,
    kwh             double precision,
    rrp             double precision,
    cost_aud        double precision,
    PRIMARY KEY (meter_id, interval_start)
);

CREATE TABLE fact_usage_1m (
    meter_id     text,
    minute       timestamptz,
    reads        int,
    kwh          double precision,
    max_kw       double precision,
    cost_aud     double precision,
    PRIMARY KEY (meter_id, minute)
);

CREATE TABLE fact_alert (
    meter_id    text,
    event_ts    timestamptz,
    alert_ts    timestamptz,
    kind        text,
    value       double precision,
    latency_ms  double precision,
    PRIMARY KEY (meter_id, event_ts, kind)
);

CREATE TABLE fact_rtm_window (
    region_id    text,
    window_start timestamptz,
    reads        bigint,
    kwh          double precision,
    emitted_at   timestamptz,
    PRIMARY KEY (region_id, window_start)
);

CREATE TABLE meter_profile (
    meter_id    text PRIMARY KEY,
    profile     double precision[],
    updated_at  timestamptz DEFAULT now()
);

CREATE TABLE meter_similarity (
    meter_id         text,
    rank             int,
    similar_meter_id text,
    similarity       double precision,
    computed_at      timestamptz DEFAULT now(),
    PRIMARY KEY (meter_id, rank)
);

CREATE TABLE fact_device_diagnostic (
    device_id   text,
    meter_id    text,
    event_ts    timestamptz,
    firmware    text,
    ordinal     bigint,
    component   text,
    temp_c      double precision,
    efficiency  double precision,
    has_fault   boolean,
    PRIMARY KEY (device_id, event_ts, ordinal)
);

CREATE TABLE kpi_region (
    region_id    text PRIMARY KEY,
    meters       bigint,
    kwh          double precision,
    cost_aud     double precision,
    avg_rrp      double precision,
    computed_at  timestamptz
);

CREATE TABLE pipeline_run (
    run_id      bigserial PRIMARY KEY,
    job         text,
    started_at  timestamptz,
    finished_at timestamptz,
    status      text,
    detail      text
);

CREATE VIEW v_meter_current AS
SELECT m.*, r.region_name, b.substation_id, s.name AS substation, b.distance_km
FROM dim_meter m
JOIN dim_region r USING (region_id)
LEFT JOIN bridge_meter_substation b ON b.meter_id = m.meter_id AND b.rank = 1
LEFT JOIN dim_substation s ON s.substation_id = b.substation_id;

ALTER DATABASE energy SET search_path TO energy, public;
