import os

import altair as alt
import pandas as pd
import psycopg
import streamlit as st

DSN = os.getenv("PG_DSN", "postgresql://lab:lab@localhost:5433/energy")
REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
REGION_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
REGION_SCALE = alt.Scale(domain=REGIONS, range=REGION_COLORS)

st.set_page_config(page_title="Spark 4.3 Energy Lab", layout="wide")


@st.cache_resource
def connection():
    conn = psycopg.connect(DSN, autocommit=True, options="-c search_path=energy")
    return conn


def q(sql, params=None):
    try:
        conn = connection()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d.name for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    except psycopg.OperationalError:
        st.cache_resource.clear()
        raise


def region_line(df, x, y, title, y_title):
    hover = alt.selection_point(fields=[x], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(df).encode(
        x=alt.X(f"{x}:T", title=None),
        y=alt.Y(f"{y}:Q", title=y_title),
        color=alt.Color("region_id:N", scale=REGION_SCALE, title="Region"))
    lines = base.mark_line(strokeWidth=2, interpolate="step-after" if "rrp" in y else "monotone")
    points = base.mark_point(size=70, filled=True).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip(f"{x}:T", format="%H:%M:%S"), "region_id:N", alt.Tooltip(f"{y}:Q", format=",.3f")]
    ).add_params(hover)
    rule = alt.Chart(df).mark_rule(color="#8a8984", strokeWidth=1).encode(x=f"{x}:T").transform_filter(hover)
    return (lines + points + rule).properties(title=title, height=260)


st.title("Spark 4.3 Energy Lab")
st.caption("Kafka → Spark 4.3 (Real-Time Mode, Auto CDC, ASOF JOIN, NEAREST BY, JSON_TABLE, metric views) → Postgres")

live_tab, meter_tab, model_tab = st.tabs(["Live", "Meter explorer", "Model & runs"])

with live_tab:
    @st.fragment(run_every="5s")
    def live():
        k = q("""
            SELECT
              (SELECT count(*) FROM fact_meter_read WHERE read_ts > now() - interval '1 minute') AS reads_1m,
              (SELECT coalesce(sum(kwh), 0) FROM fact_meter_read WHERE read_ts > now() - interval '1 hour') AS kwh_1h,
              (SELECT coalesce(sum(cost_aud), 0) FROM fact_meter_read WHERE read_ts > now() - interval '1 hour') AS cost_1h,
              (SELECT avg(rrp) FROM fact_price_5min p WHERE interval_start = (SELECT max(interval_start) FROM fact_price_5min)) AS rrp_now,
              (SELECT count(*) FROM fact_alert WHERE event_ts > now() - interval '15 minutes') AS alerts_15m,
              (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms) FROM fact_alert
                 WHERE event_ts > now() - interval '15 minutes') AS rtm_p50
        """).iloc[0]
        c = st.columns(6)
        c[0].metric("Reads, last minute", f"{int(k.reads_1m):,}")
        c[1].metric("kWh, last hour", f"{k.kwh_1h:,.2f}")
        c[2].metric("Cost, last hour", f"${k.cost_1h:,.2f}")
        c[3].metric("Avg RRP now ($/MWh)", "–" if pd.isna(k.rrp_now) else f"{k.rrp_now:,.2f}")
        c[4].metric("Alerts, 15 min", int(k.alerts_15m))
        c[5].metric("RTM alert latency p50", "–" if pd.isna(k.rtm_p50) else f"{k.rtm_p50:,.0f} ms")

        left, right = st.columns(2)
        prices = q("SELECT region_id, interval_start, rrp, source FROM fact_price_5min "
                   "WHERE interval_start > now() - interval '3 hours' ORDER BY interval_start")
        if not prices.empty:
            src = ", ".join(sorted(prices.source.unique()))
            left.altair_chart(region_line(prices, "interval_start", "rrp", f"Dispatch price, 5 min ({src})", "$/MWh"),
                              use_container_width=True)
        windows = q("""SELECT region_id, window_start, kwh FROM fact_rtm_window
                       WHERE window_start > now() - interval '10 minutes' ORDER BY window_start""")
        if not windows.empty:
            right.altair_chart(region_line(windows, "window_start", "kwh",
                                           "kWh per 10 s window (stateful Real-Time Mode)", "kWh"),
                               use_container_width=True)

        usage = q("""SELECT m.region_id, u.minute, sum(u.kwh) AS kwh, sum(u.cost_aud) AS cost_aud
                     FROM fact_usage_1m u JOIN dim_meter m USING (meter_id)
                     WHERE u.minute > now() - interval '30 minutes' AND u.minute < date_trunc('minute', now())
                     GROUP BY 1, 2 ORDER BY 2""")
        a, b = st.columns(2)
        if not usage.empty:
            a.altair_chart(region_line(usage, "minute", "cost_aud", "Cost per minute, priced by ASOF JOIN", "AUD"),
                           use_container_width=True)
        alerts = q("""SELECT meter_id, kind, round(value::numeric, 2) AS value, event_ts, round(latency_ms::numeric, 1) AS latency_ms
                      FROM fact_alert ORDER BY event_ts DESC LIMIT 15""")
        b.markdown("**Alerts from Real-Time Mode** (latency = alert produced − read produced)")
        b.dataframe(alerts, hide_index=True, use_container_width=True, height=260)

    live()

with meter_tab:
    meters = q("SELECT meter_id FROM dim_meter ORDER BY meter_id")
    if meters.empty:
        st.info("Waiting for the first Auto CDC pipeline run.")
    else:
        meter_id = st.selectbox("Meter", meters.meter_id)
        cur = q("SELECT * FROM v_meter_current WHERE meter_id = %s", (meter_id,))
        if not cur.empty:
            r = cur.iloc[0]
            c = st.columns(5)
            c[0].metric("Customer", r.customer)
            c[1].metric("Segment", r.segment)
            c[2].metric("Tariff", r.tariff)
            c[3].metric("Status", r.status)
            c[4].metric("Nearest substation", r.substation or "–",
                        f"{r.distance_km:.1f} km" if r.substation else None, delta_color="off")

        h_left, h_right = st.columns(2)
        h_left.markdown("**SCD Type 2 history** (Auto CDC, `STORED AS SCD TYPE 2`)")
        h_left.dataframe(q("""SELECT tariff, status, customer, valid_from, valid_to, is_current
                              FROM dim_meter_history WHERE meter_id = %s ORDER BY start_seq""", (meter_id,)),
                         hide_index=True, use_container_width=True)
        h_right.markdown("**Change feed of the SCD1 table** (`CHANGES FROM VERSION … WITH ('computeUpdates')`)")
        h_right.dataframe(q("""SELECT commit_version AS version, commit_ts, change_type, tariff, status, customer
                               FROM dim_meter_change_log WHERE meter_id = %s
                               ORDER BY commit_version, change_type DESC""", (meter_id,)),
                          hide_index=True, use_container_width=True)

        left, right = st.columns(2)
        profile = q("SELECT profile FROM meter_profile WHERE meter_id = %s", (meter_id,))
        similar = q("""SELECT s.rank, s.similar_meter_id, round(s.similarity::numeric, 4) AS cosine, m.segment, m.region_id
                       FROM meter_similarity s JOIN dim_meter m ON m.meter_id = s.similar_meter_id
                       WHERE s.meter_id = %s ORDER BY s.rank""", (meter_id,))
        if not profile.empty:
            ids = [meter_id] + similar.similar_meter_id.tolist()
            prof = q("SELECT meter_id, profile FROM meter_profile WHERE meter_id = ANY(%s)", (ids,))
            rows = [{"meter": m, "hour": h, "load": v} for m, p in zip(prof.meter_id, prof.profile) for h, v in enumerate(p)]
            chart = alt.Chart(pd.DataFrame(rows)).mark_line(strokeWidth=2).encode(
                x=alt.X("hour:Q", title="Hour (AEST)", scale=alt.Scale(domain=[0, 23])),
                y=alt.Y("load:Q", title="Normalised load"),
                color=alt.Color("meter:N", scale=alt.Scale(domain=ids, range=["#2a78d6", "#eb6834", "#1baf7a", "#6250d6"]),
                                title="Meter"),
                tooltip=["meter:N", "hour:Q", alt.Tooltip("load:Q", format=".3f")]
            ).properties(title="24h load profile vs nearest neighbours (NEAREST BY + vector_cosine_similarity)", height=280)
            left.altair_chart(chart, use_container_width=True)
        right.markdown("**Most similar meters**")
        right.dataframe(similar, hide_index=True, use_container_width=True)
        right.markdown("**Device diagnostics** (VARIANT edit + JSON_TABLE)")
        right.dataframe(q("""SELECT event_ts, firmware, component, temp_c, efficiency, has_fault
                             FROM fact_device_diagnostic WHERE meter_id = %s ORDER BY event_ts DESC, ordinal LIMIT 12""",
                          (meter_id,)), hide_index=True, use_container_width=True)

        intervals = q("""SELECT interval_start, kwh, rrp, cost_aud FROM fact_meter_interval
                         WHERE meter_id = %s ORDER BY interval_start""", (meter_id,))
        if not intervals.empty:
            st.altair_chart(alt.Chart(intervals).mark_bar(color="#2a78d6", cornerRadiusTopLeft=2, cornerRadiusTopRight=2).encode(
                x=alt.X("interval_start:T", title=None), y=alt.Y("kwh:Q", title="kWh"),
                tooltip=[alt.Tooltip("interval_start:T", format="%d %b %H:%M"), alt.Tooltip("kwh:Q", format=".3f"),
                         alt.Tooltip("rrp:Q", title="ASOF RRP", format=".2f"), alt.Tooltip("cost_aud:Q", format=".4f")]
            ).properties(title="15-min interval reads (24h backfill) — LEFT ASOF JOIN leaves cost empty before price history",
                         height=220), use_container_width=True)

        geo = q("SELECT meter_id, lat, lon FROM dim_meter")
        st.markdown("**Meter locations** (GEOGRAPHY from WKB, SRID 4326)")
        st.map(geo, latitude="lat", longitude="lon", size=900)

with model_tab:
    st.markdown("**Region KPIs** from the metric view `region_metrics` (`MEASURE()` over the last hour)")
    st.dataframe(q("SELECT * FROM kpi_region ORDER BY region_id"), hide_index=True, use_container_width=True)
    st.markdown("**Dimension change feed** — `SELECT … FROM lake.energy.dim_meter CHANGES FROM VERSION n EXCLUSIVE` on the Iceberg SCD1 table")
    st.dataframe(q("""SELECT commit_version AS version, commit_ts, change_type, meter_id, tariff, status, customer
                      FROM dim_meter_change_log ORDER BY commit_version DESC, meter_id, change_type DESC LIMIT 40"""),
                 hide_index=True, use_container_width=True)
    st.markdown("**Row counts**")
    st.dataframe(q("""
        SELECT 'dim_meter' t, count(*) FROM dim_meter UNION ALL
        SELECT 'dim_meter_history', count(*) FROM dim_meter_history UNION ALL
        SELECT 'dim_meter_change_log', count(*) FROM dim_meter_change_log UNION ALL
        SELECT 'bridge_meter_substation', count(*) FROM bridge_meter_substation UNION ALL
        SELECT 'fact_price_5min', count(*) FROM fact_price_5min UNION ALL
        SELECT 'fact_meter_read', count(*) FROM fact_meter_read UNION ALL
        SELECT 'fact_meter_interval', count(*) FROM fact_meter_interval UNION ALL
        SELECT 'fact_usage_1m', count(*) FROM fact_usage_1m UNION ALL
        SELECT 'fact_alert', count(*) FROM fact_alert UNION ALL
        SELECT 'fact_rtm_window', count(*) FROM fact_rtm_window UNION ALL
        SELECT 'fact_device_diagnostic', count(*) FROM fact_device_diagnostic UNION ALL
        SELECT 'meter_similarity', count(*) FROM meter_similarity
    """), hide_index=True)
    st.markdown("**Batch runs**")
    st.dataframe(q("SELECT job, status, started_at, finished_at, left(detail, 200) AS detail FROM pipeline_run "
                   "ORDER BY run_id DESC LIMIT 30"), hide_index=True, use_container_width=True)
