import csv
import io
import json
import logging
import math
import os
import random
import re
import threading
import time
import zipfile
from datetime import datetime, timedelta, timezone

import requests
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("generator")
logging.getLogger("kafka").setLevel(logging.WARNING)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
PRICE_SOURCE = os.getenv("PRICE_SOURCE", "aemo")
METERS = int(os.getenv("METERS", "40"))
READ_INTERVAL = float(os.getenv("READ_INTERVAL_SEC", "2"))
NEM_TZ = timezone(timedelta(hours=10))
NEMWEB = "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/"

REGIONS = {
    "NSW1": (-33.87, 151.21),
    "QLD1": (-27.47, 153.03),
    "VIC1": (-37.81, 144.96),
    "SA1": (-34.93, 138.60),
    "TAS1": (-42.88, 147.33),
}
SEGMENTS = ["residential", "residential", "residential", "solar", "ev", "commercial"]
TARIFFS = ["flat", "tou", "demand"]
FIRST = ["Alex", "Sam", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Jamie", "Quinn", "Avery"]
LAST = ["Nguyen", "Smith", "Patel", "Brown", "Wilson", "Chen", "Taylor", "Kelly", "Singh", "Martin"]
TOPICS = ["meter_changes", "meter_reads", "meter_intervals", "prices", "device_events", "alerts", "rtm_region_kwh"]


def now():
    return datetime.now(timezone.utc)


def iso(ts):
    return ts.isoformat(timespec="microseconds").replace("+00:00", "Z")


def seq():
    return time.time_ns() // 1000


class Meter:
    def __init__(self, i):
        self.meter_id = f"MTR-{i:04d}"
        self.region_id = random.choice(list(REGIONS))
        lat, lon = REGIONS[self.region_id]
        self.lat = round(lat + random.uniform(-0.35, 0.35), 5)
        self.lon = round(lon + random.uniform(-0.35, 0.35), 5)
        self.segment = random.choice(SEGMENTS)
        self.tariff = random.choice(TARIFFS)
        self.status = "active"
        self.customer = f"{random.choice(FIRST)} {random.choice(LAST)}"
        self.scale = random.uniform(0.6, 1.6)

    def record(self, op="UPSERT", at=None):
        return {
            "meter_id": self.meter_id,
            "region_id": self.region_id,
            "customer": self.customer,
            "segment": self.segment,
            "tariff": self.tariff,
            "status": self.status,
            "lat": self.lat,
            "lon": self.lon,
            "op": op,
            "seq": at or seq(),
            "changed_at": iso(now()),
        }

    def kw(self, ts):
        h = ts.astimezone(NEM_TZ).hour + ts.astimezone(NEM_TZ).minute / 60
        base = 0.35 + 0.9 * math.exp(-((h - 7.5) ** 2) / 3) + 1.6 * math.exp(-((h - 19) ** 2) / 5)
        if self.segment == "solar":
            base -= 3.2 * max(0.0, math.sin(math.pi * (h - 6) / 13)) if 6 <= h <= 19 else 0
        elif self.segment == "ev":
            base += 6.5 if (h >= 22 or h < 3) else 0
        elif self.segment == "commercial":
            base = 2.0 + (9.0 if 8 <= h <= 18 else 1.0)
        return round(self.scale * base * random.uniform(0.85, 1.15), 4)


def ensure_topics():
    for _ in range(60):
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
            existing = set(admin.list_topics())
            new = [NewTopic(t, num_partitions=2, replication_factor=1) for t in TOPICS if t not in existing]
            if new:
                admin.create_topics(new)
            admin.close()
            return
        except Exception as e:
            log.warning("waiting for kafka: %s", e)
            time.sleep(2)
    raise SystemExit("kafka not reachable")


def wait_for_leaders():
    for _ in range(60):
        meta = producer.partitions_for("meter_changes")
        ready = all(producer.partitions_for(t) for t in TOPICS)
        if meta and ready:
            return
        time.sleep(1)
    raise SystemExit("kafka partitions have no leader")


producer = None


def send(topic, key, value, ts=None):
    producer.send(topic, key=key.encode(), value=json.dumps(value).encode(),
                  timestamp_ms=int((ts or now()).timestamp() * 1000))


def emit_history(meters):
    end = now().replace(minute=(now().minute // 15) * 15, second=0, microsecond=0)
    start = end - timedelta(hours=24)
    n = 0
    for m in meters:
        t = start
        while t < end:
            kwh = round(m.kw(t + timedelta(minutes=7.5)) * 0.25, 5)
            send("meter_intervals", m.meter_id, {"meter_id": m.meter_id, "interval_start": iso(t), "interval_minutes": 15,
                                                 "kwh": kwh, "quality": "A"}, t)
            t += timedelta(minutes=15)
            n += 1
    producer.flush()
    log.info("backfilled %s interval reads (24h, NEM12-style 15 min)", n)


class Prices:
    def __init__(self):
        self.latest = {r: 100.0 for r in REGIONS}
        self.seen = set()
        self.lock = threading.Lock()

    def publish(self, region, start, rrp, source):
        key = (region, start)
        if key in self.seen:
            return
        self.seen.add(key)
        with self.lock:
            self.latest[region] = rrp
        send("prices", region, {"region_id": region, "interval_start": iso(start),
                                "interval_end": iso(start + timedelta(minutes=5)), "rrp": rrp, "source": source}, start)

    def aemo_files(self, limit):
        html = requests.get(NEMWEB, timeout=30).text
        files = sorted(set(re.findall(r"PUBLIC_DISPATCHIS_\d+_\d+\.zip", html)))
        return files[-limit:]

    def load_aemo(self, name):
        raw = requests.get(NEMWEB + name, timeout=30).content
        z = zipfile.ZipFile(io.BytesIO(raw))
        text = z.read(z.namelist()[0]).decode()
        out = []
        for cols in csv.reader(text.splitlines()):
            if cols[:3] != ["D", "DISPATCH", "PRICE"]:
                continue
            settlement = datetime.strptime(cols[4], "%Y/%m/%d %H:%M:%S").replace(tzinfo=NEM_TZ)
            region, intervention, rrp = cols[6], cols[8], float(cols[9])
            if intervention == "0" and region in REGIONS:
                out.append((region, (settlement - timedelta(minutes=5)).astimezone(timezone.utc), rrp))
        return out

    def synthetic(self, start):
        for r in REGIONS:
            h = start.astimezone(NEM_TZ).hour
            target = 60 + 90 * math.exp(-((h - 18.5) ** 2) / 4) - 50 * math.exp(-((h - 12.5) ** 2) / 6)
            self.publish(r, start, round(0.7 * self.latest[r] + 0.3 * target + random.gauss(0, 8), 2), "SYNTH")

    def run(self):
        use_aemo = PRICE_SOURCE == "aemo"
        if use_aemo:
            try:
                for f in self.aemo_files(24):
                    for region, start, rrp in self.load_aemo(f):
                        self.publish(region, start, rrp, "AEMO")
                log.info("backfilled AEMO dispatch prices: %s intervals", len(self.seen))
            except Exception as e:
                log.warning("AEMO unavailable (%s), falling back to synthetic prices", e)
                use_aemo = False
        if not use_aemo:
            t = now().replace(second=0, microsecond=0)
            t -= timedelta(minutes=t.minute % 5)
            for i in range(24, -1, -1):
                self.synthetic(t - timedelta(minutes=5 * i))
        while True:
            time.sleep(30)
            try:
                if use_aemo:
                    for f in self.aemo_files(2):
                        for region, start, rrp in self.load_aemo(f):
                            self.publish(region, start, rrp, "AEMO")
                else:
                    t = now().replace(second=0, microsecond=0)
                    self.synthetic(t - timedelta(minutes=t.minute % 5))
                producer.flush()
            except Exception as e:
                log.warning("price poll failed: %s", e)


def device_event(m, ts):
    readings = [
        {"component": "meter", "metrics": {"temp_c": round(random.uniform(25, 45), 1)}},
        {"component": "comms", "metrics": {"rssi": random.randint(-95, -55)}, "fault": random.random() < 0.05},
    ]
    if m.segment == "solar":
        readings.append({"component": "inverter", "metrics": {"temp_c": round(random.uniform(35, 75), 1),
                                                              "efficiency": round(random.uniform(0.9, 0.98), 3)},
                         "fault": random.random() < 0.08})
    if m.segment == "ev":
        readings.append({"component": "charger", "metrics": {"temp_c": round(random.uniform(30, 60), 1)},
                         "fault": random.random() < 0.04})
    return {"device": {"id": f"DEV-{m.meter_id[4:]}", "meter_id": m.meter_id, "firmware": random.choice(["3.2.1", "3.3.0", "4.0.0-rc1"])},
            "ts": iso(ts), "readings": readings, "debug": {"heap_kb": random.randint(200, 900), "boot_id": random.randint(1, 99999)}}


def mutate(meters, retired):
    m = random.choice(meters)
    roll = random.random()
    if roll < 0.35:
        m.tariff = random.choice([t for t in TARIFFS if t != m.tariff])
        send("meter_changes", m.meter_id, m.record())
        log.info("CDC tariff change %s -> %s", m.meter_id, m.tariff)
    elif roll < 0.55:
        m.status = "suspended" if m.status == "active" else "active"
        send("meter_changes", m.meter_id, m.record())
        log.info("CDC status change %s -> %s", m.meter_id, m.status)
    elif roll < 0.7:
        m.customer = f"{random.choice(FIRST)} {random.choice(LAST)}"
        send("meter_changes", m.meter_id, m.record())
        log.info("CDC new occupant %s", m.meter_id)
    elif roll < 0.8:
        stale = m.record(at=seq() - 3_600_000_000)
        stale["tariff"] = "legacy"
        send("meter_changes", m.meter_id, stale)
        log.info("CDC late out-of-order event %s", m.meter_id)
    elif roll < 0.88 and len(meters) > 10:
        meters.remove(m)
        retired.append(m)
        send("meter_changes", m.meter_id, m.record(op="DELETE"))
        log.info("CDC delete %s", m.meter_id)
    elif retired:
        r = retired.pop(0)
        r.status = "active"
        meters.append(r)
        send("meter_changes", r.meter_id, r.record())
        log.info("CDC re-activate %s", r.meter_id)


def main():
    global producer
    ensure_topics()
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, linger_ms=50, acks=1, retries=20, retry_backoff_ms=500)
    wait_for_leaders()
    random.seed(42)
    meters = [Meter(i) for i in range(1, METERS + 1)]
    random.seed()
    retired = []
    base = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)
    for i, m in enumerate(meters):
        send("meter_changes", m.meter_id, m.record(at=base + i))
    emit_history(meters)
    prices = Prices()
    threading.Thread(target=prices.run, daemon=True).start()
    tick = 0
    while True:
        started = time.time()
        ts = now()
        for m in list(meters):
            if m.status != "active":
                continue
            kw = m.kw(ts)
            voltage = round(random.gauss(240, 2.5), 2)
            if random.random() < 0.004:
                voltage = round(random.uniform(254, 262), 2)
            if random.random() < 0.003:
                kw = round(kw + random.uniform(30, 45), 3)
            read = {"meter_id": m.meter_id, "region_id": m.region_id, "read_ts": iso(ts),
                    "power_kw": kw, "kwh": round(kw * READ_INTERVAL / 3600, 6), "voltage": voltage}
            send("meter_reads", m.meter_id, read, ts)
            if random.random() < 0.01:
                send("meter_reads", m.meter_id, read, ts)
        if tick % max(1, int(5 / READ_INTERVAL)) == 0:
            m = random.choice(meters)
            send("device_events", m.meter_id, device_event(m, ts), ts)
        if tick % max(1, int(20 / READ_INTERVAL)) == 0 and tick > 0:
            mutate(meters, retired)
        producer.flush()
        tick += 1
        time.sleep(max(0.0, READ_INTERVAL - (time.time() - started)))


if __name__ == "__main__":
    main()
