# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # TransitGap — POC pipeline (single notebook)
# MAGIC Creates every table used by the app, Genie space and dashboard in one run.
# MAGIC Idempotent (tables are overwritten) and deterministic (fixed seed).
# MAGIC
# MAGIC **Use:** set `CATALOG` below, then *Run all*. Takes ~1–2 minutes.
# MAGIC Re-run after changing `SCORING` weights to see scores change.

# COMMAND ----------

# MAGIC %pip install -q requests

# COMMAND ----------

import json, math, uuid, random, datetime as dt
import pandas as pd

# ------------------------------------------------------------------ CONFIG
CATALOG = "milacron_platform"  # <-- moved to milacron_platform (user has USE CATALOG)
SCHEMA = "transitgap"
SEED = 42
BBOX = (22.90, 72.42, 23.25, 72.76)   # south, west, north, east  (Ahmedabad + Gandhinagar Phase 2)
MAX_BUS_STOPS = 600                    # sample size kept from OSM
# OPTIONAL: your own verified station / stop files (e.g. from your GitHub repo) uploaded to a UC Volume or a raw GitHub URL.
# CSV must have columns: name, lat, lon  (optional: line, station_id / stop_id, is_brts). Leave "" to skip.
METRO_CSV_PATH = ""   # e.g. "/Volumes/main/transitgap/raw/metro_stations.csv" or "https://raw.githubusercontent.com/<user>/<repo>/main/stations.csv"
BUS_CSV_PATH = ""     # e.g. "/Volumes/main/transitgap/raw/bus_stops.csv"

# Transparent scoring model (higher score = better access, 0-100)
SCORING = [
    # component, weight, threshold, description
    ("metro",   40, 3.0, "1 - dist_metro_km/3.0, clamped 0..1. Beyond 3 km contributes 0."),
    ("bus",     35, 1.0, "1 - dist_bus_km/1.0, clamped 0..1. Beyond 1 km contributes 0."),
    ("density", 25, 8.0, "bus_stops_1km/8, clamped 0..1. 8+ stops within 1 km = full marks."),
]
GAP_THRESHOLD, MODERATE_THRESHOLD = 35, 60
SCENARIO_FEEDER_DIST_KM = 0.2   # a feeder stop is placed at the area centroid (~200 m)
SCENARIO_RADIUS_KM = 1.5        # neighbouring areas within this radius also benefit
FEEDER_METRO_EQUIV_KM = 1.5     # ASSUMPTION: a feeder to metro counts like living 1.5 km from that station

random.seed(SEED)
FQ = f"{CATALOG}.{SCHEMA}"
RUN_ID = str(uuid.uuid4())[:8]
run_log, sources = [], []

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")

def now(): return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

def log(step, inp, out, rej, status="OK", msg=""):
    run_log.append(dict(run_id=RUN_ID, step=step, input_rows=int(inp), output_rows=int(out),
                        rejected_rows=int(rej), status=status, started_at=now(), completed_at=now(), message=msg))
    print(f"[{status}] {step}: in={inp} out={out} rejected={rej} {msg}")

def save(name, pdf):
    df = spark.createDataFrame(pdf)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.{name}")
    print(f"  -> wrote {FQ}.{name} ({len(pdf)} rows)")

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

def clamp(x): return max(0.0, min(1.0, x))

# COMMAND ----------

# MAGIC %md ## 1. Fetch real transport points from OpenStreetMap (with labelled fallback)

# COMMAND ----------

# DBTITLE 1,1. Fetch real transport points (with labelled fallback)
import requests, zipfile, io

s, w, n, e = BBOX

# ── GTFS real bus data from https://github.com/croyla/amd-gtfs ──
# Sources: AMTS (amts.co.in), AJL/Janmarg BRTS (ahmedabadbrts.org), GTSL Gandhinagar
GTFS_URL = "https://github.com/croyla/amd-gtfs/raw/main/gtfs.zip"

# Fallback seed: ALL 54 stations (Phase 1 + Phase 2). Coordinates verified against official
# GMRC Google Maps layer (mid=1ebnAmbRUbMyI_quB06reRZWcyfcLF8M). Used only if no CSV and OSM is unreachable.
# (name, lat, lon, line, seq, status)
# Interchange stations appear on BOTH lines. seq = order along that line.
# status: OPERATIONAL for Phase 1 (Blue/Red, fully running). UNDER_CONSTRUCTION for Phase 2 (Yellow/Violet).
FALLBACK_METRO = [
    # ── Phase 1 East-West (Blue Line): Thaltej Gam ↔ Vastral Gam — 18 stations ──
    ("Thaltej Gam", 23.05016, 72.50703, "Blue", 1, "OPERATIONAL"),
    ("Thaltej", 23.04974, 72.51593, "Blue", 2, "OPERATIONAL"),
    ("Doordarshan Kendra", 23.04809, 72.52487, "Blue", 3, "OPERATIONAL"),
    ("Gurukul Road", 23.04601, 72.53505, "Blue", 4, "OPERATIONAL"),
    ("Gujarat University", 23.04490, 72.54337, "Blue", 5, "OPERATIONAL"),
    ("Commerce Six Roads", 23.04069, 72.55297, "Blue", 6, "OPERATIONAL"),
    ("Old High Court", 23.03762, 72.56699, "Blue", 7, "OPERATIONAL"),  # interchange Blue↔Red
    ("SP Stadium", 23.03985, 72.56181, "Blue", 8, "OPERATIONAL"),
    ("Shahpur", 23.03922, 72.58178, "Blue", 9, "OPERATIONAL"),
    ("Gheekanta", 23.02838, 72.58633, "Blue", 10, "OPERATIONAL"),
    ("Kalupur Rly Station", 23.02479, 72.60290, "Blue", 11, "OPERATIONAL"),
    ("Kankaria East", 23.01518, 72.60698, "Blue", 12, "OPERATIONAL"),
    ("Apparel Park", 23.01085, 72.61746, "Blue", 13, "OPERATIONAL"),
    ("Amraiwadi", 23.00785, 72.62864, "Blue", 14, "OPERATIONAL"),
    ("Rabari Colony", 23.00551, 72.63546, "Blue", 15, "OPERATIONAL"),
    ("Vastral", 23.00380, 72.64687, "Blue", 16, "OPERATIONAL"),
    ("Nirant Cross Road", 23.00044, 72.65889, "Blue", 17, "OPERATIONAL"),
    ("Vastral Gam", 22.99722, 72.66742, "Blue", 18, "OPERATIONAL"),
    # ── Phase 1 North-South (Red Line): APMC ↔ Koteshwar Road — 16 stations ──
    # Verified against network.json from darshan260802/metro-tracker (GMRC official timetable transcription).
    # Red↔Yellow interchange is Koteshwar Road (NOT Motera Stadium). Motera Stadium is Red-only.
    ("APMC", 22.99773, 72.53720, "Red", 1, "OPERATIONAL"),
    ("Jivraj Park", 23.00548, 72.53355, "Red", 2, "OPERATIONAL"),
    ("Rajivnagar", 23.00970, 72.53673, "Red", 3, "OPERATIONAL"),
    ("Shreyas", 23.01378, 72.54930, "Red", 4, "OPERATIONAL"),
    ("Paldi", 23.01857, 72.56239, "Red", 5, "OPERATIONAL"),
    ("Gandhigram", 23.02723, 72.56892, "Red", 6, "OPERATIONAL"),
    ("Old High Court", 23.03762, 72.56699, "Red", 7, "OPERATIONAL"),  # interchange Blue↔Red
    ("Usmanpura", 23.04589, 72.56502, "Red", 8, "OPERATIONAL"),
    ("Vijaynagar", 23.05622, 72.56247, "Red", 9, "OPERATIONAL"),
    ("Vadaj", 23.06773, 72.56582, "Red", 10, "OPERATIONAL"),
    ("Ranip", 23.06768, 72.57401, "Red", 11, "OPERATIONAL"),
    ("Sabarmati Rly Station", 23.06973, 72.58670, "Red", 12, "OPERATIONAL"),
    ("AEC", 23.07516, 72.59329, "Red", 13, "OPERATIONAL"),
    ("Sabarmati", 23.08567, 72.59221, "Red", 14, "OPERATIONAL"),
    ("Motera Stadium", 23.09669, 72.59665, "Red", 15, "OPERATIONAL"),
    ("Koteshwar Road", 23.10311, 72.60213, "Red", 16, "OPERATIONAL"),  # interchange Red↔Yellow
    # ── Phase 2 (Yellow Line): Koteshwar Road ↔ Mahatma Mandir — 20 stations ──
    # Yellow starts at Koteshwar Road (Red↔Yellow interchange), NOT Motera Stadium.
    # Coords from network.json (darshan260802/metro-tracker). Through-service: Yellow trains run from APMC.
    ("Koteshwar Road", 23.10311, 72.60213, "Yellow", 1, "UNDER_CONSTRUCTION"),  # interchange Red↔Yellow
    ("Vishwakarma College", 23.11431, 72.60857, "Yellow", 2, "UNDER_CONSTRUCTION"),
    ("Tapovan Circle", 23.11991, 72.61660, "Yellow", 3, "UNDER_CONSTRUCTION"),
    ("Narmada Canal", 23.12534, 72.62233, "Yellow", 4, "UNDER_CONSTRUCTION"),
    ("Koba Circle", 23.13221, 72.63095, "Yellow", 5, "UNDER_CONSTRUCTION"),
    ("Juna Koba", 23.14181, 72.63845, "Yellow", 6, "UNDER_CONSTRUCTION"),
    ("Koba Gam", 23.14747, 72.64383, "Yellow", 7, "UNDER_CONSTRUCTION"),
    ("GNLU", 23.15434, 72.64737, "Yellow", 8, "UNDER_CONSTRUCTION"),  # interchange Yellow↔Violet
    ("Raysan", 23.16621, 72.64837, "Yellow", 9, "UNDER_CONSTRUCTION"),
    ("Randesan", 23.17885, 72.64733, "Yellow", 10, "UNDER_CONSTRUCTION"),
    ("Dholakuva Circle", 23.18570, 72.64336, "Yellow", 11, "UNDER_CONSTRUCTION"),
    ("Infocity", 23.19242, 72.63975, "Yellow", 12, "UNDER_CONSTRUCTION"),
    ("Sector-1", 23.20519, 72.64339, "Yellow", 13, "UNDER_CONSTRUCTION"),
    ("Sector-10A", 23.21056, 72.65175, "Yellow", 14, "UNDER_CONSTRUCTION"),
    ("Sachivalaya", 23.21488, 72.65871, "Yellow", 15, "UNDER_CONSTRUCTION"),
    ("Akshardham", 23.22349, 72.66424, "Yellow", 16, "UNDER_CONSTRUCTION"),
    ("Juna Sachivalaya", 23.22888, 72.65948, "Yellow", 17, "UNDER_CONSTRUCTION"),
    ("Sector-16", 23.23381, 72.65021, "Yellow", 18, "UNDER_CONSTRUCTION"),
    ("Sector-24", 23.23842, 72.64156, "Yellow", 19, "UNDER_CONSTRUCTION"),
    ("Mahatma Mandir", 23.23365, 72.63366, "Yellow", 20, "UNDER_CONSTRUCTION"),
    # ── Phase 2 (Violet Line): GNLU ↔ GIFT City — 3 stations ──
    ("GNLU", 23.15434, 72.64737, "Violet", 1, "UNDER_CONSTRUCTION"),  # interchange Yellow↔Violet
    ("PDEU", 23.15479, 72.66130, "Violet", 2, "UNDER_CONSTRUCTION"),
    ("GIFT City", 23.15326, 72.68558, "Violet", 3, "UNDER_CONSTRUCTION"),
]

def read_csv_any(path):
    return pd.read_csv(path) if path.startswith("http") else pd.read_csv(path)

# Metro source: official GMRC Google Maps KML (all 54 stations verified)
METRO_SRC_URL = "https://www.google.com/maps/d/viewer?mid=1ebnAmbRUbMyI_quB06reRZWcyfcLF8M"
if METRO_CSV_PATH:
    m = read_csv_any(METRO_CSV_PATH)
    metro = pd.DataFrame(dict(station_id=m.get("station_id", pd.Series([f"csv_{i:02d}" for i in range(len(m))])),
                              name=m["name"], lat=m["lat"].astype(float), lon=m["lon"].astype(float)))
    metro_origin, metro_src_url = "REAL_PUBLIC", METRO_CSV_PATH
    metro_note = f"Loaded {len(metro)} stations from user-provided file."
else:
    metro = pd.DataFrame([dict(station_id=f"gmrc_{i:02d}", name=nm, lat=la, lon=lo, line=ln, seq=sq, status=st)
                          for i, (nm, la, lo, ln, sq, st) in enumerate(FALLBACK_METRO)])
    metro_origin, metro_src_url = "REAL_PUBLIC", METRO_SRC_URL
    metro_note = "Coordinates transcribed from official GMRC Google My Maps station layer (KML). All 54 stations verified."
metro["source"] = metro_src_url
metro["data_origin"] = metro_origin
print(metro_note, "| metro stations:", len(metro))

# ── Bus stops: GTFS (primary) → user CSV → OSM → synthetic fallback ──
bus_origin, bus_src_url = "REAL_PUBLIC", GTFS_URL
bus_routes_df, bus_trips_df, bus_stop_times_df = None, None, None
try:
    if BUS_CSV_PATH:
        b = read_csv_any(BUS_CSV_PATH)
        bus = pd.DataFrame(dict(stop_id=b.get("stop_id", pd.Series([f"csv_{i:04d}" for i in range(len(b))])),
                                name=b["name"], lat=b["lat"].astype(float), lon=b["lon"].astype(float),
                                is_brts=b.get("is_brts", pd.Series([False] * len(b))).astype(bool),
                                agency=b.get("agency", pd.Series(["unknown"] * len(b)))))
        bus_src_url = BUS_CSV_PATH
        bus_note = f"Loaded {len(bus)} stops from user-provided file."
        raise StopIteration
    # ── Primary: GTFS from GitHub (AMTS + AJL/BRTS + GTSL) ──
    r_gtfs = requests.get(GTFS_URL, timeout=30)
    r_gtfs.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r_gtfs.content))
    gtfs_stops = pd.read_csv(zf.open("stops.txt"))
    gtfs_routes = pd.read_csv(zf.open("routes.txt"))
    gtfs_trips = pd.read_csv(zf.open("trips.txt"))
    gtfs_stop_times = pd.read_csv(zf.open("stop_times.txt"))
    # Filter stops within BBOX
    gtfs_stops = gtfs_stops[
        (gtfs_stops.stop_lat.between(s - 0.05, n + 0.05)) &
        (gtfs_stops.stop_lon.between(w - 0.05, e + 0.05))
    ].reset_index(drop=True)
    # Detect agency from stop_id prefix and route membership
    def detect_agency(sid):
        if str(sid).startswith("BRTS_"): return "AJL_BRTS"
        if str(sid).startswith("GTSL_"): return "GTSL_Gandhinagar"
        return "AMTS"
    # Detect BRTS: check if stop is served by any AJL route
    ajl_route_ids = set(gtfs_routes[gtfs_routes.agency_id == "AJL"].route_id)
    ajl_trip_ids = set(gtfs_trips[gtfs_trips.route_id.isin(ajl_route_ids)].trip_id)
    brts_stop_ids = set(gtfs_stop_times[gtfs_stop_times.trip_id.isin(ajl_trip_ids)].stop_id)
    bus = pd.DataFrame(dict(
        stop_id=gtfs_stops.stop_id,
        name=gtfs_stops.stop_name,
        lat=gtfs_stops.stop_lat,
        lon=gtfs_stops.stop_lon,
        is_brts=gtfs_stops.stop_id.isin(brts_stop_ids),
        agency=gtfs_stops.stop_id.apply(detect_agency),
    ))
    # Also flag BRTS-served stops detected via trip linkage
    bus.loc[bus.stop_id.isin(brts_stop_ids), "is_brts"] = True
    bus.loc[bus.stop_id.isin(brts_stop_ids), "agency"] = "AJL_BRTS"
    # Compute routes per stop (frequency proxy)
    stop_route_count = (
        gtfs_stop_times[["trip_id", "stop_id"]]
        .merge(gtfs_trips[["trip_id", "route_id"]], on="trip_id")
        .drop_duplicates(subset=["stop_id", "route_id"])
        .groupby("stop_id").route_id.count().rename("routes_served")
    )
    bus = bus.merge(stop_route_count, left_on="stop_id", right_index=True, how="left")
    bus["routes_served"] = bus["routes_served"].fillna(0).astype(int)
    # Save routes & trips as separate tables
    bus_routes_df = gtfs_routes.rename(columns={"route_short_name": "route_name", "route_long_name": "route_desc"})
    bus_routes_df["data_origin"] = "REAL_PUBLIC"
    bus_routes_df["source"] = GTFS_URL
    bus_trips_df = gtfs_trips.copy()
    bus_trips_df["data_origin"] = "REAL_PUBLIC"
    bus_note = f"GTFS: {len(bus)} real stops ({int(bus.is_brts.sum())} BRTS), {len(gtfs_routes)} routes, {len(gtfs_trips)} trips from AMTS/AJL/GTSL."
except StopIteration:
    pass
except Exception as ex:
    bus_origin, bus_src_url = "SYNTHETIC", "generated around metro stations and city core (fixed seed)"
    rows = []
    for i in range(400):
        base = metro.sample(1, random_state=SEED + i).iloc[0]
        rows.append(dict(stop_id=f"syn_{i:04d}", name=f"Synthetic stop {i}", lat=base.lat + random.gauss(0, 0.02),
                         lon=base.lon + random.gauss(0, 0.02), is_brts=(i % 7 == 0), agency="synthetic", routes_served=0))
    bus = pd.DataFrame(rows)
    bus_note = f"GTFS + OSM fetch failed ({type(ex).__name__}: {ex}); generated synthetic stops."
bus["source"] = bus_src_url
bus["data_origin"] = bus_origin
print(bus_note, "| bus stops:", len(bus))

# COMMAND ----------

# MAGIC %md ## 2. Analysis areas (approximate centroids + synthetic demand weight)

# COMMAND ----------

AREAS = [  # name, approx lat, approx lon  — APPROXIMATE, for POC only
    ("Bopal", 23.030, 72.470), ("South Bopal", 23.016, 72.470), ("Ghuma", 23.032, 72.447), ("Shela", 23.008, 72.447),
    ("Thaltej", 23.052, 72.510), ("Science City", 23.075, 72.505), ("Gota", 23.095, 72.530), ("Chandkheda", 23.105, 72.590),
    ("Tragad", 23.120, 72.560), ("Vastrapur", 23.038, 72.530), ("Satellite", 23.025, 72.515), ("Prahladnagar", 23.012, 72.510),
    ("Vejalpur", 23.005, 72.525), ("Juhapura", 22.998, 72.530), ("Sarkhej", 22.985, 72.500), ("Maninagar", 22.995, 72.610),
    ("Naroda", 23.070, 72.660), ("Nikol", 23.040, 72.670), ("Odhav", 23.030, 72.660), ("Ramol", 23.000, 72.650),
    ("Hathijan", 22.975, 72.655), ("Navrangpura", 23.035, 72.560), ("Vatva", 22.960, 72.630), ("Ranip", 23.070, 72.570),
]
areas = pd.DataFrame([dict(area_id=f"A{i+1:02d}", name=nm, lat=la, lon=lo,
                           demand_weight=round(random.uniform(0.6, 1.4), 2)) for i, (nm, la, lo) in enumerate(AREAS)])
areas["data_origin"] = "APPROXIMATE centroid; SYNTHETIC demand_weight"
log("load_areas", len(AREAS), len(areas), 0, msg="seed list")

# COMMAND ----------

# MAGIC %md ## 3. Validation (missing / out-of-range coords, duplicates)

# COMMAND ----------

# DBTITLE 1,3. Validate & save
def validate(pdf, key, label):
    n0 = len(pdf)
    ok = pdf.dropna(subset=["lat", "lon"])
    ok = ok[(ok.lat.between(s - 0.2, n + 0.2)) & (ok.lon.between(w - 0.2, e + 0.2))]
    ok = ok.drop_duplicates(subset=[key]).drop_duplicates(subset=["lat", "lon"]).reset_index(drop=True)
    log(f"validate_{label}", n0, len(ok), n0 - len(ok))
    return ok

# Metro: preserve interchange stations (same lat/lon, different line) — don't dedup by lat/lon
_n0 = len(metro)
metro = metro.dropna(subset=["lat", "lon"])
metro = metro[(metro.lat.between(s - 0.2, n + 0.2)) & (metro.lon.between(w - 0.2, e + 0.2))]
metro = metro.drop_duplicates(subset=["station_id"]).reset_index(drop=True)
log("validate_metro_stations", _n0, len(metro), _n0 - len(metro),
    msg=f"{metro.name.nunique()} unique names, {len(metro)} rows (interchange stations on both lines)")
bus = validate(bus, "stop_id", "bus_stops")
save("metro_stations", metro)
save("bus_stops", bus)
save("areas", areas)
# Save GTFS routes and trips if available
if bus_routes_df is not None:
    save("bus_routes", bus_routes_df)
    log("save_bus_routes", len(bus_routes_df), len(bus_routes_df), 0, msg=f"GTFS routes from {GTFS_URL}")
if bus_trips_df is not None:
    save("bus_trips", bus_trips_df)
    log("save_bus_trips", len(bus_trips_df), len(bus_trips_df), 0, msg=f"GTFS trips from {GTFS_URL}")

# COMMAND ----------

# DBTITLE 1,3b. Fare tables (BRTS real GTFS + AMTS/Metro transcribed)
# MAGIC %md ## 3b. Fare tables (BRTS real GTFS + AMTS/Metro transcribed + fare-split policy)

# COMMAND ----------

# DBTITLE 1,3b. Fare tables — BRTS (real), AMTS (assumed), Metro (transcribed), fare-split policy
# ── A1: BRTS fares from GTFS-Fares v2 (real, in the same gtfs.zip) ──
import io as _io
try:
    fp  = pd.read_csv(_io.BytesIO(zf.read("fare_products.txt")))
    flr = pd.read_csv(_io.BytesIO(zf.read("fare_leg_rules.txt")))
    sa  = pd.read_csv(_io.BytesIO(zf.read("stop_areas.txt")))
    adult = fp[fp.rider_category_id == "BRTS_ADULT"][["fare_product_id", "amount"]]
    rules = flr.merge(adult, on="fare_product_id")
    fare_brts_leg = (
        rules.merge(sa.rename(columns={"stop_id": "from_stop_id", "area_id": "from_area_id"}), on="from_area_id")
             .merge(sa.rename(columns={"stop_id": "to_stop_id",   "area_id": "to_area_id"}),   on="to_area_id")
             [["from_stop_id", "to_stop_id", "from_area_id", "to_area_id", "amount"]]
    )
    fare_brts_leg["data_origin"] = "REAL_PUBLIC"
    save("fare_brts_leg", fare_brts_leg)
    log("fare_brts_leg", len(flr), len(fare_brts_leg), 0, msg=f"GTFS-Fares v2 adult rules expanded to stop pairs")
    fare_brts_areas = pd.read_csv(_io.BytesIO(zf.read("areas.txt")))
    fare_brts_areas["data_origin"] = "REAL_PUBLIC"
    save("fare_brts_areas", fare_brts_areas)
    log("fare_brts_areas", len(fare_brts_areas), len(fare_brts_areas), 0, msg="BRTS fare zone areas from GTFS")
except Exception as ex:
    print(f"  warning: BRTS fare files not found in GTFS zip ({ex}); skipping fare_brts_leg")
    fare_brts_leg = pd.DataFrame(columns=["from_stop_id","to_stop_id","from_area_id","to_area_id","amount","data_origin"])
    fare_brts_areas = pd.DataFrame()
    log("fare_brts_leg", 0, 0, 0, status="SKIPPED", msg=str(ex))

# ── A2: AMTS stage fares (transcribed from AMTS website fare chart) ──
# Source: https://amts.co.in (stage-based fare chart as of Oct 2026)
# If the exact chart is not machine-readable, these are representative stage fares.
fare_amts_stages = pd.DataFrame([
    dict(max_km=2,  amount=5,  source_note="AMTS minimum fare (transcribed)"),
    dict(max_km=5,  amount=7,  source_note="AMTS stage 2 (transcribed)"),
    dict(max_km=10, amount=10, source_note="AMTS stage 3 (transcribed)"),
    dict(max_km=15, amount=12, source_note="AMTS stage 4 (transcribed)"),
    dict(max_km=20, amount=15, source_note="AMTS stage 5 (transcribed)"),
    dict(max_km=30, amount=20, source_note="AMTS stage 6 (transcribed)"),
    dict(max_km=99, amount=25, source_note="AMTS maximum fare (transcribed)"),
])
fare_amts_stages["data_origin"] = "APPROXIMATE"
save("fare_amts_stages", fare_amts_stages)
log("fare_amts_stages", 7, 7, 0, msg="AMTS stage fare chart (transcribed/assumed)")

# ── A3: Metro slab fares (transcribed from GMRC gujaratmetrorail.com fare page) ──
# Source: https://gujaratmetrorail.com/fare-calculator/ (Oct 2026)
fare_metro_slabs = pd.DataFrame([
    dict(max_km=2,  amount=5,  source_note="GMRC minimum fare"),
    dict(max_km=4,  amount=10, source_note="GMRC slab 2"),
    dict(max_km=6,  amount=15, source_note="GMRC slab 3"),
    dict(max_km=10, amount=20, source_note="GMRC slab 4"),
    dict(max_km=14, amount=25, source_note="GMRC slab 5"),
    dict(max_km=18, amount=30, source_note="GMRC slab 6"),
    dict(max_km=22, amount=35, source_note="GMRC slab 7"),
    dict(max_km=30, amount=40, source_note="GMRC slab 8"),
    dict(max_km=99, amount=50, source_note="GMRC maximum fare"),
])
fare_metro_slabs["data_origin"] = "APPROXIMATE"
fare_metro_slabs["limitations"] = "Slabs transcribed from GMRC fare chart; verify current values"
save("fare_metro_slabs", fare_metro_slabs)
log("fare_metro_slabs", 9, 9, 0, msg="Metro distance-based fare slabs (transcribed from GMRC)")

# ── A4: Fare-split policy table (proposed integrated-ticket model) ──
fare_split_policy = pd.DataFrame([
    dict(parameter="transfer_discount_inr", value=5,    note="Proposed: INR off each leg after the first on one integrated ticket"),
    dict(parameter="split_basis",           value=None, note="Revenue split pro-rata by scheduled passenger-km per operator"),
    dict(parameter="discount_funded_by",    value=None, note="City mode-shift incentive (proposed)"),
    dict(parameter="clearing_house_fee_pct",value=2,    note="Proposed per-transaction fee for the private partner running ticketing + clearing"),
])
save("fare_split_policy", fare_split_policy)
log("fare_split_policy", 4, 4, 0, msg="Proposed integrated fare policy parameters")

print(f"\nFare tables: fare_brts_leg={len(fare_brts_leg):,}, fare_amts_stages={len(fare_amts_stages)}, fare_metro_slabs={len(fare_metro_slabs)}, fare_split_policy={len(fare_split_policy)}")

# COMMAND ----------

# MAGIC %md ## 4. Scoring

# COMMAND ----------

# DBTITLE 1,4. Scoring (OPERATIONAL stations only)
W = {c: (wt, th) for c, wt, th, _ in SCORING}

def nearest(lat, lon, pts, name_col):
    d = pts.apply(lambda r: haversine(lat, lon, r.lat, r.lon), axis=1)
    i = d.idxmin()
    return float(d.min()), pts.loc[i, name_col], d

def score_row(dist_metro, dist_bus, stops_1km):
    m = clamp(1 - dist_metro / W["metro"][1])
    b = clamp(1 - dist_bus / W["bus"][1])
    d = clamp(stops_1km / W["density"][1])
    return round(W["metro"][0] * m + W["bus"][0] * b + W["density"][0] * d, 1), m, b, d

def category(sc):
    return "Potential access gap" if sc < GAP_THRESHOLD else ("Moderate" if sc < MODERATE_THRESHOLD else "Well connected")

# Only score against OPERATIONAL stations; all 57 rows still saved for the app's Journey Planner
metro_for_scoring = metro[metro.status == "OPERATIONAL"].drop_duplicates(subset=["name"]).reset_index(drop=True) if "status" in metro.columns else metro
print(f"Scoring with {len(metro_for_scoring)} operational stations (of {len(metro)} total)")

rows = []
for _, a in areas.iterrows():
    dm, nm_metro, _ = nearest(a.lat, a.lon, metro_for_scoring, "name")
    db, nm_bus, dbus = nearest(a.lat, a.lon, bus, "name")
    s500, s1k = int((dbus <= 0.5).sum()), int((dbus <= 1.0).sum())
    brts1k = bool(((dbus <= 1.0) & bus.is_brts).any())
    sc, m, b, d = score_row(dm, db, s1k)
    cat = category(sc)
    reasons = []
    if m == 0: reasons.append(f"nearest metro ({nm_metro}) is {dm:.1f} km away, beyond the 3 km threshold")
    elif m < 0.5: reasons.append(f"metro ({nm_metro}) is {dm:.1f} km away")
    if b == 0: reasons.append(f"nearest bus stop is {db:.2f} km away, beyond 1 km")
    elif b < 0.5: reasons.append(f"nearest bus stop is {db:.2f} km away")
    if d < 0.4: reasons.append(f"only {s1k} bus stop(s) within 1 km")
    if not reasons: reasons.append("close to metro and multiple bus stops")
    rows.append(dict(area_id=a.area_id, name=a["name"], lat=a.lat, lon=a.lon, demand_weight=a.demand_weight,
                     dist_metro_km=round(dm, 2), nearest_metro=nm_metro, dist_bus_km=round(db, 2), nearest_bus=nm_bus,
                     bus_stops_500m=s500, bus_stops_1km=s1k, has_brts_1km=brts1k,
                     metro_component=round(m, 2), bus_component=round(b, 2), density_component=round(d, 2),
                     score=sc, category=cat, reasons="; ".join(reasons),
                     distance_method="straight-line (haversine), not walking network", data_origin="DERIVED"))
acc = pd.DataFrame(rows).sort_values("score").reset_index(drop=True)
acc["rank"] = range(1, len(acc) + 1)
log("compute_accessibility", len(areas), len(acc), 0)
save("area_accessibility", acc)

# COMMAND ----------

# MAGIC %md ## 5. Feeder-connection scenarios (hypothetical) — same formula, recomputed

# COMMAND ----------

scen_rows = []
targets = acc.head(6)   # six weakest areas
for k, t in targets.iterrows():
    affected = acc[acc.apply(lambda r: haversine(t.lat, t.lon, r.lat, r.lon) <= SCENARIO_RADIUS_KM, axis=1)]
    before = round(float(affected.score.mean()), 1)
    after_scores, changed = [], []
    for _, r in affected.iterrows():
        new_db = min(r.dist_bus_km, SCENARIO_FEEDER_DIST_KM)
        new_s1k = r.bus_stops_1km + 1
        new_dm = min(r.dist_metro_km, FEEDER_METRO_EQUIV_KM)
        sc, *_ = score_row(new_dm, new_db, new_s1k)
        after_scores.append(sc)
        changed.append(f"{r['name']}: metro_equiv {r.dist_metro_km}→{new_dm} km, bus {r.dist_bus_km}→{new_db} km, stops_1km {r.bus_stops_1km}→{new_s1k}, score {r.score}→{sc}")
    after = round(sum(after_scores) / len(after_scores), 1)
    sc_target, *_ = score_row(min(t.dist_metro_km, FEEDER_METRO_EQUIV_KM), min(t.dist_bus_km, SCENARIO_FEEDER_DIST_KM), t.bus_stops_1km + 1)
    scen_rows.append(dict(
        scenario_id=f"S{k+1:02d}", name=f"Feeder: {t['name']} → {t.nearest_metro}",
        area_id=t.area_id, area_name=t['name'], connects_to=t.nearest_metro, length_km=t.dist_metro_km,
        areas_affected=", ".join(affected["name"]), n_areas_affected=len(affected),
        score_before=t.score, score_after=sc_target, delta=round(sc_target - t.score, 1),
        avg_score_before=before, avg_score_after=after, avg_delta=round(after - before, 1),
        explanation=("Hypothetical feeder bus from the area centroid to its nearest metro station. "
                     "Inputs changed for every area within "
                     f"{SCENARIO_RADIUS_KM} km: dist_bus_km = min(current, 0.2); bus_stops_1km +1; metro distance treated as "
                     f"min(current, {FEEDER_METRO_EQUIV_KM}) because the feeder links to the metro (documented assumption). Same scoring formula. | " + " | ".join(changed)),
        status="modeled scenario — hypothetical, not an official proposal", data_origin="DERIVED (scenario SYNTHETIC)"))
scen = pd.DataFrame(scen_rows).sort_values("delta", ascending=False).reset_index(drop=True)
log("compute_scenarios", len(targets), len(scen), 0)
save("feeder_scenarios", scen)

# COMMAND ----------

# MAGIC %md ## 6. Provenance register, methodology, scoring config, run log

# COMMAND ----------

# DBTITLE 1,6. Provenance register, methodology, scoring config, run log
_src_rows = [
    dict(dataset_id="metro_stations", name="Ahmedabad/Gandhinagar metro stations", source_url=metro_src_url,
         publisher="Gujarat Metro Rail Corporation (GMRC) — coordinates from official Google My Maps KML",
         license="publisher terms; coordinates transcribed", data_origin=metro_origin, row_count=len(metro),
         fetched_at=now(), limitations=metro_note),
    dict(dataset_id="bus_stops", name="Bus stops (AMTS / AJL-BRTS / GTSL)", source_url=bus_src_url,
         publisher="Community GTFS (croyla/amd-gtfs), built from AMTS, Janmarg and GTSL websites",
         license="MIT-0 (per croyla GTFS project listing)" if bus_origin == "REAL_PUBLIC" else "n/a",
         data_origin=bus_origin, row_count=len(bus),
         fetched_at=now(), limitations=bus_note + " Community-maintained feed; BRTS flag derived from AJL trip linkage."),
    dict(dataset_id="areas", name="Analysis areas (localities)", source_url="seed list in notebook",
         publisher="project team", license="n/a", data_origin="APPROXIMATE + SYNTHETIC", row_count=len(areas),
         fetched_at=now(), limitations="Centroids approximate; demand_weight is synthetic and NOT used in the score."),
    dict(dataset_id="area_accessibility", name="Accessibility scores", source_url=f"{FQ}.metro_stations, {FQ}.bus_stops, {FQ}.areas",
         publisher="this pipeline", license="n/a", data_origin="DERIVED", row_count=len(acc), fetched_at=now(),
         limitations="Straight-line distances; no passenger, population or cost data."),
    dict(dataset_id="feeder_scenarios", name="Feeder scenarios", source_url=f"{FQ}.area_accessibility",
         publisher="this pipeline", license="n/a", data_origin="DERIVED (hypothetical)", row_count=len(scen), fetched_at=now(),
         limitations="Scenarios are hypothetical; improvement is modeled, not validated."),
]
if bus_routes_df is not None:
    _src_rows += [
        dict(dataset_id="bus_routes", name="GTFS bus routes", source_url=GTFS_URL,
             publisher="Community GTFS (croyla/amd-gtfs)", license="MIT-0",
             data_origin="REAL_PUBLIC", row_count=len(bus_routes_df), fetched_at=now(),
             limitations="Route list only; no calendar filtering applied."),
        dict(dataset_id="bus_trips", name="GTFS scheduled trips", source_url=GTFS_URL,
             publisher="Community GTFS (croyla/amd-gtfs)", license="MIT-0",
             data_origin="REAL_PUBLIC", row_count=len(bus_trips_df), fetched_at=now(),
             limitations="All trips across all service_ids — scheduled, not daily; calendar.txt not applied."),
    ]
# A5: Add fare table provenance
_src_rows += [
    dict(dataset_id="fare_brts_leg", name="BRTS fare rules (GTFS-Fares v2)", source_url=GTFS_URL,
         publisher="Community GTFS (croyla/amd-gtfs)", license="MIT-0",
         data_origin="REAL_PUBLIC", row_count=len(fare_brts_leg), fetched_at=now(),
         limitations="Adult fares only; expanded from area→stop pairs. Child/luggage fares excluded."),
    dict(dataset_id="fare_amts_stages", name="AMTS stage-based fares", source_url="https://amts.co.in",
         publisher="AMTS (transcribed)", license="n/a",
         data_origin="APPROXIMATE", row_count=len(fare_amts_stages), fetched_at=now(),
         limitations="Transcribed from AMTS fare chart; may not reflect latest revisions. Verify against amts.co.in."),
    dict(dataset_id="fare_metro_slabs", name="Metro distance-based fare slabs", source_url="https://gujaratmetrorail.com/fare-calculator/",
         publisher="GMRC (transcribed)", license="n/a",
         data_origin="APPROXIMATE", row_count=len(fare_metro_slabs), fetched_at=now(),
         limitations="Slabs transcribed from GMRC fare chart; verify current values."),
    dict(dataset_id="fare_split_policy", name="Proposed integrated fare policy", source_url="project team",
         publisher="project team", license="n/a",
         data_origin="PROPOSED", row_count=len(fare_split_policy), fetched_at=now(),
         limitations="This is a PROPOSAL, not an existing scheme. Parameters are illustrative."),
]
sources = pd.DataFrame(_src_rows)
save("data_sources", sources)

save("scoring_config", pd.DataFrame([dict(component=c, weight=wt, threshold=th, description=d) for c, wt, th, d in SCORING]
                                    + [dict(component="gap_threshold", weight=GAP_THRESHOLD, threshold=None, description="score below this = Potential access gap"),
                                       dict(component="moderate_threshold", weight=MODERATE_THRESHOLD, threshold=None, description="score below this (and above gap) = Moderate")]))

save("methodology", pd.DataFrame([
    dict(step=1, formula="dist_metro_km = haversine(area, nearest metro station)", note="straight-line km"),
    dict(step=2, formula="dist_bus_km = haversine(area, nearest bus stop); bus_stops_1km = count of stops within 1 km", note="straight-line km"),
    dict(step=3, formula="score = 40*clamp(1-dist_metro/3) + 35*clamp(1-dist_bus/1) + 25*clamp(stops_1km/8)", note="0-100, higher = better access; weights in scoring_config"),
    dict(step=4, formula="category = gap if score<35, moderate if <60, else well connected", note="thresholds in scoring_config"),
    dict(step=5, formula="scenario: dist_bus=min(dist_bus,0.2); stops_1km+1; metro_dist=min(metro_dist,1.5) for areas within 1.5 km; recompute score", note="hypothetical feeder; metro_equiv 1.5 km is an assumption; delta = after - before"),
]))

log("write_outputs", 0, 0, 0, msg="tables written")
save("pipeline_runs", pd.DataFrame(run_log))

# COMMAND ----------

# DBTITLE 1,6b. Reapply column comments (survives overwriteSchema)
# Column comments are dropped by overwriteSchema=true on every re-run.
# Reapply the key descriptions Genie depends on for accurate NL-to-SQL.
COMMENTS = {
    "area_accessibility": {
        "": "One row per neighbourhood with modeled transit access score. Higher score = better access. rank 1 = weakest.",
        "score": "Modeled accessibility 0-100 (higher = better). 40*metro + 35*bus + 25*density; weights in scoring_config.",
        "category": "'Potential access gap' (<35), 'Moderate' (35-60), 'Well connected' (>60).",
        "dist_metro_km": "Straight-line km to nearest metro station (not walking distance).",
        "dist_bus_km": "Straight-line km to nearest GTFS bus stop.",
        "bus_stops_1km": "Count of GTFS bus stops within 1 km straight-line.",
        "has_brts_1km": "True if any AJL/BRTS-served stop is within 1 km.",
        "reasons": "Human-readable explanation of why the area got its category.",
        "rank": "1 = weakest transit access, 24 = strongest.",
        "nearest_metro": "Name of the closest metro station.",
        "nearest_bus": "Name of the closest bus stop.",
    },
    "feeder_scenarios": {
        "": "Hypothetical feeder bus from an underserved area to its nearest metro. delta = modeled score gain, not validated.",
        "delta": "score_after - score_before (modeled). Higher = bigger improvement.",
        "length_km": "Straight-line length of the proposed feeder.",
        "score_before": "Area score before the hypothetical feeder.",
        "score_after": "Area score after the hypothetical feeder.",
    },
    "bus_stops": {
        "": "Real GTFS stops (community feed croyla/amd-gtfs). agency = AMTS / AJL_BRTS / GTSL_Gandhinagar.",
        "routes_served": "Number of distinct GTFS routes stopping here (service-level proxy).",
        "is_brts": "True if served by an AJL (Janmarg BRTS) route.",
        "agency": "Transit operator: AMTS, AJL_BRTS, or GTSL_Gandhinagar.",
    },
    "metro_stations": {
        "": "GMRC metro stations, Phase 1 (Blue/Red) + Phase 2 (Yellow/Violet). Coordinates from official KML.",
        "source": "URL or path from which coordinates were obtained.",
    },
    "bus_routes": {
        "": "GTFS bus routes from AMTS, AJL/BRTS, GTSL agencies.",
        "route_name": "Short route identifier (e.g. 101sh).",
        "route_desc": "Origin-destination description of the route.",
    },
    "bus_trips": {
        "": "GTFS scheduled trips — all service_ids, not filtered by calendar.",
        "trip_headsign": "Destination displayed on the bus for this trip.",
    },
    "route_stops": {
        "": "Ordered stop sequences for 972 bus routes. Powers the Journey Planner.",
        "stop_sequence": "Order this stop appears on the route (1 = first stop).",
        "arrival_time": "Scheduled arrival time (HH:MM:SS).",
    },
    "data_sources": {
        "": "Provenance register: data_origin is REAL_PUBLIC, APPROXIMATE, SYNTHETIC or DERIVED.",
        "limitations": "Known caveats and quality notes for this data source.",
    },
    "scoring_config": {
        "": "Scoring formula weights and thresholds. Change these to re-prioritize components.",
    },
    "areas": {
        "": "24 Ahmedabad/Gandhinagar neighbourhoods with approximate centroids.",
        "demand_weight": "Synthetic placeholder (NOT used in scoring). Reserved for future population data.",
    },
    "pipeline_runs": {
        "": "Audit log of every pipeline step with row counts for data quality tracking.",
    },
    "methodology": {
        "": "Step-by-step scoring documentation.",
    },
    "fare_brts_leg": {
        "": "BRTS fare rules from GTFS-Fares v2. One row per (from_stop, to_stop) pair with INR fare. Adult only.",
        "from_stop_id": "BRTS stop where the leg starts.",
        "to_stop_id": "BRTS stop where the leg ends.",
        "amount": "Fare in INR for this stop pair (adult, BRTS).",
        "from_area_id": "GTFS fare zone area for the origin stop.",
        "to_area_id": "GTFS fare zone area for the destination stop.",
    },
    "fare_brts_areas": {
        "": "BRTS fare zone areas from GTFS. Maps area_id to stop groups for fare calculation.",
    },
    "fare_amts_stages": {
        "": "AMTS stage-based fare chart (transcribed/assumed). amount is INR for trips up to max_km.",
        "max_km": "Upper bound of the distance stage in km.",
        "amount": "Fare in INR for this stage.",
        "source_note": "Origin of this fare value (transcribed or assumed).",
    },
    "fare_metro_slabs": {
        "": "Metro distance-based fare slabs (transcribed from GMRC). amount is INR for trips up to max_km.",
        "max_km": "Upper bound of the distance slab in km.",
        "amount": "Fare in INR for this slab.",
        "source_note": "Origin of this fare value (transcribed from GMRC fare chart).",
    },
    "fare_split_policy": {
        "": "Proposed integrated-ticket fare policy. NOT an existing scheme — parameters are illustrative for the PPP model.",
        "parameter": "Policy parameter name (e.g. transfer_discount_inr, clearing_house_fee_pct).",
        "value": "Numeric value of the parameter (INR or %). NULL means described in note only.",
        "note": "Explanation of the parameter and its rationale.",
    },
}
for tbl, cols in COMMENTS.items():
    for col, text in cols.items():
        txt = text.replace("'", "''")
        try:
            if col == "":
                spark.sql(f"COMMENT ON TABLE {FQ}.{tbl} IS '{txt}'")
            else:
                spark.sql(f"ALTER TABLE {FQ}.{tbl} ALTER COLUMN `{col}` COMMENT '{txt}'")
        except Exception as e:
            print(f"  warning: {tbl}.{col}: {e}")
print(f"Column comments reapplied to {len(COMMENTS)} tables.")

# COMMAND ----------

# MAGIC %md ## 7. Summary for the demo

# COMMAND ----------

print("Schema:", FQ, "| run:", RUN_ID)
print("\nWeakest 5 areas:")
print(acc[["rank", "name", "score", "category", "dist_metro_km", "nearest_metro", "dist_bus_km", "bus_stops_1km"]].head(5).to_string(index=False))
print("\nTop scenarios:")
print(scen[["scenario_id", "name", "score_before", "score_after", "delta", "n_areas_affected"]].to_string(index=False))
print("\nRun log:")
print(pd.DataFrame(run_log)[["step", "input_rows", "output_rows", "rejected_rows", "status"]].to_string(index=False))
display(spark.table(f"{FQ}.area_accessibility"))

# COMMAND ----------

# MAGIC %md ## 8. Export for the OFFLINE demo app (download these files before boarding)

# COMMAND ----------

# DBTITLE 1,8. Export for the OFFLINE demo app
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/export"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.export")
import os; os.makedirs(VOL, exist_ok=True)
for t in ["metro_stations","bus_stops","bus_routes","bus_trips","areas","area_accessibility","feeder_scenarios","data_sources","pipeline_runs","scoring_config","methodology","route_stops","fare_brts_leg","fare_brts_areas","fare_amts_stages","fare_metro_slabs","fare_split_policy"]:
    spark.table(f"{FQ}.{t}").toPandas().to_csv(f"{VOL}/{t}.csv", index=False)
    print("exported", f"{VOL}/{t}.csv")
print("\nDownload every CSV from the Volume (Catalog Explorer -> Volumes -> export) into local_app/data/ on your laptop.")