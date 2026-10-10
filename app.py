"""TransitGap — Databricks App (Streamlit). Reads the tables created by transitgap_pipeline.py."""
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
from fpdf import FPDF
from io import BytesIO
from datetime import datetime

CATALOG = os.getenv("CATALOG", "main")
SCHEMA = os.getenv("SCHEMA", "transitgap")
HTTP_PATH = os.getenv("WAREHOUSE_HTTP_PATH", "")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
FQ = f"{CATALOG}.{SCHEMA}"

st.set_page_config(page_title="TransitGap", page_icon="🚇", layout="wide")
cfg = Config()


@st.cache_data(ttl=300)
def q(sql_text: str) -> pd.DataFrame:
    with sql.connect(server_hostname=cfg.host, http_path=HTTP_PATH,
                     credentials_provider=lambda: cfg.authenticate) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
            cols = [c[0] for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)


try:
    acc = q(f"SELECT * FROM {FQ}.area_accessibility ORDER BY score")
    metro = q(f"SELECT * FROM {FQ}.metro_stations ORDER BY line, seq")
    bus = q(f"SELECT * FROM {FQ}.bus_stops")
    scen = q(f"SELECT * FROM {FQ}.feeder_scenarios ORDER BY delta DESC")
    srcs = q(f"SELECT * FROM {FQ}.data_sources")
    runs = q(f"SELECT * FROM {FQ}.pipeline_runs ORDER BY started_at")
    scfg = q(f"SELECT * FROM {FQ}.scoring_config")
    try:
        bus_routes = q(f"SELECT * FROM {FQ}.bus_routes")
        bus_trips = q(f"SELECT * FROM {FQ}.bus_trips")
    except Exception:
        bus_routes, bus_trips = pd.DataFrame(), pd.DataFrame()
    # Route-stop sequences for Journey Planner
    try:
        rstops = q(f"SELECT * FROM {FQ}.route_stops ORDER BY route_id, stop_sequence")
    except Exception:
        
        rstops = pd.DataFrame()
    # Route coverage per area (spatial join: bus stops within 1km of each area centroid)
    try:
        route_cov = q(f"""
            SELECT a.name AS area,
                   COUNT(DISTINCT b.stop_id) AS stops_1km,
                   COALESCE(SUM(b.routes_served), 0) AS total_route_connections,
                   COUNT(DISTINCT CASE WHEN b.is_brts THEN b.stop_id END) AS brts_stops,
                   COALESCE(MAX(b.routes_served), 0) AS busiest_stop_routes,
                   ROUND(COALESCE(AVG(b.routes_served), 0), 1) AS avg_routes_per_stop,
                   COUNT(DISTINCT CASE WHEN b.agency = 'AMTS' THEN b.stop_id END) AS amts_stops,
                   COUNT(DISTINCT CASE WHEN b.agency = 'AJL_BRTS' THEN b.stop_id END) AS ajl_stops
            FROM {FQ}.areas a CROSS JOIN {FQ}.bus_stops b
            WHERE 6371*2*ASIN(SQRT(POWER(SIN(RADIANS(b.lat-a.lat)/2),2)
                  +COS(RADIANS(a.lat))*COS(RADIANS(b.lat))*POWER(SIN(RADIANS(b.lon-a.lon)/2),2))) <= 1.0
            GROUP BY a.name""")
    except Exception:
        route_cov = pd.DataFrame()
    # Metro timetable (for real scheduled travel times in Journey Planner)
    try:
        metro_st = q(f"""SELECT t.route_id, t.service_id, s.trip_id, s.stop_id,
                                s.stop_sequence, s.arrival_time, s.departure_time
                         FROM {FQ}.metro_stop_times s JOIN {FQ}.metro_trips t ON s.trip_id = t.trip_id""")
    except Exception:
        metro_st = pd.DataFrame()
    # Extra tables for Data Journey showcase
    try:
        metro_trips_df = q(f"SELECT * FROM {FQ}.metro_trips")
        metro_calendar_df = q(f"SELECT * FROM {FQ}.metro_calendar")
        metro_transfers_df = q(f"SELECT * FROM {FQ}.metro_transfers")
        methodology_df = q(f"SELECT * FROM {FQ}.methodology")
    except Exception:
        metro_trips_df = metro_calendar_df = metro_transfers_df = methodology_df = pd.DataFrame()
    # Fare tables for Journey Planner (PS-08: One City, One Journey)
    try:
        fare_brts = q(f"SELECT from_stop_id, to_stop_id, amount FROM {FQ}.fare_brts_leg")
        fare_amts = q(f"SELECT * FROM {FQ}.fare_amts_stages ORDER BY max_km")
        fare_metro = q(f"SELECT * FROM {FQ}.fare_metro_slabs ORDER BY max_km")
        fare_policy = q(f"SELECT * FROM {FQ}.fare_split_policy")
    except Exception:
        fare_brts = fare_amts = fare_metro = fare_policy = pd.DataFrame()
except Exception as ex:
    st.error(f"Could not read tables from {FQ}. Run transitgap_pipeline.py first and check WAREHOUSE_HTTP_PATH. Error: {ex}")
    if GENIE_SPACE_ID:
        st.info(f"You can still ask questions via [Genie Space](https://adb-2271628843535125.5.azuredatabricks.net/genie/rooms/{GENIE_SPACE_ID}).")
    st.stop()

COLORS = {"Potential access gap": "#d62728", "Moderate": "#ff9f1c", "Well connected": "#2ca02c"}

st.title("🚇 TransitGap · One City, One Journey")
st.caption("Metro + BRTS + AMTS in one plan, one fare estimate, real transfer walks \u00b7 PS-08 \u00b7 Ahmedabad")

page = st.sidebar.radio("Go to", ["Journey Planner", "Pilot & PPP", "Map", "Gap Ranking", "What-If Scenarios", "Ask Genie", "Data Journey", "How It Works"])

n_gaps = int((acc.category == "Potential access gap").sum())
n_moderate = int((acc.category == "Moderate").sum())
n_good = int((acc.category == "Well connected").sum())
n_brts = int(bus.get("is_brts", pd.Series(dtype=bool)).sum()) if "is_brts" in bus.columns else 0
n_routes = len(bus_routes) if len(bus_routes) else 0
n_trips = len(bus_trips) if len(bus_trips) else 0
n_metro_unique = metro.name.nunique()  # 54 unique stations (interchange stations duplicated across lines)

# ---------------------------------------------------------------- Pilot & PPP
if page == "Pilot & PPP":
    _ch_fee = float(fare_policy.loc[fare_policy.parameter == "clearing_house_fee_pct", "value"].iloc[0]) if len(fare_policy) and "parameter" in fare_policy.columns else 2
    _xfer_disc = float(fare_policy.loc[fare_policy.parameter == "transfer_discount_inr", "value"].iloc[0]) if len(fare_policy) and "parameter" in fare_policy.columns else 5
    # --- Problem Statement ---
    st.markdown("""
    <div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%); border-radius: 16px;
        padding: 2rem 2.5rem; color: white; margin-bottom: 1.5rem;">
        <h2 style="color: white; margin: 0 0 0.8rem 0;">🎯 The Problem: Transit Access Inequality</h2>
        <p style="color: rgba(255,255,255,0.92); font-size: 1.05rem; line-height: 1.6;">
        Ahmedabad's metro network is expanding rapidly — <b>{} stations across 4 lines</b> — but many residential
        areas remain <b>beyond walking distance</b> from the nearest station or bus stop.
        Without data-driven analysis, planners cannot prioritize where new connecting bus routes, last-mile connectivity,
        and future metro extensions will have the <b>greatest impact</b>.<br><br>
        <b>TransitGap</b> combines real GTFS bus schedules (<b>{:,}</b> stops, <b>{:,}</b> routes, <b>{:,}</b> scheduled trips)
        with official metro station coordinates to score every neighbourhood — revealing exactly where
        public transport gaps exist and what interventions would close them.</p>
    </div>
    """.format(n_metro_unique, len(bus), n_routes, n_trips), unsafe_allow_html=True)

    # --- Key Metrics ---
    c = st.columns(6)
    c[0].metric("🏘️ Areas Analyzed", len(acc))
    c[1].metric("🚇 Metro Stations", n_metro_unique, help=f"{n_metro_unique} unique stations from official GMRC KML (4 lines)")
    c[2].metric("🚌 Bus Stops (Real)", f"{len(bus):,}", help=f"GTFS: {len(bus)-n_brts} AMTS + {n_brts} BRTS")
    c[3].metric("🛤️ Bus Routes", f"{n_routes:,}", help="AMTS + AJL/BRTS + GTSL routes")
    c[4].metric("🔴 Access Gaps", n_gaps, help="Score < 35: areas with poor transit access")
    c[5].metric("🟢 Well Connected", n_good, help="Score > 60: good multi-modal access")

    # --- Weakest areas ---
    st.subheader("⚠️ Areas Most in Need of Better Transit")
    st.dataframe(acc[["rank", "name", "score", "category", "dist_metro_km", "nearest_metro", "dist_bus_km", "bus_stops_1km"]].head(5),
                 hide_index=True, use_container_width=True)

    # --- Score chart ---
    st.subheader("📊 Accessibility Score by Area")
    fig = px.bar(acc, x="name", y="score", color="category", color_discrete_map=COLORS, range_y=[0, 100])
    fig.update_layout(xaxis_title="", yaxis_title="Accessibility score (0–100)", legend_title="")
    st.plotly_chart(fig, use_container_width=True)

    # --- PPP model ---
    st.divider()
    st.subheader("Public-Private Partnership Model")
    pp1, pp2, pp3 = st.columns(3)
    with pp1:
        st.markdown(f"**Who Builds:** A private MaaS / ticketing partner builds the integrated-ticket mobile app "
                    f"and the clearing house. Partner earns **{_ch_fee:.0f}%** per transaction.")
    with pp2:
        st.markdown("**Who Runs:** GMRC (metro), AJL (BRTS) and AMTS (city bus) keep own fare boxes. "
                    "Clearing house settles daily, pro-rata by passenger-km.")
    with pp3:
        st.markdown(f"**Who Pays:** City funds the **\u20b9{_xfer_disc:.0f} transfer discount** as mode-shift incentive. "
                    "Operators gain ridership. Partner earns per-transaction fee.")
    st.info("**Pilot:** Navrangpura \u2192 Mahatma Mandir. 3-month trial. Success metric: integrated-ticket uptake.")

    # --- What each page does (WHAT / WHY / RESOLVES) ---
    st.subheader("📖 What Each Page Does & Why It Matters")
    page_cards = [
        ("🚗", "Journey Planner", "Pick source and destination — get step-by-step walk + bus + metro route with real GTFS schedules.",
         "The whole point of transit is getting from A to B. This proves the data works end-to-end as a real routing tool.",
         "How do I get from Bopal to Maninagar using bus and metro?"),
        ("🗺️", "Map", "Interactive map of metro stations, bus stops, and area scores with BRTS/agency filtering.",
         "Tables hide spatial patterns. A planner needs to SEE where transit deserts cluster geographically.",
         "Where exactly are the transit deserts, and how do they relate to metro corridors?"),
        ("📋", "Gap Ranking", "Score breakdown for every area: distance to metro & bus, BRTS coverage, route frequency, and component-level drill-down.",
         "A single score doesn't explain the root cause. Planners need to know WHICH component (metro, bus, density) is dragging a score down.",
         "Why does a specific area score poorly, and what single factor would improve it most?"),
        ("🔄", "What-If Scenarios", "What happens if we add a new direct bus route from an underserved area to the nearest metro station? See modeled before/after scores.",
         "Building new routes costs money. Decision-makers need data-backed evidence before committing budget.",
         "If we add one new bus route connecting an underserved area to metro, how much does the score improve?"),
        ("📜", "Data Journey", "Full provenance: every data source, publisher, license, pipeline run log, scoring formula, GTFS route coverage, and known limitations.",
         "Trust requires transparency. No black boxes — every number must be traceable to its source.",
         "Can I trust these scores? Where did the data come from, and what are the caveats?"),
        ("🤖", "Ask Genie", "Natural-language Q&A over all 16 Delta tables. Predefined questions by category or free-form input, with SQL transparency.",
         "Non-technical stakeholders (mayors, RWAs, citizens) need answers without learning SQL or navigating dashboards.",
         "How do I get a specific answer from the data without any technical skill?"),
        ("🏗️", "How It Works", "Full architecture diagram: data pipeline, scoring engine, AI integration, and the technology behind every page.",
         "Judges and technical reviewers need to understand the engineering depth — what was built, what was hard, what is innovative.",
         "How was this built? What technologies power it? What made it technically challenging?"),
    ]
    for icon, name, what, why, resolves in page_cards:
        with st.expander(f"{icon} **{name}**", expanded=False):
            st.markdown(f"**What it does:** {what}")
            st.markdown(f"**Why it matters:** {why}")
            st.markdown(f"**Key question it resolves:** *{resolves}*")

    # --- Who should use this ---
    st.subheader("👥 Who Is This For?")
    st.markdown("""
    | User | How They Use TransitGap |
    | --- | --- |
    | **Urban Planners (AMC/AUDA)** | Identify which areas need feeder routes or metro extensions first |
    | **Transport Authority (GMRC/AMTS)** | Prioritize service gaps using real GTFS data + metro coverage |
    | **City Leadership** | Data-backed briefings on transit equity — no guesswork |
    | **Researchers & Students** | Open methodology + scored dataset for urban mobility studies |
    | **Citizens & RWAs** | Understand your area's transit score and see what improvement would look like |
    """)

    st.info("💡 **End users can go directly to Ask Genie** — no SQL knowledge needed. Pick a question category "
            "(Gap Analysis, Scenarios, Data Quality) and get instant answers from the live data.")

# ---------------------------------------------------------------- Map
elif page == "Map":
    st.info("🎯 **What this resolves:** *Where exactly are the transit deserts, and how do they relate to metro corridors?*\n\n"
            "💡 **Tip:** Enable 'Show nearest metro lines' to see which metro station each neighbourhood depends on.")
    mc1, mc2, mc3 = st.columns(3)
    show_bus = mc1.checkbox("Show bus stops", value=True)
    show_brts_only = mc2.checkbox("Highlight BRTS stops", value=False)
    show_connections = mc3.checkbox("Show nearest metro lines", value=False)
    fig = go.Figure()
    if show_bus:
        if show_brts_only and "is_brts" in bus.columns:
            brts_df = bus[bus.is_brts == True]
            amts_df = bus[bus.is_brts == False]
            fig.add_trace(go.Scattermapbox(lat=amts_df.lat, lon=amts_df.lon, mode="markers", name=f"AMTS stops ({len(amts_df):,})",
                                           marker=dict(size=4, color="#aaaaaa", opacity=0.3), text=amts_df.name, hoverinfo="text"))
            fig.add_trace(go.Scattermapbox(lat=brts_df.lat, lon=brts_df.lon, mode="markers", name=f"BRTS stops ({len(brts_df):,})",
                                           marker=dict(size=7, color="#e377c2", opacity=0.8), text=brts_df.name, hoverinfo="text"))
        else:
            fig.add_trace(go.Scattermapbox(lat=bus.lat, lon=bus.lon, mode="markers", name=f"Bus stops ({len(bus):,} real GTFS)",
                                           marker=dict(size=5, color="#7f7f7f", opacity=0.4), text=bus.name, hoverinfo="text"))
    metro_map = metro.drop_duplicates(subset=["name"])  # one marker per station (interchange stations deduplicated)
    fig.add_trace(go.Scattermapbox(lat=metro_map.lat, lon=metro_map.lon, mode="markers", name=f"Metro stations ({n_metro_unique})",
                                   marker=dict(size=11, color="#1f77b4"), text=metro_map.name, hoverinfo="text"))
    if show_connections:
        for _, r in acc.iterrows():
            nm = metro[metro.name == r.nearest_metro]
            if len(nm):
                nm = nm.iloc[0]
                clr = COLORS.get(r.category, "#999")
                fig.add_trace(go.Scattermapbox(lat=[r.lat, nm.lat], lon=[r.lon, nm.lon], mode="lines",
                    line=dict(width=1.5, color=clr), name=f"{r['name']}→{nm['name']}", showlegend=False,
                    hoverinfo="text", hovertext=f"{r['name']} → {nm['name']}: {r.dist_metro_km} km"))
    for cat, col in COLORS.items():
        sub = acc[acc.category == cat]
        fig.add_trace(go.Scattermapbox(lat=sub.lat, lon=sub.lon, mode="markers+text", name=cat, textposition="top center",
                                       marker=dict(size=16, color=col, opacity=0.9), text=sub.name,
                                       hovertext=sub.apply(lambda r: f"{r['name']}: score {r.score:.0f} | {r.nearest_metro} ({r.dist_metro_km}km) | {r.bus_stops_1km} bus stops", axis=1), hoverinfo="text"))
    fig.update_layout(mapbox_style="open-street-map", mapbox=dict(center=dict(lat=23.03, lon=72.57), zoom=10.6),
                      height=700, margin=dict(l=0, r=0, t=0, b=0), legend=dict(orientation="h", y=1.02))
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Red = potential access gap, orange = moderate, green = well connected. Toggle 'nearest metro lines' to see which station each area depends on.")

# ---------------------------------------------------------------- Journey Planner
elif page == "Journey Planner":
    import math, time
    import numpy as np
    import requests as _rq
    st.markdown("""
    <div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%); border-radius: 16px;
        padding: 1.5rem 2rem; color: white; margin-bottom: 1rem;">
        <h3 style="color: white; margin: 0 0 0.5rem 0;">PS-08: One City, One Journey</h3>
        <p style="color: rgba(255,255,255,0.92); font-size: 0.95rem; line-height: 1.5; margin:0;">
        Metro, BRTS and city buses are run by different operators with separate tickets, and transfers are confusing.
        A commuter combining metro, BRTS and AMTS legs gets no single view of the journey, its total cost or the transfer walks involved.<br>
        <b>This planner</b> combines all three into one route with one fare estimate and real transfer walking times.</p>
    </div>
    """, unsafe_allow_html=True)
    MAX_WALK_KM = 0.8
    WALK_SPEED_KPH = 5.0
    MIN_PER_STATION = 2.5
    XFER_WAIT = 3  # minutes assumed bus-to-bus transfer wait
    INTERCHANGES = {("Blue", "Red"): "Old High Court", ("Red", "Yellow"): "Koteshwar Road", ("Yellow", "Violet"): "GNLU"}

    # --- OSRM with circuit breaker (stop calling after first failure) ---
    if "osrm_ok" not in st.session_state:
        st.session_state.osrm_ok = True

    def hav(lat1, lon1, lat2, lon2):
        R = 6371
        dlat, dlon = math.radians(lat2-lat1), math.radians(lon2-lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
        return R*2*math.asin(math.sqrt(a))

    def batch_hav(lat1, lon1, lats, lons):
        R = 6371
        dlat, dlon = np.radians(lats-lat1), np.radians(lons-lon1)
        a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1))*np.cos(np.radians(lats))*np.sin(dlon/2)**2
        return R*2*np.arcsin(np.sqrt(a))

    @st.cache_data(ttl=3600, show_spinner=False)
    def _osrm_call(lat1, lon1, lat2, lon2):
        url = f"https://router.project-osrm.org/route/v1/foot/{lon1},{lat1};{lon2},{lat2}?overview=full&geometries=geojson"
        r = _rq.get(url, timeout=2)
        r.raise_for_status()
        d = r.json()
        if d.get("code") != "Ok": raise RuntimeError(d.get("code"))
        rt = d["routes"][0]
        return [(c[1], c[0]) for c in rt["geometry"]["coordinates"]], rt["distance"]/1000, rt["duration"]/60

    def walk_route(lat1, lon1, lat2, lon2):
        """Real road walk from OSRM when available; straight-line otherwise."""
        if st.session_state.osrm_ok:
            try:
                c, km, mn = _osrm_call(round(lat1,5), round(lon1,5), round(lat2,5), round(lon2,5))
                return c, km, mn, "OSRM"
            except Exception:
                st.session_state.osrm_ok = False
        d = hav(lat1, lon1, lat2, lon2)
        return [(lat1, lon1), (lat2, lon2)], d, d/WALK_SPEED_KPH*60, "straight-line"

    def est_walk_min(km):
        return km / WALK_SPEED_KPH * 60

    # --- Cached GTFS index (built once, not on every click) ---
    @st.cache_resource(show_spinner="Indexing GTFS routes\u2026")
    def build_route_index(_rstops, _bus):
        rs = _rstops.sort_values(["route_id", "stop_sequence"])
        rsmap = rs.groupby("route_id")["stop_id"].apply(list).to_dict()
        first = rs.drop_duplicates("route_id").set_index("route_id")
        rname, ragency = first["route_short_name"].to_dict(), first["agency_id"].to_dict()
        sname = dict(zip(_bus.stop_id, _bus.name))
        sloc = dict(zip(_bus.stop_id, zip(_bus.lat, _bus.lon)))
        extra = rs[~rs.stop_id.isin(sloc)].drop_duplicates("stop_id")
        sloc.update(dict(zip(extra.stop_id, zip(extra.stop_lat, extra.stop_lon))))
        sname.update(dict(zip(extra.stop_id, extra.stop_name)))
        t = rs.dropna(subset=["arrival_time"]).copy()
        parts = t["arrival_time"].astype(str).str.split(":", expand=True)
        t["mins"] = pd.to_numeric(parts[0], errors="coerce")*60 + pd.to_numeric(parts[1], errors="coerce")
        rs_time = {(a,b): c for a,b,c in zip(t.route_id, t.stop_id, t.mins) if pd.notna(c)}
        return rsmap, rname, ragency, sname, sloc, rs_time

    # --- Metro path by line + interchange (not row index) ---
    def metro_path(metro_df, a_name, b_name):
        A = metro_df[metro_df.name == a_name].iloc[0]
        B = metro_df[metro_df.name == b_name].iloc[0]
        def seg(line, s_from, s_to):
            L = metro_df[metro_df.line == line].sort_values("seq")
            i = int(L[L.name == s_from].seq.iloc[0])
            j = int(L[L.name == s_to].seq.iloc[0])
            lo, hi = min(i,j), max(i,j)
            part = L[(L.seq >= lo) & (L.seq <= hi)]
            return part.iloc[::-1] if i > j else part
        if A.line == B.line:
            p = seg(A.line, A["name"], B["name"])
            return p, max(1, len(p)-1), f"{A.line} line"
        key = tuple(sorted([A.line, B.line]))
        x = INTERCHANGES.get(key)
        if x is None or x not in set(metro_df.name):
            return pd.concat([A.to_frame().T, B.to_frame().T]), 1, "lines not connected"
        p1, p2 = seg(A.line, A["name"], x), seg(B.line, x, B["name"])
        p = pd.concat([p1, p2.iloc[1:]])
        return p, max(1, len(p)-1), f"transfer at {x} ({A.line}\u2192{B.line})"

    # --- Filter operational stations for scoring/routing ---
    metro_op = metro[metro.status == "OPERATIONAL"].reset_index(drop=True) if "status" in metro.columns else metro
    idx = build_route_index(rstops, bus)

    # --- Real scheduled metro travel times (from metro_stop_times + metro_trips) ---
    @st.cache_resource(show_spinner=False)
    def build_metro_time_index(_metro_st, _metro):
        """(station_name_a, station_name_b) -> scheduled_minutes using first matching trip."""
        if len(_metro_st) == 0:
            return {}
        import re
        def norm(s): return re.sub(r"[^a-z0-9]", "", str(s).lower())
        name_by_norm = {norm(n): n for n in _metro.name.unique()}
        def to_name(stop_id):
            k = norm(stop_id.replace("_", " "))
            return name_by_norm.get(k) or next((v for kk, v in name_by_norm.items() if kk.startswith(k[:6])), stop_id)
        df = _metro_st.copy()
        df["name"] = df.stop_id.map(to_name)
        p = df.arrival_time.astype(str).str.split(":", expand=True)
        df["mins"] = pd.to_numeric(p[0], errors="coerce") * 60 + pd.to_numeric(p[1], errors="coerce") + pd.to_numeric(p.get(2, 0), errors="coerce").fillna(0) / 60
        out = {}
        for trip_id, g in df.sort_values(["trip_id", "stop_sequence"]).groupby("trip_id"):
            names, mins = g.name.tolist(), g.mins.tolist()
            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    key = (names[i], names[j])
                    rkey = (names[j], names[i])  # reverse direction
                    val = round(mins[j] - mins[i])
                    if key not in out:
                        out[key] = val
                    if rkey not in out:
                        out[rkey] = val
        return out

    metro_time = build_metro_time_index(metro_st, metro)

    # --- C1: Fare helpers (PS-08) ---
    @st.cache_resource(show_spinner=False)
    def build_brts_fare_index(_fare_brts):
        return {(a, b): float(c) for a, b, c in zip(_fare_brts.from_stop_id, _fare_brts.to_stop_id, _fare_brts.amount)} if len(_fare_brts) else {}
    brts_fare = build_brts_fare_index(fare_brts)

    def slab_fare(km, table):
        if len(table) == 0: return None
        row = table[table.max_km >= km].head(1)
        return float(row.amount.iloc[0]) if len(row) else float(table.amount.iloc[-1])

    def leg_fare_bus(rid, s_from, s_to, km):
        ag = ragency.get(rid, "")
        if ag == "AJL":
            f = brts_fare.get((s_from, s_to))
            return (f, "BRTS", "GTFS fare rule") if f is not None else (slab_fare(km, fare_amts), "BRTS", "stage table, assumed")
        return (slab_fare(km, fare_amts), "AMTS" if ag == "AMTS" else ag, "stage table, assumed")

    def leg_fare_metro(path_df):
        km = sum(hav(path_df.iloc[k].lat, path_df.iloc[k].lon, path_df.iloc[k+1].lat, path_df.iloc[k+1].lon) for k in range(len(path_df)-1))
        return slab_fare(km, fare_metro), "Metro", "GMRC slab, transcribed", km

    DISC = float(fare_policy.loc[fare_policy.parameter == "transfer_discount_inr", "value"].iloc[0]) if len(fare_policy) and "parameter" in fare_policy.columns else 5.0
    def split_fare(j):
        legs = [s for s in j["steps"] if s.get("fare")]
        total = sum(s["fare"] for s in legs)
        integrated = max(0.0, total - DISC * max(0, len(legs) - 1))
        pkm = {}
        for s in legs: pkm[s["operator"]] = pkm.get(s["operator"], 0) + s.get("km", 0)
        tot_km = sum(pkm.values()) or 1
        split = {op: round(integrated * k / tot_km) for op, k in pkm.items()}
        return dict(sum_fares=total, integrated=integrated, city_incentive=total - integrated, pkm=pkm, split=split)

    # --- Presets ---
    pc1, pc2 = st.columns(2)
    if pc1.button("\U0001f680 Navrangpura \u2192 Mahatma Mandir", use_container_width=True):
        st.session_state["jp_from"] = "Navrangpura"
        st.session_state["jp_to"] = "Tragad"
    if pc2.button("\U0001f68c Maninagar \u2192 Naroda (BRTS + AMTS)", use_container_width=True):
        st.session_state["jp_from"] = "Maninagar"
        st.session_state["jp_to"] = "Naroda"
    rank_by = st.radio("Rank by", ["Fastest", "Cheapest", "Least walking"], horizontal=True)

    areas_jp = q(f"SELECT * FROM {FQ}.areas")
    all_locs = sorted(areas_jp.name.tolist())
    _def_from = st.session_state.get("jp_from", all_locs[0]) if all_locs else ""
    _def_to = st.session_state.get("jp_to", all_locs[min(5, len(all_locs)-1)]) if all_locs else ""
    _idx_from = all_locs.index(_def_from) if _def_from in all_locs else 0
    _idx_to = all_locs.index(_def_to) if _def_to in all_locs else min(5, len(all_locs)-1)
    lc1, lc2 = st.columns(2)
    origin_name = lc1.selectbox("\U0001f4cd From", all_locs, index=_idx_from)
    dest_name = lc2.selectbox("\U0001f3c1 To", all_locs, index=_idx_to)

    if origin_name == dest_name:
        st.warning("Pick two different locations.")
    elif len(rstops) == 0:
        st.error(f"Route stop data not available. Run the pipeline to create {FQ}.route_stops.")
    else:
        origin = areas_jp[areas_jp.name==origin_name].iloc[0]
        dest = areas_jp[areas_jp.name==dest_name].iloc[0]
        direct_km = hav(origin.lat, origin.lon, dest.lat, dest.lon)
        st.caption(f"Direct distance: {direct_km:.1f} km")

        # --- Candidate search: estimate with haversine first, OSRM only for top 3 ---
        rsmap, rname, ragency, sname, sloc, rs_time = idx
        bus_lats, bus_lons, bus_ids = bus.lat.values, bus.lon.values, bus.stop_id.values
        orig_d = batch_hav(origin.lat, origin.lon, bus_lats, bus_lons)
        dest_d = batch_hav(dest.lat, dest.lon, bus_lats, bus_lons)
        od = dict(zip(bus_ids[orig_d <= MAX_WALK_KM], orig_d[orig_d <= MAX_WALK_KM]))
        dd = dict(zip(bus_ids[dest_d <= MAX_WALK_KM], dest_d[dest_d <= MAX_WALK_KM]))
        m_lats, m_lons, m_names = metro_op.lat.values, metro_op.lon.values, metro_op.name.values
        om_d = batch_hav(origin.lat, origin.lon, m_lats, m_lons)
        dm_d = batch_hav(dest.lat, dest.lon, m_lats, m_lons)
        # Pick best metro station pair (top-3 × top-3) minimising total
        # walk-to-metro + metro-span + metro-to-walk.  Prevents choosing a
        # "nearest" station on the wrong line that forces a cross-city detour.
        _om_top = np.argsort(om_d)[:3]
        _dm_top = np.argsort(dm_d)[:3]
        _best_pair, _best_ov = None, float('inf')
        for _oi in _om_top:
            for _di in _dm_top:
                if metro_op.iloc[int(_oi)]["name"] == metro_op.iloc[int(_di)]["name"]:
                    continue
                _ov = om_d[_oi] + hav(float(metro_op.iloc[int(_oi)].lat), float(metro_op.iloc[int(_oi)].lon),
                                       float(metro_op.iloc[int(_di)].lat), float(metro_op.iloc[int(_di)].lon)) + dm_d[_di]
                if _ov < _best_ov:
                    _best_ov = _ov
                    _best_pair = (int(_oi), int(_di))
        om_i, dm_i = _best_pair if _best_pair else (int(np.argmin(om_d)), int(np.argmin(dm_d)))
        om_st, dm_st = metro_op.iloc[om_i], metro_op.iloc[dm_i]

        # Metro detour penalty: if total path via metro >> direct trip, deprioritise
        _metro_span = hav(float(om_st.lat), float(om_st.lon), float(dm_st.lat), float(dm_st.lon))
        _metro_ratio = (om_d[om_i] + _metro_span + dm_d[dm_i]) / max(direct_km, 0.5)
        m_pen = 1.0 if _metro_ratio <= 2.0 else (99.0 if _metro_ratio > 5.0 else _metro_ratio / 1.5)

        def ride_time(rid, s1, s2, nstops):
            t1, t2 = rs_time.get((rid, s1)), rs_time.get((rid, s2))
            return (t2-t1, "GTFS") if t1 is not None and t2 is not None and t2 > t1 else (nstops*2, "est.")

        def seg_km(slist, i0, i1):
            """Sum of haversine between consecutive stops along the route segment."""
            d = 0.0
            for k in range(i0, i1):
                s1, s2 = slist[k], slist[k+1]
                if s1 in sloc and s2 in sloc:
                    d += hav(sloc[s1][0], sloc[s1][1], sloc[s2][0], sloc[s2][1])
            return d

        def detour_penalty(slist, i0, i1):
            """Penalize circuitous routes: ratio of route-path distance to stop-to-stop direct distance."""
            s1, s2 = slist[i0], slist[i1]
            if s1 not in sloc or s2 not in sloc:
                return 1.0
            direct = hav(sloc[s1][0], sloc[s1][1], sloc[s2][0], sloc[s2][1])
            path = seg_km(slist, i0, i1)
            if direct < 0.3:
                return 1.0  # very short trips — don't penalize
            ratio = path / direct
            if ratio > 5:
                return 99  # absurd detour — effectively skip
            return max(1.0, ratio / 2)  # >2x direct → penalty ramps up

        cands = []
        # 1) Direct bus
        for rid, slist in rsmap.items():
            om_ix = [i for i,s in enumerate(slist) if s in od][:2]
            dm_ix = [i for i,s in enumerate(slist) if s in dd][:2]
            for oi in om_ix:
                for di in dm_ix:
                    if di > oi:
                        bs, als = slist[oi], slist[di]
                        r_min, src = ride_time(rid, bs, als, di-oi)
                        pen = detour_penalty(slist, oi, di)
                        cands.append(dict(type="direct_bus", est=(est_walk_min(od[bs])+r_min+est_walk_min(dd[als]))*pen,
                                          rid=rid, oi=oi, di=di, bs=bs, als=als, ride=r_min, src=src,
                                          detour=round(pen, 1)))
        # 2) Walk + metro (both ends near a station)
        if om_d[om_i] <= MAX_WALK_KM and dm_d[dm_i] <= MAX_WALK_KM and om_st["name"] != dm_st["name"]:
            path, n, note = metro_path(metro_op, om_st["name"], dm_st["name"])
            sched_m = metro_time.get((om_st["name"], dm_st["name"]))
            metro_min = sched_m if sched_m else n * MIN_PER_STATION
            raw_est = est_walk_min(om_d[om_i]) + metro_min + est_walk_min(dm_d[dm_i])
            cands.append(dict(type="walk_metro", est=raw_est * m_pen,
                              om=om_st, dm=dm_st, path=path, n=n, note=note, detour=round(m_pen, 1)))
        # 3) Bus + metro
        if om_st["name"] != dm_st["name"]:
            mb = batch_hav(om_st.lat, om_st.lon, bus_lats, bus_lons)
            md = dict(zip(bus_ids[mb <= 0.5], mb[mb <= 0.5]))
            path, n, note = metro_path(metro_op, om_st["name"], dm_st["name"])
            sched_m = metro_time.get((om_st["name"], dm_st["name"]))
            metro_min = sched_m if sched_m else n * MIN_PER_STATION
            for rid, slist in rsmap.items():
                o_ix = [i for i,s in enumerate(slist) if s in od]
                m_ix = [i for i,s in enumerate(slist) if s in md]
                if o_ix and m_ix:
                    oi = o_ix[0]
                    mi = min((m for m in m_ix if m > oi), default=None)
                    if mi:
                        bs, als = slist[oi], slist[mi]
                        r_min, src = ride_time(rid, bs, als, mi-oi)
                        pen = detour_penalty(slist, oi, mi)
                        cands.append(dict(type="bus_metro", est=(est_walk_min(od[bs])+r_min+est_walk_min(md.get(als,0.3))+metro_min+est_walk_min(dm_d[dm_i]))*pen*m_pen,
                                          rid=rid, oi=oi, mi=mi, bs=bs, als=als, ride=r_min, src=src, om=om_st, dm=dm_st, path=path, n=n, note=note,
                                          detour=round(pen * m_pen, 1)))
        # 4) Bus + bus (one transfer at a shared stop — no metro needed)
        _s2r = {}
        for _rid, _sl in rsmap.items():
            for _sid in _sl:
                _s2r.setdefault(_sid, set()).add(_rid)
        _bb_seen = set()
        _o_routes = {rid for rid, slist in rsmap.items() if any(s in od for s in slist)}
        for r1 in _o_routes:
            slist1 = rsmap[r1]
            oi = next((i for i, s in enumerate(slist1) if s in od), None)
            if oi is None:
                continue
            for ti in range(oi + 1, min(oi + 30, len(slist1))):
                xstop = slist1[ti]
                if xstop not in _s2r:
                    continue
                for r2 in _s2r[xstop]:
                    if r2 == r1 or (r1, r2) in _bb_seen:
                        continue
                    slist2 = rsmap[r2]
                    ti2 = next((i for i, s in enumerate(slist2) if s == xstop), None)
                    if ti2 is None:
                        continue
                    di2 = next((i for i, s in enumerate(slist2) if s in dd and i > ti2), None)
                    if di2 is None:
                        continue
                    bs1, als2 = slist1[oi], slist2[di2]
                    if bs1 not in sloc or als2 not in sloc or xstop not in sloc:
                        continue
                    r1_min, r1_src = ride_time(r1, bs1, xstop, ti - oi)
                    r2_min, r2_src = ride_time(r2, xstop, slist2[di2], di2 - ti2)
                    pen1 = detour_penalty(slist1, oi, ti)
                    pen2 = detour_penalty(slist2, ti2, di2)
                    pen = max(pen1, pen2)
                    est_val = (est_walk_min(od[bs1]) + r1_min + XFER_WAIT + r2_min + est_walk_min(dd[als2])) * pen
                    cands.append(dict(type="bus_bus", est=est_val,
                                      r1=r1, r2=r2, oi=oi, ti=ti, ti2=ti2, di2=di2,
                                      bs1=bs1, als2=als2, xfer=xstop,
                                      ride1=r1_min, ride2=r2_min, src1=r1_src, src2=r2_src,
                                      detour=round(pen, 1)))
                    _bb_seen.add((r1, r2))
        cands.sort(key=lambda c: c["est"])
        top3 = cands[:3]

        # --- Materialize top 3 with real OSRM walking paths ---
        def materialize(c):
            steps, total, walk_total = [], 0, 0.0
            def walk_step(a, b, label):
                nonlocal total, walk_total
                coords, km, mn, src = walk_route(*a, *b)
                total += round(mn)
                walk_total += mn
                steps.append(dict(mode="\U0001f6b6 Walk", detail=f"{round(mn)} min ({km:.1f} km, {src}) to {label}", coords=coords,
                                  fare=0, operator="walk", fare_src="", km=round(km, 2), route="", time_min=round(mn)))
            def bus_step(rid, i0, i1, ride, src):
                nonlocal total
                total += ride
                slist = rsmap[rid]
                coords = [sloc[s] for s in slist[i0:i1+1] if s in sloc]
                km = seg_km(slist, i0, i1)
                amt, op, fsrc = leg_fare_bus(rid, slist[i0], slist[i1], km)
                lab = "BRTS" if ragency.get(rid)=="AJL" else ragency.get(rid, "")
                steps.append(dict(mode=f"\U0001f68c {lab} {rname.get(rid,rid)}",
                    detail=f"{i1-i0} stops (~{ride} min, {src}) to {sname.get(slist[i1],slist[i1])}", coords=coords,
                    fare=amt or 0, operator=op, fare_src=fsrc, km=round(km, 2), route=rname.get(rid, rid), time_min=ride))
            def metro_step(c):
                nonlocal total
                sched = metro_time.get((c["om"]["name"], c["dm"]["name"]))
                if sched:
                    mn, m_src = sched, "GMRC timetable"
                else:
                    mn, m_src = c["n"] * MIN_PER_STATION, f"{MIN_PER_STATION} min/stn est."
                total += round(mn)
                amt, op, fsrc, km = leg_fare_metro(c["path"])
                steps.append(dict(mode="\U0001f687 Metro",
                    detail=f"{c['om']['name']} \u2192 {c['dm']['name']} ({c['n']} stations, ~{round(mn)} min, {m_src}, {c['note']})",
                    coords=list(zip(c["path"].lat.astype(float), c["path"].lon.astype(float))),
                    fare=amt or 0, operator=op, fare_src=fsrc, km=round(km, 2), route="Metro", time_min=round(mn)))
            def xfer_step(xstop_id):
                nonlocal total
                total += XFER_WAIT
                xname = sname.get(xstop_id, xstop_id)
                steps.append(dict(mode="\U0001f504 Transfer", detail=f"at {xname} (~{XFER_WAIT} min wait)",
                    coords=[sloc.get(xstop_id, (0,0))]*2, fare=0, operator="transfer", fare_src="", km=0, route="", time_min=XFER_WAIT))
            if c["type"] == "direct_bus":
                walk_step((origin.lat, origin.lon), sloc[c["bs"]], sname.get(c["bs"], c["bs"]))
                bus_step(c["rid"], c["oi"], c["di"], c["ride"], c["src"])
                walk_step(sloc[c["als"]], (dest.lat, dest.lon), dest_name)
            elif c["type"] == "walk_metro":
                walk_step((origin.lat, origin.lon), (c["om"].lat, c["om"].lon), f"{c['om']['name']} metro")
                metro_step(c)
                walk_step((c["dm"].lat, c["dm"].lon), (dest.lat, dest.lon), dest_name)
            elif c["type"] == "bus_bus":
                walk_step((origin.lat, origin.lon), sloc[c["bs1"]], sname.get(c["bs1"], c["bs1"]))
                bus_step(c["r1"], c["oi"], c["ti"], c["ride1"], c["src1"])
                xfer_step(c["xfer"])
                bus_step(c["r2"], c["ti2"], c["di2"], c["ride2"], c["src2"])
                walk_step(sloc[c["als2"]], (dest.lat, dest.lon), dest_name)
            else:  # bus_metro
                walk_step((origin.lat, origin.lon), sloc[c["bs"]], sname.get(c["bs"], c["bs"]))
                bus_step(c["rid"], c["oi"], c["mi"], c["ride"], c["src"])
                walk_step(sloc[c["als"]], (c["om"].lat, c["om"].lon), f"{c['om']['name']} metro")
                metro_step(c)
                walk_step((c["dm"].lat, c["dm"].lon), (dest.lat, dest.lon), dest_name)
            total_fare = sum(s["fare"] for s in steps)
            n_xfers = sum(1 for i in range(1, len(steps)) if steps[i].get("operator","") != steps[i-1].get("operator","") and steps[i]["operator"] not in ("walk","transfer")) - (1 if len([s for s in steps if s["operator"] not in ("walk","transfer")]) > 0 else 0)
            n_xfers = max(0, n_xfers)
            return dict(type=c["type"], total_min=total, steps=steps, detour=c.get("detour", 1.0),
                        total_fare=total_fare, walk_min=round(walk_total), n_transfers=n_xfers)

        best = [materialize(c) for c in cands[:5]]
        if rank_by == "Cheapest":
            best.sort(key=lambda j: (j["total_fare"], j["total_min"]))
        elif rank_by == "Least walking":
            best.sort(key=lambda j: j["walk_min"])
        else:
            best.sort(key=lambda j: j["total_min"])
        best = best[:3]

        if not best:
            st.warning(f"No route found within {MAX_WALK_KM} km walking distance. Try nearby areas.")
        else:
            st.success(f"Found {len(cands)} routes. Showing top {len(best)} (ranked by {rank_by.lower()}):")
            for i, j in enumerate(best):
                _type_labels = {"direct_bus": "\U0001f68c Direct Bus", "walk_metro": "\U0001f6b6+\U0001f687 Walk + Metro",
                                "bus_bus": "\U0001f68c+\U0001f68c Bus Transfer", "bus_metro": "\U0001f68c+\U0001f687 Bus + Metro"}
                lbl = _type_labels.get(j["type"], j["type"])
                indirect = " \u26a0\ufe0f indirect" if j.get("detour", 1.0) > 1.5 else ""
                sf = split_fare(j)
                with st.expander(f"**Option {i+1}: {lbl}** \u2014 ~{j['total_min']} min \u00b7 \u20b9{j['total_fare']:.0f} (one ticket \u20b9{sf['integrated']:.0f}) \u00b7 {j['walk_min']} min walk \u00b7 {j['n_transfers']} xfers{indirect}", expanded=(i==0)):
                    if j.get("detour", 1.0) > 1.5:
                        st.warning("This route is indirect \u2014 the bus takes a circuitous path. Look for shorter options below.")
                    for sn, step in enumerate(j["steps"], 1):
                        st.markdown(f"**{sn}. {step['mode']}** \u2192 {step['detail']}")
                    # --- Fare table per leg ---
                    fare_rows = []
                    for sn2, step in enumerate(j["steps"], 1):
                        if step["operator"] in ("walk", "transfer"):
                            fare_rows.append({"Leg": sn2, "Operator": step["operator"].title(), "Route": "", "Time": step["time_min"], "Fare \u20b9": 0, "Source": ""})
                        else:
                            fare_rows.append({"Leg": sn2, "Operator": step["operator"], "Route": step.get("route",""), "Time": step["time_min"], "Fare \u20b9": step["fare"], "Source": step["fare_src"]})
                    st.dataframe(pd.DataFrame(fare_rows), hide_index=True, use_container_width=True)
                    if sf["sum_fares"] > 0:
                        split_str = " / ".join(f"{op} \u20b9{v}" for op, v in sf["split"].items())
                        st.markdown(f"**One ticket:** \u20b9{sf['sum_fares']:.0f} \u2192 \u20b9{sf['integrated']:.0f} (\u20b9{sf['city_incentive']:.0f} city incentive) \u2192 {split_str}")
                    # --- Map with real paths ---
                    jfig = go.Figure()
                    step_colors = {"Walk": "#2196F3", "Bus": "#4CAF50", "BRTS": "#E91E63",
                                   "AMTS": "#4CAF50", "GTSL": "#FF9800", "Metro": "#9C27B0"}
                    for si, step in enumerate(j["steps"]):
                        coords = step.get("coords", [])
                        if not coords or len(coords) < 2:
                            continue
                        lats = [c[0] for c in coords]
                        lons = [c[1] for c in coords]
                        is_walk = "Walk" in step["mode"]
                        is_metro = "Metro" in step["mode"]
                        color = "#2196F3" if is_walk else ("#9C27B0" if is_metro else "#4CAF50")
                        for k, v in step_colors.items():
                            if k in step["mode"]:
                                color = v
                                break
                        jfig.add_trace(go.Scattermapbox(
                            lat=lats, lon=lons, mode="lines",
                            line=dict(width=3 if is_walk else 5, color=color),
                            name=step["mode"], hovertext=step["detail"], hoverinfo="text"))
                        # Stop markers for bus/metro (intermediate points)
                        if not is_walk and len(coords) > 2:
                            jfig.add_trace(go.Scattermapbox(
                                lat=lats[1:-1], lon=lons[1:-1], mode="markers",
                                marker=dict(size=5, color=color, opacity=0.7),
                                name="Stops", showlegend=False, hoverinfo="skip"))
                    # Origin + destination markers
                    jfig.add_trace(go.Scattermapbox(lat=[origin.lat], lon=[origin.lon], mode="markers+text",
                        marker=dict(size=14, color="green", symbol="circle"),
                        text=[origin_name], textposition="top center", name="Origin"))
                    jfig.add_trace(go.Scattermapbox(lat=[dest.lat], lon=[dest.lon], mode="markers+text",
                        marker=dict(size=14, color="red", symbol="circle"),
                        text=[dest_name], textposition="top center", name="Destination"))
                    mid_lat, mid_lon = (origin.lat+dest.lat)/2, (origin.lon+dest.lon)/2
                    jfig.update_layout(mapbox_style="open-street-map",
                                       mapbox=dict(center=dict(lat=mid_lat, lon=mid_lon), zoom=12),
                                       height=450, margin=dict(l=0,r=0,t=0,b=0),
                                       showlegend=True, legend=dict(orientation="h", y=1.02))
                    st.plotly_chart(jfig, use_container_width=True)

    walk_src = "OSRM road network" if st.session_state.osrm_ok else "straight-line (OSRM unreachable)"
    metro_src = "scheduled times from GMRC timetable" if len(metro_time) else f"{MIN_PER_STATION} min/station est."
    st.caption(f"Walking: {walk_src}. Bus times: GTFS where available. Metro: {metro_src}. Top options shown, not exhaustive.")

# ---------------------------------------------------------------- Ranking
elif page == "Gap Ranking":
    st.info("🎯 **What this resolves:** *Why does a specific area score poorly, and what single factor would improve it most?*")

    # --- PDF Export ---
    def build_gap_pdf():
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=20)
        pdf.add_page()
        # Title
        pdf.set_font("Helvetica", "B", 22)
        pdf.cell(0, 14, "TransitGap - Accessibility Gap Report", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 6, f"Ahmedabad Metro | Generated {datetime.now().strftime('%d %b %Y %H:%M')} | {FQ}", new_x="LMARGIN", new_y="NEXT")
        pdf.cell(0, 6, "Distances are straight-line (haversine). Bus data: GTFS (AMTS/BRTS/GTSL). Metro: official GMRC KML.", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(6)
        # Summary metrics
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, "1. Summary", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 10)
        for label, val in [("Areas analyzed", len(acc)), ("Metro stations", n_metro_unique),
                           ("Bus stops (real GTFS)", f"{len(bus):,}"), ("Bus routes", f"{n_routes:,}"),
                           ("Potential access gaps", n_gaps), ("Moderate", n_moderate), ("Well connected", n_good)]:
            pdf.cell(55, 6, f"{label}:", new_x="RIGHT")
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(0, 6, str(val), new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "", 10)
        pdf.ln(4)
        # All areas ranked
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, "2. All Areas Ranked by Accessibility Score", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "B", 8)
        col_w = [10, 32, 14, 36, 22, 36, 20, 20]
        headers = ["Rank", "Area", "Score", "Category", "Metro km", "Nearest Metro", "Bus km", "Stops 1km"]
        for w, h in zip(col_w, headers):
            pdf.cell(w, 6, h, border=1)
        pdf.ln()
        pdf.set_font("Helvetica", "", 7)
        for _, row in acc.iterrows():
            if pdf.get_y() > 260:
                pdf.add_page()
            cat = row.category
            if cat == "Potential access gap":
                pdf.set_fill_color(255, 220, 220)
            elif cat == "Moderate":
                pdf.set_fill_color(255, 243, 220)
            else:
                pdf.set_fill_color(220, 255, 220)
            vals = [str(int(row["rank"])), str(row["name"]), f"{row.score:.1f}", cat,
                    f"{row.dist_metro_km:.1f}", str(row.nearest_metro), f"{row.dist_bus_km:.2f}", str(int(row.bus_stops_1km))]
            for w, v in zip(col_w, vals):
                pdf.cell(w, 5, v[:18], border=1, fill=True)
            pdf.ln()
        pdf.ln(4)
        # Route coverage
        if len(route_cov):
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 14)
            pdf.cell(0, 10, "3. Bus Route Coverage by Area (within 1km)", new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "B", 8)
            rc_w = [32, 22, 32, 20, 28, 28]
            rc_h = ["Area", "Stops 1km", "Route Connections", "BRTS", "Busiest Stop", "Avg Routes"]
            for w, h in zip(rc_w, rc_h):
                pdf.cell(w, 6, h, border=1)
            pdf.ln()
            pdf.set_font("Helvetica", "", 7)
            for _, row in route_cov.sort_values("total_route_connections", ascending=False).iterrows():
                vals = [str(row.area), str(int(row.stops_1km)), str(int(row.total_route_connections)),
                        str(int(row.brts_stops)), str(int(row.busiest_stop_routes)), str(row.avg_routes_per_stop)]
                for w, v in zip(rc_w, vals):
                    pdf.cell(w, 5, v[:20], border=1)
                pdf.ln()
            pdf.ln(4)
        # Scoring methodology
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        sn = "4" if len(route_cov) else "3"
        pdf.cell(0, 10, f"{sn}. Scoring Methodology", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 9)
        pdf.multi_cell(0, 5, "score = 40 x clamp(1 - dist_metro/3) + 35 x clamp(1 - dist_bus/1) + 25 x clamp(stops_1km/8)")
        pdf.ln(2)
        pdf.multi_cell(0, 5, "Categories: < 35 = Potential access gap | 35-60 = Moderate | > 60 = Well connected")
        pdf.ln(2)
        pdf.multi_cell(0, 5, "Metro component (weight 40): Normalized proximity to nearest metro station. Threshold 3km.\n"
                       "Bus component (weight 35): Normalized proximity to nearest bus stop. Threshold 1km.\n"
                       "Density component (weight 25): Bus stop count within 1km, normalized to 8 stops.")
        pdf.ln(4)
        # Scenarios
        nn = str(int(sn) + 1)
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, f"{nn}. Feeder Scenarios (Hypothetical)", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "B", 8)
        sc_w = [50, 28, 22, 22, 18, 28]
        sc_h = ["Scenario", "Connects To", "Before", "After", "Delta", "Areas Affected"]
        for w, h in zip(sc_w, sc_h):
            pdf.cell(w, 6, h, border=1)
        pdf.ln()
        pdf.set_font("Helvetica", "", 7)
        for _, row in scen.iterrows():
            vals = [str(row.area_name), str(row.connects_to), f"{row.score_before:.1f}",
                    f"{row.score_after:.1f}", f"+{row.delta:.1f}", str(int(row.n_areas_affected))]
            for w, v in zip(sc_w, vals):
                pdf.cell(w, 5, v[:28], border=1)
            pdf.ln()
        pdf.ln(6)
        # Footer
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(120, 120, 120)
        pdf.multi_cell(0, 4, "Data: Metro from GMRC KML (mid=1ebnAmbRUbMyI_quB06reRZWcyfcLF8M). "
                       "Bus from GTFS (github.com/croyla/amd-gtfs). Scenarios modeled, not validated.")
        buf = BytesIO()
        buf.write(pdf.output())
        buf.seek(0)
        return buf

    st.download_button(
        "📄 Download Gap Report (PDF)",
        data=build_gap_pdf(),
        file_name=f"TransitGap_Report_{datetime.now().strftime('%Y%m%d')}.pdf",
        mime="application/pdf",
        use_container_width=False,
    )
    cats = st.multiselect("Category", list(COLORS), default=list(COLORS))
    tbl = acc[acc.category.isin(cats)]
    # Merge route coverage if available
    if len(route_cov):
        tbl_display = tbl.merge(route_cov, left_on="name", right_on="area", how="left", suffixes=("", "_rc"))
        show_cols = ["rank", "name", "score", "category", "dist_metro_km", "nearest_metro", "dist_bus_km",
                     "bus_stops_1km", "total_route_connections", "brts_stops", "has_brts_1km", "reasons"]
        show_cols = [c for c in show_cols if c in tbl_display.columns]
    else:
        tbl_display = tbl
        show_cols = ["rank", "name", "score", "category", "dist_metro_km", "nearest_metro", "dist_bus_km",
                     "bus_stops_500m", "bus_stops_1km", "has_brts_1km", "reasons", "data_origin"]
    st.dataframe(tbl_display[show_cols], hide_index=True, use_container_width=True, height=420)

    # --- Area deep-dive ---
    sel = st.selectbox("Inspect an area", tbl.name.tolist())
    r = acc[acc.name == sel].iloc[0]
    st.subheader(f"{r['name']} — score {r.score} ({r.category})")
    st.write(f"**Why:** {r.reasons}.")

    # Score breakdown + visual bar
    comp = pd.DataFrame({
        "component": ["Metro proximity", "Bus proximity", "Stop density"],
        "input": [f"{r.dist_metro_km} km to {r.nearest_metro}", f"{r.dist_bus_km} km to {r.nearest_bus}", f"{int(r.bus_stops_1km)} stops within 1 km"],
        "normalized (0–1)": [r.metro_component, r.bus_component, r.density_component],
        "weight": [40, 35, 25],
    })
    comp["points"] = (comp["normalized (0–1)"] * comp["weight"]).round(1)
    dc1, dc2 = st.columns([3, 2])
    with dc1:
        st.dataframe(comp, hide_index=True, use_container_width=True)
    with dc2:
        fig_comp = px.bar(comp, x="points", y="component", orientation="h",
                          color="component", color_discrete_sequence=["#1f77b4", "#ff7f0e", "#2ca02c"],
                          range_x=[0, 40], text="points")
        fig_comp.update_layout(showlegend=False, height=200, margin=dict(l=0, r=0, t=10, b=0),
                               xaxis_title="Points (max: weight)", yaxis_title="")
        st.plotly_chart(fig_comp, use_container_width=True)
    st.caption(f"score = Σ(normalized × weight) = **{comp.points.sum():.1f}**. Distance method: {r.distance_method}.")

    # Route coverage detail for this area
    if len(route_cov):
        rc = route_cov[route_cov.area == sel]
        if len(rc):
            rc = rc.iloc[0]
            st.subheader(f"🚌 Route Coverage: {sel}")
            rc_cols = st.columns(5)
            rc_cols[0].metric("Bus stops (1km)", int(rc.stops_1km))
            rc_cols[1].metric("Route connections", int(rc.total_route_connections))
            rc_cols[2].metric("BRTS stops", int(rc.brts_stops))
            rc_cols[3].metric("Busiest stop", f"{int(rc.busiest_stop_routes)} routes")
            rc_cols[4].metric("Avg routes/stop", rc.avg_routes_per_stop)

# ---------------------------------------------------------------- Scenario
elif page == "What-If Scenarios":
    st.info("🎯 **What this resolves:** *If we add one new bus route connecting an underserved area directly to the nearest metro station, how much does the transit score improve?*\n\n"
            "💡 Each scenario models a simple idea: a new direct bus route from a neighbourhood with poor access to its closest metro station. "
            "The score is recalculated assuming residents can now easily reach that station by bus.")
    st.warning("These are hypothetical scenarios — not approved by any transport authority. They show what *could* improve if the route were built.")
    sel = st.selectbox("Scenario", scen.name.tolist())
    s = scen[scen.name == sel].iloc[0]
    c = st.columns(4)
    c[0].metric("Score before", s.score_before)
    c[1].metric("Score after (modeled)", s.score_after, delta=f"{s.delta:+.1f}")
    c[2].metric("Connection length", f"{s.length_km} km")
    c[3].metric("Areas within 1.5 km", s.n_areas_affected)
    fig = go.Figure(data=[go.Bar(name="Before", x=[s.area_name], y=[s.score_before], marker_color="#d62728"),
                          go.Bar(name="After (modeled)", x=[s.area_name], y=[s.score_after], marker_color="#2ca02c")])
    fig.update_layout(barmode="group", yaxis=dict(range=[0, 100], title="Score"), height=320)
    st.plotly_chart(fig, use_container_width=True)
    st.write(f"**Proposed route:** New direct bus from **{s.area_name}** to **{s.connects_to}** metro station · **Nearby areas that also benefit:** {s.areas_affected}")
    st.write("**How the score changes:**")
    parts = s.explanation.split(" | ")
    st.write(parts[0])
    for p in parts[1:]:
        st.write(f"- {p}")
    st.subheader("All proposed routes ranked by impact")
    st.dataframe(scen[["scenario_id", "name", "score_before", "score_after", "delta", "n_areas_affected", "status"]],
                 hide_index=True, use_container_width=True)

# ---------------------------------------------------------------- Data journey
elif page == "Data Journey":
    st.info("🎯 **What this resolves:** *Can I trust these scores? Where did the data come from, and what are the caveats?*")
    st.markdown("**Source files / APIs → notebook validation → Delta tables → score → this UI and Genie.** The numbers below come from the last real pipeline run.")
    st.subheader("1. Data sources & provenance")
    st.dataframe(srcs[["dataset_id", "name", "source_url", "publisher", "license", "data_origin", "row_count", "fetched_at", "limitations"]],
                 hide_index=True, use_container_width=True)

    st.subheader("2. All 21 Tables \u2014 Key Columns & Sample Data")
    st.caption("Showing the most important columns per table so you can verify the data is real, structured, and correct.")
    areas_df = q(f"SELECT * FROM {FQ}.areas")
    TABLE_SHOWCASE = [
        ("metro_stations", metro, ["name", "lat", "lon", "line", "seq", "status", "source", "data_origin"],
         "54 unique stations (57 rows — interchange stations on both lines). Official GMRC KML. Blue (EW), Red (NS), Yellow (Gandhinagar), Violet (GIFT City)."),
        ("bus_stops", bus, ["name", "lat", "lon", "is_brts", "agency", "routes_served", "data_origin"],
         "3,138 real stops from GTFS. Each stop knows its agency (AMTS/BRTS/GTSL) and how many routes serve it."),
        ("bus_routes", bus_routes, ["route_id", "agency_id", "route_name", "route_desc"],
         "978 bus routes with origin-destination descriptions.") if len(bus_routes) else None,
        ("bus_trips", bus_trips, ["route_id", "trip_id", "trip_headsign", "direction_id"],
         "14,323 scheduled trips \u2014 trip count per route = frequency proxy.") if len(bus_trips) else None,
        ("areas", areas_df, ["name", "lat", "lon", "demand_weight"],
         "24 neighbourhoods with approximate centroids. Demand weight reserved for future population weighting."),
        ("area_accessibility", acc, ["rank", "name", "score", "category", "dist_metro_km", "nearest_metro", "dist_bus_km", "bus_stops_1km", "has_brts_1km", "metro_component", "bus_component", "density_component"],
         "Core scoring table. Score = 40\u00d7metro + 35\u00d7bus + 25\u00d7density. Category bands: <35 gap, 35-60 moderate, >60 good."),
        ("feeder_scenarios", scen, ["area_name", "connects_to", "length_km", "score_before", "score_after", "delta", "n_areas_affected"],
         "6 what-if scenarios: proposed bus routes from underserved areas to nearest metro."),
        ("data_sources", srcs, ["name", "source_url", "publisher", "license", "data_origin", "row_count"],
         "Provenance registry \u2014 every data source, its publisher, license, and limitations."),
        ("scoring_config", scfg, ["component", "weight", "threshold", "description"],
         "Formula weights and thresholds. Change these to re-prioritize metro vs bus vs density."),
        ("pipeline_runs", runs, ["run_id", "step", "input_rows", "output_rows", "rejected_rows", "status"],
         "Audit log \u2014 every pipeline step with row counts for data quality tracking."),
        ("route_stops", rstops, ["route_id", "agency_id", "route_short_name", "stop_sequence", "stop_name", "arrival_time"],
         "42K stop sequences for 972 bus routes \u2014 powers the Journey Planner. One representative trip per route with scheduled times.") if len(rstops) else None,
        ("metro_trips", metro_trips_df, ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"],
         "732 train runs from the GMRC timetable (effective 18 May 2026). Blue 560, Red 88, Yellow 68, Violet 16.") if len(metro_trips_df) else None,
        ("metro_stop_times", metro_st.head(500) if len(metro_st) else pd.DataFrame(), ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"],
         "14,268 rows: every train at every station. Powers scheduled metro times in the Journey Planner and Genie.") if len(metro_st) else None,
        ("metro_calendar", metro_calendar_df, ["service_id", "monday", "saturday", "sunday", "start_date", "end_date"],
         "Service patterns: WEEKDAY/SATURDAY/SUNDAY (Blue) and DAILY (Red/Yellow/Violet).") if len(metro_calendar_df) else None,
        ("metro_transfers", metro_transfers_df, ["from_stop_id", "to_stop_id", "min_transfer_time"],
         "Interchange transfer time at Old High Court, Koteshwar Road, GNLU \u2014 2 min assumption.") if len(metro_transfers_df) else None,
        ("methodology", methodology_df, ["step", "formula", "note"],
         "Step-by-step scoring documentation that Genie reads to explain the formula.") if len(methodology_df) else None,
        ("fare_brts_leg", fare_brts, ["from_stop_id", "to_stop_id", "amount"],
         f"{len(fare_brts):,} BRTS fare rules (GTFS-Fares v2). One row per (from_stop, to_stop) pair. Adult only.") if len(fare_brts) else None,
        ("fare_amts_stages", fare_amts, ["max_km", "amount", "source_note"],
         "AMTS stage-based fare chart (transcribed). Fare in \u20b9 for trips up to max_km.") if len(fare_amts) else None,
        ("fare_metro_slabs", fare_metro, ["max_km", "amount", "source_note"],
         "GMRC metro fare slabs (transcribed). Fare in \u20b9 for trips up to max_km.") if len(fare_metro) else None,
        ("fare_split_policy", fare_policy, ["parameter", "value", "note"],
         "Proposed integrated-ticket fare policy. NOT an existing scheme \u2014 illustrative parameters for PPP model.") if len(fare_policy) else None,
    ]
    for item in TABLE_SHOWCASE:
        if item is None:
            continue
        tname, tdf, key_cols, desc = item
        tdf_cols = set(tdf.columns)
        existing_cols = [c for c in key_cols if c in tdf_cols]
        all_col_names = ", ".join(tdf_cols)
        with st.expander(f"**{tname}** \u2014 {len(tdf):,} rows \u00b7 {len(existing_cols)} key columns", expanded=False):
            st.markdown(f"_{desc}_")
            st.dataframe(tdf[existing_cols].head(5), hide_index=True, use_container_width=True)
            dl1, dl2 = st.columns([3, 1])
            dl1.caption(f"Showing 5 of {len(tdf):,} rows. All {len(tdf_cols)} columns: {all_col_names}")
            dl2.download_button(f"\u2b07 Download {tname}.csv", data=tdf.to_csv(index=False),
                               file_name=f"{tname}.csv", mime="text/csv", key=f"dl_{tname}")

    st.subheader("3. Pipeline run log")
    st.dataframe(runs[["run_id", "step", "input_rows", "output_rows", "rejected_rows", "status", "completed_at", "message"]],
                 hide_index=True, use_container_width=True)
    st.subheader("4. Scoring configuration")
    st.dataframe(scfg, hide_index=True, use_container_width=True)
    st.code("score = 40·clamp(1 − dist_metro/3) + 35·clamp(1 − dist_bus/1) + 25·clamp(stops_1km/8)\n"
            "category: < 35 potential access gap · 35–60 moderate · > 60 well connected", language="text")
    if len(bus_routes):
        st.subheader("5. Bus route coverage (GTFS)")
        c = st.columns(3)
        c[0].metric("Total routes", f"{len(bus_routes):,}")
        c[1].metric("Total trips", f"{len(bus_trips):,}")
        c[2].metric("BRTS stops", f"{n_brts:,}")
        if "agency_id" in bus_routes.columns:
            st.dataframe(bus_routes.groupby("agency_id").size().reset_index(name="routes").sort_values("routes", ascending=False),
                         hide_index=True, use_container_width=True)
    st.subheader("6. Limitations")
    st.markdown("- Straight-line distances, not walking network\n- Area centroids approximate; demand weights synthetic and unused in the score\n"
                "- GTFS data from community-maintained repo (croyla/amd-gtfs), not official AMC open-data portal\n"
                "- No passenger volume, population density, or cost data\n- Scenarios are hypothetical modeled improvements")

# ---------------------------------------------------------------- Architecture
elif page == "How It Works":
    st.info("🎯 **What this resolves:** *How was this built? What technologies power it? What made it technically challenging?*")

    # --- Pipeline Architecture ---
    st.subheader("🏗️ End-to-End Architecture")
    st.markdown("""
    <div style="background: #f8f9fa; border: 2px solid #e0e0e0; border-radius: 12px; padding: 1.5rem; font-family: monospace; font-size: 0.85rem; line-height: 1.8; overflow-x: auto;">
    <b style="font-size:1rem;">DATA SOURCES</b><br>
    📡 GMRC Official KML ─────┐<br>
    &nbsp;&nbsp;&nbsp;(Google Maps, 54 stations)&nbsp;&nbsp;&nbsp;&nbsp;│<br>
    📡 GTFS Bus Data ────────┤&nbsp;&nbsp;──▶&nbsp;&nbsp;<b>Databricks Notebook Pipeline</b>&nbsp;&nbsp;──▶&nbsp;&nbsp;<b>16 Delta Tables</b>&nbsp;&nbsp;──▶&nbsp;&nbsp;<b>Streamlit App (8 pages)</b><br>
    &nbsp;&nbsp;&nbsp;(AMTS/BRTS/GTSL, 3138 stops)&nbsp;│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(Python + Spark SQL)&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(Unity Catalog)&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│<br>
    📡 Area Seed List ────────┘&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│<br>
    &nbsp;&nbsp;&nbsp;(24 neighbourhoods)&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│<br>
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>Databricks Genie AI</b>&nbsp;&nbsp;◀───┘<br>
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(Natural Language → SQL → Answer)
    </div>
    """, unsafe_allow_html=True)

    # --- How AI Works ---
    st.subheader("🤖 How the AI (Genie) Works")
    st.markdown("""
    The **Ask Genie** page uses Databricks' built-in AI engine to let anyone ask questions in plain English:

    1. **User types a question** → e.g. *"Which areas have the weakest transit access?"*
    2. **Genie reads the table metadata** → Column names, comments (109 AI-ready descriptions we added), table relationships
    3. **Genie generates SQL** → Automatically writes the correct query against the 16 Delta tables
    4. **SQL executes on Databricks warehouse** → Returns real, live data (not cached or hardcoded)
    5. **Genie explains the result** → Natural language answer + the SQL for full transparency

    This means **any user** — a mayor, a citizen, a student — can query the transit data without knowing SQL.
    """)

    # --- Scoring Engine ---
    st.subheader("🧮 Scoring Engine: How We Calculate Access Gaps")
    se1, se2 = st.columns([3, 2])
    with se1:
        st.markdown("""
        For each of the 24 neighbourhoods, we compute a **Transit Access Score (0–100)**:

        | Component | What it measures | Weight | Threshold |
        | --- | --- | --- | --- |
        | **Metro proximity** | How close is the nearest metro station? | 40/100 | 3 km (beyond = score 0) |
        | **Bus proximity** | How close is the nearest bus stop? | 35/100 | 1 km (beyond = score 0) |
        | **Stop density** | How many bus stops within walking distance? | 25/100 | 8 stops within 1 km = max |

        **Categories:**
        * Score < 35 → 🔴 **Potential access gap** (needs intervention)
        * Score 35–60 → 🟠 **Moderate** (some coverage, could improve)
        * Score > 60 → 🟢 **Well connected** (good multi-modal access)
        """)
    with se2:
        st.code("score = 40 × clamp(1 − dist_metro/3)\n"
                "      + 35 × clamp(1 − dist_bus/1)\n"
                "      + 25 × clamp(stops_1km/8)\n\n"
                "clamp(x) = max(0, min(1, x))\n\n"
                "Example: Hathijan\n"
                " metro: 2.78km → clamp(1-2.78/3) = 0.07\n"
                " bus:   0.59km → clamp(1-0.59/1) = 0.41\n"
                " stops: 5      → clamp(5/8) = 0.625\n"
                " score = 40×0.07 + 35×0.41 + 25×0.625\n"
                "       = 2.8 + 14.4 + 15.6 = 32.8", language="text")

    # --- Tech Stack ---
    st.subheader("🛠️ Technology Stack")
    st.markdown("""
    | Layer | Technology | Purpose |
    | --- | --- | --- |
    | **Data Platform** | Databricks (Unity Catalog) | Storage, governance, table-level permissions for the app's service principal |
    | **Pipeline** | Python notebook (PySpark + pandas) | Fetch GTFS/KML data, validate, compute scores, write 16 Delta tables |
    | **Storage** | Delta Lake | ACID tables with schema enforcement, column comments for AI |
    | **Scoring** | Haversine distance + weighted formula | Compute straight-line distances, normalize, weight, categorize |
    | **App** | Streamlit on Databricks Apps | 8-page interactive portal with maps, charts, journey planner, PDF export |
    | **Maps** | Plotly + OpenStreetMap | Interactive map with 3,000+ bus stops, 54 metro stations, area markers |
    | **AI Q&A** | Databricks Genie (NL-to-SQL) | Natural language queries over live Delta tables (incl. metro timetable) |
    | **PDF Export** | fpdf2 | Downloadable gap report with tables, scores, methodology |
    | **Bus Data** | GTFS (General Transit Feed Specification) | 3,138 real stops, 978 routes, 14,323 trips from AMTS/BRTS/GTSL |
    | **Metro Data** | Official GMRC KML (Google Maps) | 54 verified station coordinates across 4 metro lines |
    """)

    # --- What Was Hard ---
    st.subheader("⚡ Technical Challenges Overcome")
    challenges = [
        ("🚫 OSM API Blocked", "OpenStreetMap's Overpass API returns HTTP 406 from Databricks serverless egress IPs. "
         "We couldn't fetch live transit data the standard way.",
         "Found the croyla/amd-gtfs community GTFS repo — real schedules from AMTS/BRTS/GTSL official websites. "
         "Upgraded from 400 synthetic stops to 3,138 real stops with route frequency data."),
        ("📍 Unreliable Geocoding", "Wikipedia/Nominatim coordinates for metro stations were wildly inaccurate. "
         "9 out of 22 stations were >500m off; Narmada Canal station was 7km wrong.",
         "Sourced official GMRC KML from Google Maps (mid=1ebnAmbRUbMyI_quB06reRZWcyfcLF8M). "
         "All 54 stations now verified against the official source."),
        ("🔐 App Permissions", "The app's service principal couldn't access the data catalog — the original catalog was "
         "owned by a locked identity with no way to grant access.",
         "Migrated to a catalog where we had admin rights, granting USE CATALOG + USE SCHEMA + SELECT "
         "to the service principal, plus CAN_USE on SQL warehouses and CAN_RUN on Genie Space."),
        ("🤖 AI-Ready Metadata", "Genie gave generic answers because tables had no column descriptions.",
         "Added 109 column and table comments explaining every field — what it means, how it's computed, "
         "what values to expect. Genie now understands transit domain context."),
        ("📊 Real vs Synthetic", "Initial prototype used random noise for bus stops. Scores were meaningless.",
         "GTFS integration brought real schedules: route count per stop, BRTS detection via trip linkage, "
         "agency classification. Scores now reflect actual transit coverage."),
    ]
    for title, problem, solution in challenges:
        with st.expander(title):
            st.markdown(f"**Problem:** {problem}")
            st.markdown(f"**Solution:** {solution}")

    # --- Data Tables ---
    st.subheader("🗃️ Data Model: 16 Delta Tables")
    st.markdown("""
    | Table | Rows | Description |
    | --- | --- | --- |
    | `metro_stations` | 57 | Official GMRC station coordinates (4 lines, 2 phases; interchange stations on both lines) |
    | `bus_stops` | {:,} | Real GTFS stops with agency, BRTS flag, routes_served count |
    | `bus_routes` | {:,} | GTFS routes (AMTS + AJL/BRTS + GTSL) |
    | `bus_trips` | {:,} | Scheduled trips across all service_ids (frequency proxy) |
    | `areas` | 24 | Neighbourhood centroids for scoring |
    | `area_accessibility` | 24 | **Core table:** scores, components, categories, reasons |
    | `feeder_scenarios` | 6 | What-if models: new bus route impact |
    | `data_sources` | 5+ | Provenance registry (source, license, limitations) |
    | `scoring_config` | 5 | Formula weights and thresholds |
    | `methodology` | 5 | Step-by-step scoring documentation |
    | `route_stops` | 42K | Stop sequences for 972 routes — powers Journey Planner |
    | `pipeline_runs` | 6+ | Audit log of every pipeline step |
    | `metro_trips` | 732 | Train-level timetable: 560 Blue, 88 Red, 68 Yellow, 16 Violet |
    | `metro_stop_times` | 14,268 | Every train at every station: arrival/departure times |
    | `metro_calendar` | 4 | Service patterns: WEEKDAY/SATURDAY/SUNDAY/DAILY |
    | `metro_transfers` | 3 | Interchange transfer times (2 min assumed) |
    """.format(len(bus), n_routes, n_trips))

    st.success("**Built entirely on Databricks** — from data ingestion to AI-powered Q&A. "
              "No external servers, no manual data entry. The pipeline is re-runnable, "
              "the scores update automatically, and Genie answers evolve with the data.")

# ---------------------------------------------------------------- Genie
elif page == "Ask Genie":
    # --- Custom CSS for question cards ---
    st.markdown("""
    <style>
    .genie-hero { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 16px;
        padding: 2rem 2.5rem; color: white; margin-bottom: 1.5rem; }
    .genie-hero h2 { color: white !important; margin: 0 0 0.5rem 0; font-size: 1.8rem; }
    .genie-hero p { color: rgba(255,255,255,0.9); font-size: 1.05rem; margin: 0; line-height: 1.5; }
    .cat-header { display: flex; align-items: center; gap: 0.5rem; margin: 1.2rem 0 0.6rem 0; }
    .cat-header .icon { font-size: 1.4rem; }
    .cat-header .label { font-weight: 700; font-size: 1.1rem; color: #333; }
    .cat-desc { color: #666; font-size: 0.88rem; margin: -0.3rem 0 0.5rem 0; }
    </style>
    """, unsafe_allow_html=True)

    # --- Hero banner ---
    st.markdown("""
    <div class="genie-hero">
        <h2>🤖 Ask Genie — Natural Language Transit Analysis</h2>
        <p>Ask questions in plain English about Ahmedabad's metro accessibility gaps, proposed route scenarios,
        and data quality. Genie queries the same <b>16 Delta tables</b> powering this app — scores, distances,
        stations, what-if scenarios — and returns live answers with SQL transparency.<br><br>
        <b>Why use this?</b> Instead of writing SQL or navigating dashboards, simply ask what you need.
        Genie understands transit context and returns answers with the SQL it used, so you can verify everything.</p>
    </div>
    """, unsafe_allow_html=True)

    if not GENIE_SPACE_ID:
        st.error("GENIE_SPACE_ID is not set in app.yaml. Open the Genie space directly in Databricks for the demo.")
        st.stop()

    # --- Predefined question categories ---
    QUESTION_CATEGORIES = {
        "🎫 Fares & Tickets": {
            "desc": "How much does a multi-operator journey cost? PS-08 integrated fare model and per-operator splits.",
            "questions": [
                "What is the cheapest fare between any two BRTS stops?",
                "What is the GMRC metro fare for a 10 km journey?",
                "Show me the AMTS fare stages by distance",
                "What transfer discount does the integrated fare policy propose?",
                "How would the clearing house split a ₹20 fare between BRTS and metro?",
            ],
        },
        "🔴 Access Gap Analysis": {
            "desc": "Identify which areas are underserved and why — the core pain points for urban planners.",
            "questions": [
                "Which five areas have the weakest transit access?",
                "Which areas score zero and have no bus stops within 1 km?",
                "Why is Shela flagged as a potential access gap?",
                "How many areas fall in each accessibility category?",
                "Which areas are farthest from any metro station?",
            ],
        },
        "🚉 Metro & Station Coverage": {
            "desc": "Understand station distribution, line coverage, and which corridors serve which areas.",
            "questions": [
                "How many metro stations are there per phase and line?",
                "Which metro station serves the most nearby areas?",
                "What is the average distance from areas to the nearest metro?",
                "List all Phase 2 Yellow line stations from Motera to Mahatma Mandir",
                "Which areas are only served by Phase 1 stations?",
            ],
        },
        "🚌 Bus & BRTS Coverage": {
            "desc": "Evaluate bus stop density and BRTS availability — key for last-mile connectivity.",
            "questions": [
                "How many bus stops are within 1 km of each area?",
                "Which areas have BRTS coverage within 1 km?",
                "What is the bus stop density breakdown by area?",
                "Which areas have zero bus stops within 500 meters?",
                "How many bus stops are synthetic vs real?",
            ],
        },
        "📊 What-If: New Bus Routes": {
            "desc": "What if we add a new bus route from an underserved area to metro? See modeled impact.",
            "questions": [
                "Which proposed bus route gives the highest score improvement?",
                "Compare before and after scores for all proposed routes",
                "How many areas benefit if Bopal gets a direct bus to metro?",
                "What assumptions does the route impact model use?",
                "Rank proposed routes by the number of areas they improve",
            ],
        },
        "🕒 Metro Timetable (GMRC)": {
            "desc": "Train-level schedule effective 18 May 2026 — first/last trains, frequency, journey times.",
            "questions": [
                "What is the first Blue line train from Thaltej Gam on a weekday?",
                "How many Blue line trains run on a Sunday vs a weekday?",
                "How long does a metro trip take from APMC to Motera Stadium?",
                "How many trains depart Old High Court between 9 and 10 AM?",
                "Which metro line has the most daily trips?",
            ],
        },
        "🔍 Data Quality & Provenance": {
            "desc": "Verify data origins, limitations, and transparency — critical for trust in the analysis.",
            "questions": [
                "Which inputs are synthetic and which are real?",
                "What are the data sources and their licenses?",
                "Show the scoring formula weights and thresholds",
                "How many pipeline runs have been recorded?",
                "What are the known limitations of this analysis?",
            ],
        },
    }

    # --- Render category cards with clickable questions ---
    if "genie_selected" not in st.session_state:
        st.session_state.genie_selected = None

    for cat_name, cat_data in QUESTION_CATEGORIES.items():
        st.markdown(f'<div class="cat-header"><span class="label">{cat_name}</span></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="cat-desc">{cat_data["desc"]}</div>', unsafe_allow_html=True)
        cols = st.columns(len(cat_data["questions"]))
        for i, question in enumerate(cat_data["questions"]):
            with cols[i % len(cols)]:
                if st.button(question, key=f"q_{cat_name}_{i}", use_container_width=True):
                    st.session_state.genie_selected = question

    st.divider()

    # --- Chat history ---
    if "chat" not in st.session_state:
        st.session_state.chat = []
    for role, msg, df in st.session_state.chat:
        with st.chat_message(role):
            st.markdown(msg)
            if df is not None:
                st.dataframe(df, hide_index=True, use_container_width=True)

    # --- Chat input (typed or selected from cards) ---
    typed_prompt = st.chat_input("Or type your own question about transit gaps, scenarios, or data quality...")
    prompt = typed_prompt or st.session_state.genie_selected
    if st.session_state.genie_selected and not typed_prompt:
        st.session_state.genie_selected = None  # consume the selection

    if prompt:
        st.session_state.chat.append(("user", prompt, None))
        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("🔍 Genie is analyzing the transit data..."):
                try:
                    from databricks.sdk import WorkspaceClient
                    w = WorkspaceClient()
                    msg = w.genie.start_conversation_and_wait(space_id=GENIE_SPACE_ID, content=prompt)
                    texts, df = [], None
                    for att in (msg.attachments or []):
                        if getattr(att, "text", None) and att.text.content:
                            texts.append(att.text.content)
                        if getattr(att, "query", None):
                            if att.query.description:
                                texts.append(att.query.description)
                            try:
                                res = w.genie.get_message_attachment_query_result(
                                    space_id=GENIE_SPACE_ID, conversation_id=msg.conversation_id,
                                    message_id=msg.message_id, attachment_id=att.attachment_id)
                                sr = res.statement_response
                                cols = [c.name for c in sr.manifest.schema.columns]
                                df = pd.DataFrame(sr.result.data_array or [], columns=cols)
                            except Exception as qex:
                                texts.append(f"(could not fetch query result: {qex})")
                            if att.query.query:
                                texts.append(f"**SQL used:**\n```sql\n{att.query.query}\n```")
                    answer = "\n\n".join(texts) or "Genie returned no text."
                    st.markdown(answer)
                    if df is not None:
                        st.dataframe(df, hide_index=True, use_container_width=True)
                    st.session_state.chat.append(("assistant", answer, df))
                except Exception as ex:
                    err = (f"Genie call failed: {ex}. Check that the app's service principal has CAN RUN on the Genie space, "
                           "or open the Genie space directly in Databricks.")
                    st.error(err)
                    st.session_state.chat.append(("assistant", err, None))
