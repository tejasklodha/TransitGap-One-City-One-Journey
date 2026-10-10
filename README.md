# TransitGap · One City, One Journey

**A metro + BRTS + AMTS journey planner for Ahmedabad: one route, one fare estimate, real transfer walks — built on Databricks.**

Metro Hacks (DevFest Gandhinagar, 10 Oct 2026) · Track: City Public Transport · **PS-08 — One City, One Journey** · Solo entry

> **Problem (PS-08):** Metro, BRTS and city buses are run by different operators with separate tickets, and transfers between them are confusing. A commuter who has to combine metro, BRTS and AMTS legs gets no single view of the journey, its total cost or the transfer walks involved. Many give up and use private vehicles instead.

TransitGap gives that single view: pick two neighbourhoods and get a combined walk + bus + metro itinerary from real schedules, with per-leg fares, a total, and a proposed one-ticket fare split between operators.

![Journey Planner](Journy%20Planner.png)

---

## What a judge can try

1. **Journey Planner** — choose any two neighbourhoods. The planner returns the top options across direct bus, bus + bus, walk + metro and bus + metro, each as a step-by-step itinerary drawn on a map with real road walking paths. Every leg shows where its number came from: bus times from GTFS, metro times from the GMRC timetable, walking from OSRM (straight-line fallback when offline, labelled).
2. **Fares** — per-leg fare and total for the journey. BRTS fares come from the feed's 108,870 GTFS fare rules (real); AMTS stage fares and metro slabs are transcribed from the operators' public fare charts and labelled as such.
3. **Ask Genie** — plain-English questions over every table, SQL shown. Try *"First Blue line train from Thaltej Gam on a weekday?"* or *"Which areas have BRTS within 1 km?"*
4. **Where journeys fail** — map, gap ranking and what-if feeder routes: neighbourhoods where no practical integrated journey exists today, and which feeder bus link would fix it.
5. **Data Journey** — every source, licence, row count, pipeline run log and stated assumption.

More screenshots: `Journy Planner 1.png` … `Journy Planner 5.png`, PDFs alongside.

---

## Data sources (bring-your-own-data, all public)

| Data | Source | Status |
|---|---|---|
| Metro stations (54, 4 lines) | GMRC station map layer / route map, gujaratmetrorail.com | Official, coordinates transcribed |
| Metro timetable (732 trains, 14,268 stop times, effective 18 May 2026) | GMRC train-information page (images) → community transcription github.com/darshan260802/metro-tracker → converted to GTFS by us | Official timetable via community transcription; unverified line-by-line |
| AMTS / BRTS / GTSL stops, routes, trips, times, BRTS fares (3,235 stops, 978 routes, 14,323 trips) | github.com/croyla/amd-gtfs (Mobility Database mdb-3131), MIT-0 | Community GTFS built from operator websites; not an official release |
| AMTS stage fares, metro fare slabs | Operators' public fare charts | Transcribed / assumed — labelled `APPROXIMATE` |
| Neighbourhood centroids (24) | Our seed list | Approximate |
| Walking routes | OSRM public server over OpenStreetMap | Online only; straight-line fallback labelled |

Every table carries a `data_origin` column (`REAL_PUBLIC` / `APPROXIMATE` / `SYNTHETIC` / `DERIVED`). We use no ridership, population, cost or real-time data and call no area "underserved".

---

## How it maps to the judging criteria

| Criterion | Evidence |
|---|---|
| Relevance | Exactly PS-08's deliverable: multi-operator journey, one fare estimate, transfer walking times, fare-split proposal |
| Working prototype | Itineraries render end to end from real GTFS (bus + metro) with OSRM walking and real BRTS fare rules |
| Resilience | OSRM circuit breaker with labelled fallback; warehouse-down message; offline variant with local AI (see `local_app` in the kit) |
| Technical depth | GTFS-Fares v2 join, metro GTFS built from the GMRC timetable, line/interchange routing with scheduled times, Genie NL→SQL over commented Delta tables |
| Feasibility & PPP | Integrated ticket: sum of leg fares minus a transfer discount; revenue cleared daily by a private clearing-house partner, split by passenger-km; discount funded by the city as a mode-shift incentive; one-corridor 3-month pilot |

---

## Repository contents

| File | Purpose |
|---|---|
| `transitgap_pipeline.py` | Databricks notebook: downloads GTFS, builds metro GTFS, validates, scores, writes all Delta tables, exports CSVs |
| `app.py`, `app.yaml`, `requirements.txt` | Streamlit Databricks App (Journey Planner, Genie, Data Journey, analysis pages) |
| `*.csv` | Export of the Delta tables (stations, stops, routes, trips, scores, fares, sources) so the data can be inspected without Databricks |
| `Journy Planner*.png / .pdf` | Screenshots and PDF exports of the running app |

## Run it

**Databricks:** import `transitgap_pipeline.py`, set `CATALOG` in cell 1, Run all → create a Genie space on the `transitgap` schema → deploy `app.py` as a Databricks App with a SQL warehouse resource, set `CATALOG`, `WAREHOUSE_HTTP_PATH`, `GENIE_SPACE_ID` in `app.yaml`, grant the app's service principal `USE CATALOG / USE SCHEMA / SELECT` and `CAN RUN` on the Genie space.

**Offline (no Databricks):** the CSVs in this repo are the same tables; the offline Streamlit variant reads them from a `data/` folder and answers questions with a local model via Ollama.

## Limitations

Scoring uses straight-line distances (the planner uses OSRM roads when online) · neighbourhood centroids approximate · AMTS and metro fares transcribed, BRTS fares real · fare split is a proposal, not an existing scheme · no ridership, population or cost data · feeder scenarios are hypothetical.

## Tech

Databricks (Unity Catalog, Delta Lake, Databricks Apps, Genie), Python (PySpark, pandas, NumPy), Streamlit, Plotly + OpenStreetMap, OSRM, GTFS / GTFS-Fares v2, fpdf2.
