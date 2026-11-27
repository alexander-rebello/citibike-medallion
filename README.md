# Citi Bike Medallion pipeline (DAT535)

Did the pandemic and e-bikes change how New Yorkers use Citi Bike? A PySpark Bronze → Silver → Gold pipeline over
the full public Citi Bike trip history (2013-06 .. 2026-10, 161 monthly datasets, 333.9 M raw rows), with a
dashboard for city planners built from the Gold tables.

| Layer | What it holds | Code |
|---|---|---|
| Raw | source CSVs, gzip-compressed with unchanged bytes, plus a manifest | `scripts/download_raw.py` |
| Bronze | one Parquet table per source schema, all columns as strings, reconciled row counts per month | `src/citibike/bronze.py` |
| Silver | cleaned trips with one common schema and station keys; rejected rows, duplicates, quality checks | `silver.py`, `silver_run.py`, `stations.py` |
| Gold | daily riders, hourly demand, station ranking per year, distance bands from Midtown | `gold.py` |
| Dashboard | static HTML page that reads only published Gold | `dashboard/`, `scripts/build_dashboard.py` |

Design notes and evidence: `docs/` (`bronze_profile.md`, `stations.md`, `silver_rules_impact.md`, `silver_run.md`,
`performance.md`).

## Layout

```
config/              dev.toml, prod.toml, failure_demo.toml   environments
                     schema_registry.toml                     known raw headers -> logical schema
                     silver_rules.toml                        Silver cleaning rules and timestamp formats
scripts/             one entry point per pipeline step, run_pipeline.py for the whole chain, profiling/report scripts
src/citibike/        pipeline code (config, registry, sources, spark, bronze, stations, silver, silver_run, gold, pipeline)
tests/               pytest on small fixtures with a local Spark session
infra/spark/         2-node standalone cluster: config, systemd units, cluster.sh
dashboard/           template.html (source), index.html / fragment.html / data.json (build output), check.js
.github/workflows/   ci.yml, prod.yml, failure-demo.yml
```

Data never goes into git. Raw data is shared and read-only under `~/citibike/data`; each environment has its own lake
`~/citibike/lake/<env>/{bronze,silver,gold,_meta}`.

## Cloud environment

Two virtual machines provided by the university and set up by hand through the OpenStack Horizon dashboard
(the separate cloud assignment): `spark-vm` (8 vCPU, 16 GB: Spark master and a worker, driver, NFS server for
`~/citibike`, GitHub Actions self-hosted runner) and `calc` (8 vCPU: second Spark worker, mounts `~/citibike` over NFS).
Everything that runs on them is configured from this repository (`infra/spark/`, `config/`, `.github/workflows/`).
Both environments, `dev` and `prod`, run on this cluster with separate lakes.

## Setup

Ubuntu 24.04, Java 8, Spark 3.5.0 in `/opt/spark`, Python 3.11.

```
python3.11 -m venv ~/spark-env
~/spark-env/bin/pip install -r requirements.txt
export PY=~/spark-env/bin/python
```

## Environments

The environment is chosen with `CITIBIKE_ENV` (default `dev`); settings live in `config/<env>.toml`.

| Env | Months | Lake | Used by |
|---|---|---|---|
| `dev` | 6 months that together cover every header and timestamp variant | `~/citibike/lake/dev` | local development, tests of whole runs (~2 min Silver) |
| `prod` | all months | `~/citibike/lake/prod` | scheduled workflow, results |
| `failure_demo` | small, with a deliberately broken manifest | `~/citibike/lake/failure_demo` | failure-handling demo |

## Run the pipeline

Whole chain (stops at the first failing step, later steps are not run):

```
CITIBIKE_ENV=dev  $PY scripts/run_pipeline.py --summary
CITIBIKE_ENV=prod $PY scripts/run_pipeline.py --months 2026-10 --summary   # one month
CITIBIKE_ENV=prod $PY scripts/run_pipeline.py --skip bronze.download --force   # re-process what is already downloaded
```

Three layers, each made of a few steps. Every step is its own process (own Spark session, log and exit code), so a failure
points at one step, and each step can also be run on its own with the same `CITIBIKE_ENV`. `--skip` takes a step
(`bronze.download`) or a whole layer (`gold`).

| Layer | Step | Script | Result |
|---|---|---|---|
| Bronze | `bronze.download` | `download_raw.py [--years ...]` | `~/citibike/data/raw/...`, `manifest/raw_manifest.jsonl` |
| Bronze | `bronze.select` | `select_sources.py` | one file set per month; stops the run on an unregistered header |
| Bronze | `bronze.ingest` | `run_bronze.py [--months ...] [--force]` | `bronze/trips_<schema>/year=/month=`, ledger, run log |
| Silver | `silver.stations` | `update_stations.py` | `silver/stations`, `station_map`, conflicts (incremental) |
| Silver | `silver.trips` | `run_silver.py` | `silver/{trips,rejected,duplicates,quality}`; only months that pass their checks are published |
| Gold | `gold.tables` | `run_gold.py` | four Gold tables, swapped in only if all totals match Silver, then `gold/_READY` |
| Gold | `gold.dashboard` | `build_dashboard.py` | `dashboard/index.html` (+ `fragment.html`, `data.json`), published to GitHub Pages by `prod.yml` |

All steps are incremental: a month is re-processed only when its input, its rules or its stations changed (digests in
the ledgers). `--force` overrides this.

Logs and run records per environment in `lake/<env>/_meta/`:
`pipeline_runs.jsonl` (one line per pipeline run), `pipeline_logs/<run_id>/<step>.log`,
`bronze_ledger.json`, `bronze_runs.jsonl`, `silver_ledger.json`, `silver_runs.jsonl`, `station_observations_ledger.json`.

## Run on the cluster

Two nodes (spark-vm + calc), standalone master, both mount `~/citibike` over NFS. Details: `infra/spark/README.md`.

```
infra/spark/cluster.sh deploy     # once: config + systemd units
infra/spark/cluster.sh status     # start | stop | restart
CITIBIKE_ENV=prod CITIBIKE_SPARK_MASTER=spark://192.168.26.92:7077 $PY scripts/run_pipeline.py --skip bronze.download
```

Without `CITIBIKE_SPARK_MASTER` Spark runs in local mode (`local[8]`). Event logs for performance analysis are written
to `~/citibike/spark-events/` (open with the Spark history server); results in `docs/performance.md`.

## Tests

```
$PY -m pytest -q        # ~3 min, Spark on small fixtures, timezone America/New_York
node dashboard/check.js dashboard/index.html   # dashboard: prints the generated sentences, fails on bad chart coordinates
```

## Known limitations

- `localCheckpoint()` (station scan, duplicate detection) keeps blocks on the executors: if a worker is lost mid-job the
  job fails instead of recomputing. A reliable `checkpoint()` to the shared disk would survive this at the cost of a write.
- The service-station, valet and Jersey City patterns run as regular expressions on every trip row although they only
  depend on the station; computing them on the station map would cut CPU in the Silver staging job (not yet measured).
- Silver partitions by source-file month, Gold by `start_date` month; Gold is rebuilt in full, so totals match, but an
  incremental Gold would need the neighbouring month.
- Raw row counts are newline counts: a quoted field containing a newline would make the reconciliation fail (loudly).
- `spark-vm` hosts the driver, NFS and the runner: a single point of failure.

## GitHub workflows (self-hosted runner on spark-vm)

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | every push and pull request | pytest + header check of the source selection; must be green before merging into `main` |
| `prod.yml` (`prod-pipeline`) | cron `0 6 5 * *` (5th of each month, 06:00 UTC) + manual with inputs `months`, `force`, `cluster` | runs the whole pipeline in `prod`; waits for approval in the `prod` environment; one run at a time; step summary; logs uploaded as an artifact; on failure the failing step's log is shown in the summary; after a successful run the dashboard is deployed to GitHub Pages |
| `failure-demo.yml` | manual | writes a manifest with an unregistered header; the run must stop at `bronze.select`, skip all later steps and fail with exit code 1 |

Branching: feature branch → pull request → green CI → merge into `main`. `main` is never pushed to directly.

## Dashboard

```
CITIBIKE_ENV=prod CITIBIKE_SPARK_MASTER=local[4] $PY scripts/build_dashboard.py
```

Reads only published Gold (refuses to build without `gold/_READY`). `dashboard/index.html` is a self-contained page
(inline SVG, no libraries, works offline, light and dark mode). The build output is not kept in git.

**Hosted at https://alexander-rebello.github.io/citibike-medallion/**: after every successful `prod-pipeline` run the
`publish-dashboard` job deploys the freshly built page to GitHub Pages. A failed run deploys nothing, so the site keeps
the last good dashboard. (One-time setting: Settings → Pages → Source: GitHub Actions.)

## Main results (prod run)

- Silver: 333,588,780 accepted trips (99.91 %), 299,547 rejected, 531 duplicates; accepted + rejected + duplicates = Bronze for every month.
- Casual-rider share 14.0 % (2019) → 26.7 % (2021) → 19.3 % (2024); e-bike share 14 % (2020) → 66 % (2024).
- Share of trips starting within 2 km of Midtown: 38 % (2015) → 29 % (2019) → 22 % (2024).
