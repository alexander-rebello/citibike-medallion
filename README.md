# Citi Bike Medallion pipeline (DAT535)

Did the pandemic and e-bikes change how New Yorkers use Citi Bike? A Spark Bronze → Silver → Gold
pipeline over the public Citi Bike trip history (2013–2026).

## Layout

```
config/              dev.toml, prod.toml, schema_registry.toml
scripts/             download_raw.py   – mirror source archives into raw/ (gzip, unchanged bytes, manifest)
                     select_sources.py – one file set per month + header check -> source_selection.json
src/citibike/        pipeline code (config, registry, sources, …)
tests/               pytest
.github/workflows/   ci.yml (self-hosted runner)
```

Data lives outside the repo: raw under `~/citibike/data` (shared, read-only),
Bronze/Silver/Gold under `~/citibike/lake/<env>`.

## Run

```
source ~/spark-env/bin/activate
python scripts/download_raw.py --years 2015 2019 2020 2024     # raw landing
CITIBIKE_ENV=dev python scripts/select_sources.py               # file-set selection + header check
CITIBIKE_ENV=dev python scripts/run_bronze.py                   # Bronze: raw CSV -> Parquet, reconciled
python -m pytest
```

## Findings from profiling the source

- Four raw header spellings, two logical schemas (`legacy_v1`, `modern_v2`); see `config/schema_registry.toml`.
- The current bucket serves 2020 in the modern format (documentation says the change came in Feb 2021).
- Yearly archives for 2013 and 2018 contain some months twice (one whole file plus the same month
  split at 1M rows); `select_sources.py` keeps one set and proves the dropped set has equal row counts.
- Timestamp formats vary inside `legacy_v1` (`M/d/yyyy H:mm` in 2015, fractional seconds in 2018–19).
- Station ids must stay strings: legacy ids appear as `3160.0`, modern ids look like `6584.12`.
