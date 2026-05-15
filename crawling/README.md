# Crawling Scripts — Dependency Reference

Scripts must run after their listed inputs exist. Layers describe the earliest point each script can run; scripts in the same layer can run in parallel when their `Reads` columns do not depend on another script in that layer.

## Dependency Layers

### Layer 1 — GTFS bootstrap

| Script | Reads | Output files |
|--------|-------|-------------|
| `parseJourneyTime.py` | - | `routeTime.json` |
| `parseGtfs.py` | `routeTime.json` | `gtfs-tc.zip`, `gtfs-en.zip`, `gtfs-sc.zip`, `gtfs-tc/`, `gtfs-en/`, `gtfs-sc/`, `gtfs.json` |

`parseGtfs.py` can run at the beginning of the pipeline as soon as `parseJourneyTime.py` has created `routeTime.json`.

### Layer 2 — Independent raw/API preparation

| Script | Reads | Output files |
|--------|-------|-------------|
| `parseHoliday.py` | - | `holiday.json` |
| `ctb_crawl.py` | - | `ctb.raw.routeList.json`, `ctb.raw.routeStopList.json`, `ctb.raw.stopList.json` |
| `ctb.py` | `ctb.raw.routeList.json`, `ctb.raw.routeStopList.json`, `ctb.raw.stopList.json` | `routeList.ctb.json`, `stopList.ctb.json` |
| `kmb_crawl.py` | - | `kmb.raw.routeList.json`, `kmb.raw.routeStopList.json`, `kmb.raw.stopList.json` |
| `kmb.py` | `kmb.raw.routeList.json`, `kmb.raw.routeStopList.json`, `kmb.raw.stopList.json` | `routeList.kmb.json`, `stopList.kmb.json` |
| `nlb_crawl.py` | - | `nlb.raw.routeList.json`, `nlb.raw.routeStopList.json`, `nlb.raw.stopList.json` |
| `nlb.py` | `nlb.raw.routeList.json`, `nlb.raw.routeStopList.json`, `nlb.raw.stopList.json` | `routeList.nlb.json`, `stopList.nlb.json` |
| `lrtfeeder.py` | - | `routeList.lrtfeeder.json`, `stopList.lrtfeeder.json` |
| `lightRail.py` | - | `routeList.lightRail.json`, `stopList.lightRail.json` |
| `mtr.py` | - | `routeList.mtr.json`, `stopList.mtr.json` |
| `gmb_crawl.py` | - | `gmb.raw.routeNoList.json`, `gmb.raw.routeList.json`, `gmb.raw.routeStopList.json`, `gmb.raw.stopList.json` |
| `hkkf.py` | - | `routeList.hkkf.json`, `stopList.hkkf.json` |
| `mtrExits.py` | - | `exits.mtr.json` _(terminal — nothing downstream reads this)_ |

### Layer 3 — Needs `gtfs.json`

| Script | Reads | Output files |
|--------|-------|-------------|
| `gmb.py` | `gmb.raw.routeNoList.json`, `gmb.raw.routeList.json`, `gmb.raw.routeStopList.json`, `gmb.raw.stopList.json`, `gtfs-tc/calendar.txt`, `gtfs.json` | `routeList.gmb.json`, `stopList.gmb.json` |
| `sunferry.py` | `gtfs.json` | `routeList.sunferry.json`, `stopList.sunferry.json` |
| `fortuneferry.py` | `gtfs.json` | `routeList.fortuneferry.json`, `stopList.fortuneferry.json` |

These can run once Layer 1 completes and any listed raw files from Layer 2 exist.

### Layer 4 — Needs `gtfs.json` + all provider `routeList`/`stopList` files

| Script | Reads | Output files |
|--------|-------|-------------|
| `matchGtfs.py` | `gtfs.json`, `routeList.{co}.json`, `stopList.{co}.json` for all 10 operators | `routeFareList.{co}.json`, `routeGtfs.all.json` |

### Layer 5 — Needs all `routeFareList.{co}.json`

| Script | Reads | Output files |
|--------|-------|-------------|
| `cleansing.py` | `routeFareList.{co}.json` (all operators) | `routeFareList.{co}.cleansed.json` |

### Layer 6 — Needs `.cleansed.json` + `holiday.json` + `gtfs.json`

| Script | Reads | Output files |
|--------|-------|-------------|
| `mergeRoutes.py` | `routeFareList.{co}.cleansed.json`, `stopList.{co}.json`, `holiday.json`, `gtfs.json` | `routeFareList.mergeRoutes.min.json` |

### Layer 7 — Needs `routeFareList.mergeRoutes.min.json`

| Script | Reads | Output files |
|--------|-------|-------------|
| `mergeStopList.py` | `routeFareList.mergeRoutes.min.json` | `routeFareList.json`, `routeFareList.min.json` |

### Layer 8 — Needs `routeFareList.min.json`

| Script | Reads | Output files |
|--------|-------|-------------|
| `routeCompare.py` | `routeFareList.min.json` | `route-ts/` |

---

## Key notes

- **`gmb.py` is not independent** despite looking like the other operator scripts. It reads `gtfs-tc/calendar.txt` (produced by `parseGtfs.py`), making it a Layer 3 script.
- **`kmb.py` reads raw files from `kmb_crawl.py`**. Run `kmb_crawl.py` first when rebuilding from an empty `data/` directory.
- **`nlb.py` reads raw files from `nlb_crawl.py`**. Run `nlb_crawl.py` first when rebuilding from an empty `data/` directory.
- **`parseGtfs.py` can run at the start** after `parseJourneyTime.py`, because `routeTime.json` is its only generated input.
- **`sunferry.py` and `fortuneferry.py`** require `gtfs.json`, so they must wait for `parseGtfs.py`.
- **`gmb.py` reads raw files from `gmb_crawl.py`** plus GTFS calendar data. Run `gmb_crawl.py` and `parseGtfs.py` before `gmb.py`.
- **`mergeRoutes.py`** has a cross-layer dependency on `holiday.json` from Layer 2, so `parseHoliday.py` must complete before Layer 6.
- **`mtrExits.py`** is fully independent and can run at any time — nothing downstream consumes its output.
- The current workflow (`fetch-data.yml`) runs all scripts sequentially; independent scripts in layers 2 and 3 are candidates for parallelisation.
