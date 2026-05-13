# Crawling Scripts — Dependency Reference

Scripts must run in layer order. Within each layer, scripts are independent and can run in parallel.

## Dependency Layers

### Layer 1 — Independent (pure API fetchers)

| Script | Output files |
|--------|-------------|
| `parseHoliday.py` | `holiday.json` |
| `ctb.py` | `routeList.ctb.json`, `stopList.ctb.json` |
| `kmb.py` | `routeList.kmb.json`, `stopList.kmb.json` |
| `nlb.py` | `routeList.nlb.json`, `stopList.nlb.json` |
| `lrtfeeder.py` | `routeList.lrtfeeder.json`, `stopList.lrtfeeder.json` |
| `lightRail.py` | `routeList.lightRail.json`, `stopList.lightRail.json` |
| `mtr.py` | `routeList.mtr.json`, `stopList.mtr.json` |
| `parseJourneyTime.py` | `routeTime.json` |
| `mtrExits.py` | `exits.mtr.json` _(terminal — nothing downstream reads this)_ |

### Layer 2 — Needs `routeTime.json`

| Script | Reads | Output files |
|--------|-------|-------------|
| `parseGtfs.py` | `routeTime.json` | `gtfs.zip`, `gtfs-tc/` (extracted), `gtfs.json` |
| `parseGtfsEn.py` | `routeTime.json` | `gtfs-en.zip`, `gtfs-en/` (extracted), `gtfs-en.json` |

These two can run in parallel with each other.

### Layer 3 — Needs `gtfs.json` and/or `gtfs-en.json`

| Script | Reads | Output files |
|--------|-------|-------------|
| `gmb.py` | `gtfs-tc/calendar.txt`, `gtfs.json` | `routeList.gmb.json`, `stopList.gmb.json` |
| `sunferry.py` | `gtfs.json`, `gtfs-en.json` | `routeList.sunferry.json`, `stopList.sunferry.json` |
| `fortuneferry.py` | `gtfs.json`, `gtfs-en.json` | `routeList.fortuneferry.json`, `stopList.fortuneferry.json` |
| `hkkf.py` | `gtfs.json`, `gtfs-en.json` | `routeList.hkkf.json`, `stopList.hkkf.json` |

All four can run in parallel once Layer 2 completes.

### Layer 4 — Needs `gtfs.json` + all `routeList`/`stopList` from layers 1 & 3

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
- **`sunferry.py`, `fortuneferry.py`, `hkkf.py`** require *both* `gtfs.json` and `gtfs-en.json`, so they must wait for both `parseGtfs.py` and `parseGtfsEn.py`.
- **`mergeRoutes.py`** has a cross-layer dependency on `holiday.json` from Layer 1, so `parseHoliday.py` must complete before Layer 6.
- **`mtrExits.py`** is fully independent and can run at any time — nothing downstream consumes its output.
- The current workflow (`fetch-data.yml`) runs all scripts sequentially; layers 1 and 3 are candidates for parallelisation.
