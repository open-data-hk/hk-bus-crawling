import json
from pathlib import Path

# Old per-language snapshots used lang_key as the key in orig/dest dicts,
# and stored stopName as {co: name_string}. The new unified gtfs.json stores
# orig/dest as {lang_key: name} with all languages, and stopName as
# {co: {lang_key: name}}.
LANG_SNAPSHOTS = [
    ("tests/snapshots/gtfs-tc.json", "tc"),
    ("tests/snapshots/gtfs-en.json", "en"),
]


def test_unified_gtfs_route_names_match_per_language_snapshots():
    new = json.loads(Path("data/gtfs.json").read_text(encoding="UTF-8"))

    for snapshot_file, lang_key in LANG_SNAPSHOTS:
        old = json.loads(Path(snapshot_file).read_text(encoding="UTF-8"))
        for route_id, old_route in old["routeList"].items():
            new_route = new["routeList"][route_id]
            for field in ("orig", "dest"):
                old_val = old_route[field].get(lang_key, "")
                if old_val:
                    assert new_route[field].get(lang_key) == old_val, (
                        f"{snapshot_file} route {route_id} {field}[{lang_key}]: "
                        f"expected {old_val!r}, got {new_route[field].get(lang_key)!r}"
                    )


def test_unified_gtfs_stop_names_match_per_language_snapshots():
    new = json.loads(Path("data/gtfs.json").read_text(encoding="UTF-8"))

    for snapshot_file, lang_key in LANG_SNAPSHOTS:
        old = json.loads(Path(snapshot_file).read_text(encoding="UTF-8"))
        for stop_id, old_stop in old["stopList"].items():
            new_stop = new["stopList"][stop_id]
            for co, old_name in old_stop["stopName"].items():
                # old stopName[co] is a plain string; new stopName[co] is {lang_key: name}
                assert new_stop["stopName"][co][lang_key] == old_name, (
                    f"{snapshot_file} stop {stop_id} stopName[{co}][{lang_key}]: "
                    f"expected {old_name!r}, got {new_stop['stopName'][co].get(lang_key)!r}"
                )


def test_unified_gtfs_non_language_fields_match_tc_snapshot():
    new = json.loads(Path("data/gtfs.json").read_text(encoding="UTF-8"))
    tc = json.loads(Path("tests/snapshots/gtfs-tc.json").read_text(encoding="UTF-8"))

    for route_id, tc_route in tc["routeList"].items():
        new_route = new["routeList"][route_id]
        for field in ("co", "route", "stops", "fares", "freq", "jt"):
            assert new_route[field] == tc_route[field], (
                f"route {route_id} {field}: expected {tc_route[field]!r}, "
                f"got {new_route[field]!r}"
            )

    for stop_id, tc_stop in tc["stopList"].items():
        new_stop = new["stopList"][stop_id]
        assert new_stop["stopId"] == tc_stop["stopId"]
        assert new_stop["lat"] == tc_stop["lat"]
        assert new_stop["lng"] == tc_stop["lng"]

    assert new["serviceDayMap"] == tc["serviceDayMap"]
