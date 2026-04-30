import json
from pathlib import Path

SNAPSHOT = Path("tests/snapshots/routeFareList.min.json")
DATA = Path("data/routeFareList.min.json")


def load_files():
    with open(SNAPSHOT, encoding="utf-8") as f:
        snapshot = {
            k: {"fares": v.get("fares"), "faresHoliday": v.get("faresHoliday")}
            for k, v in json.load(f)["routeList"].items()
        }
    with open(DATA, encoding="utf-8") as f:
        current = {
            k: {"fares": v.get("fares"), "faresHoliday": v.get("faresHoliday")}
            for k, v in json.load(f)["routeList"].items()
        }
    return snapshot, current


def normalize(fares_dict):
    def _norm(lst):
        return (
            [float(x) if x is not None else None for x in lst]
            if lst is not None
            else None
        )

    return {
        "fares": _norm(fares_dict["fares"]),
        "faresHoliday": _norm(fares_dict["faresHoliday"]),
    }


def test_fares_unchanged():
    snapshot, current = load_files()
    mismatches = []
    for route_key, snap_fares in snapshot.items():
        if route_key not in current:
            continue
        cur_fares = current[route_key]
        if normalize(cur_fares) != normalize(snap_fares):
            mismatches.append((route_key, snap_fares, cur_fares))

    if mismatches:
        lines = [f"\n{len(mismatches)} route(s) with changed fares:"]
        for key, snap, cur in mismatches:
            lines.append(f"  {key}")
            lines.append(f"    snapshot fares:        {snap['fares']}")
            lines.append(f"    current  fares:        {cur['fares']}")
            lines.append(f"    snapshot faresHoliday: {snap['faresHoliday']}")
            lines.append(f"    current  faresHoliday: {cur['faresHoliday']}")
        assert False, "\n".join(lines)
