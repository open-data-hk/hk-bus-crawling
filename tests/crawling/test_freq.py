import json
from pathlib import Path

SNAPSHOT = Path("tests/snapshots/operators_routes.json")
DATA = Path("data/operators_routes.json")


def compress_freq_entries(entries: dict) -> str:
    """Mirror of compress_freq_entries in crawling/parseGtfs.py."""
    if isinstance(entries, str):
        return entries
    parts = []
    prev_freq_end = None
    for start in sorted(entries.keys()):
        val = entries[start]
        if val is None:
            if prev_freq_end is not None and prev_freq_end != start:
                parts.append(prev_freq_end)
            prev_freq_end = None
            parts.append(start)
        else:
            end_time, headway_secs = val
            if prev_freq_end is not None and prev_freq_end != start:
                parts.append(prev_freq_end)
            parts.append(f"{start},{int(headway_secs) // 60}")
            prev_freq_end = end_time
    if prev_freq_end is not None:
        parts.append(prev_freq_end)
    return "|".join(parts)


def test_freq_matches_snapshot():
    snapshot = json.loads(SNAPSHOT.read_text(encoding="UTF-8"))
    current = json.loads(DATA.read_text(encoding="UTF-8"))

    mismatches = []
    for route_key, snap_route in snapshot.items():
        snap_freq = snap_route.get("freq")
        if not snap_freq:
            continue
        if route_key not in current:
            continue
        cur_freq = current[route_key].get("freq", {})

        for calendar, entries in snap_freq.items():
            expected = compress_freq_entries(entries)
            actual = compress_freq_entries(cur_freq.get(calendar, {}))
            if expected != actual:
                mismatches.append((route_key, calendar, expected, actual))

    if mismatches:
        lines = [f"\n{len(mismatches)} freq mismatch(es):"]
        for route_key, calendar, expected, actual in mismatches[:10]:
            lines.append(f"  route={route_key!r} calendar={calendar}")
            lines.append(f"    expected: {expected}")
            lines.append(f"    actual:   {actual}")
        assert False, "\n".join(lines)
