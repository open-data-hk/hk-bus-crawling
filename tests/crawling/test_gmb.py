import json
from pathlib import Path


def test_identical_files():
    stop_candidates_data = json.loads(
        Path("data/stopCandidates.gmb.json").read_text(encoding="UTF-8")
    )
    stop_list_data = json.loads(
        Path("data/stopList.gmb.json").read_text(encoding="UTF-8")
    )
    route_list_data = json.loads(
        Path("data/routeList.gmb.json").read_text(encoding="UTF-8")
    )

    stop_candidates_snapshot = json.loads(
        Path("tests/snapshots/stopCandidates.gmb.json").read_text(encoding="UTF-8")
    )
    stop_list_snapshot = json.loads(
        Path("tests/snapshots/stopList.gmb.json").read_text(encoding="UTF-8")
    )
    route_list_snapshot = json.loads(
        Path("tests/snapshots/routeList.gmb.json").read_text(encoding="UTF-8")
    )

    for stop_id, stop_data in stop_candidates_data.items():
        assert stop_data == stop_candidates_snapshot[stop_id]
    for stop_id, stop_data in stop_list_data.items():
        assert stop_data == stop_list_snapshot[stop_id]
    for idx, route in enumerate(route_list_data):
        assert route == route_list_snapshot[idx]
