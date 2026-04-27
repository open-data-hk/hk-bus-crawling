import json
from pathlib import Path


def test_identical_files():
    route_list_data = json.loads(
        Path("data/routeList.mtr.json").read_text(encoding="UTF-8")
    )
    stop_list_data = json.loads(
        Path("data/stopList.mtr.json").read_text(encoding="UTF-8")
    )
    fare_list_data = json.loads(
        Path("data/routeFareList.mtr.json").read_text(encoding="UTF-8")
    )

    route_list_snapshot = json.loads(
        Path("tests/snapshots/routeList.mtr.json").read_text(encoding="UTF-8")
    )
    stop_list_snapshot = json.loads(
        Path("tests/snapshots/stopList.mtr.json").read_text(encoding="UTF-8")
    )
    fare_list_snapshot = json.loads(
        Path("tests/snapshots/routeFareList.mtr.json").read_text(encoding="UTF-8")
    )

    for idx, route in enumerate(route_list_data):
        assert route == route_list_snapshot[idx]
    for stop_id, stop_data in stop_list_data.items():
        snapshot = stop_list_snapshot[stop_id]
        dp = 5
        assert {
            **stop_data,
            "lat": round(stop_data["lat"], dp),
            "long": round(stop_data["long"], dp),
        } == {
            **snapshot,
            "lat": round(snapshot["lat"], dp),
            "long": round(snapshot["long"], dp),
        }
    for idx, fare in enumerate(fare_list_data):
        assert fare == fare_list_snapshot[idx]
