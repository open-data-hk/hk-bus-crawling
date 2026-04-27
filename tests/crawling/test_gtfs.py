import json
from pathlib import Path


def test_identical_files():
    for name in ("gtfs.json", "gtfs-en.json"):
        data = json.loads(Path(f"data/{name}").read_text(encoding="UTF-8"))
        snapshot = json.loads(
            Path(f"tests/snapshots/{name}").read_text(encoding="UTF-8")
        )

        for route_id, route in data["routeList"].items():
            assert route == snapshot["routeList"][route_id]

        for stop_id, stop in data["stopList"].items():
            assert stop == snapshot["stopList"][stop_id]

        assert data["serviceDayMap"] == snapshot["serviceDayMap"]
