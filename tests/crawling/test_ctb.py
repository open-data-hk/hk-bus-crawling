import json
from pathlib import Path

from crawling.ctb import route_stop_url, routes_url, stop_url


def test_routes_url():
    co = "ctb"

    assert routes_url() == "https://rt.data.gov.hk/v2/transport/citybus/route/ctb"

    assert routes_url(co) == "https://rt.data.gov.hk/v2/transport/citybus/route/ctb"


def test_stop_url():
    stop_id = "001234"

    assert (
        stop_url(stop_id) == "https://rt.data.gov.hk/v2/transport/citybus/stop/001234"
    )


def test_route_stop_url():

    assert (
        route_stop_url("20A", "inbound")
        == "https://rt.data.gov.hk/v2/transport/citybus/route-stop/ctb/20A/inbound"
    )


def test_identical_files():
    stop_list_snapshot_file = "tests/snapshots/stopList.ctb.json"
    route_list_snapshot_file = "tests/snapshots/routeList.ctb.json"

    stop_list_data_file = "data/stopList.ctb.json"
    route_list_data_file = "data/routeList.ctb.json"

    stop_list_data_text = Path(stop_list_data_file).read_text(encoding="UTF-8")
    route_list_data_text = Path(route_list_data_file).read_text(encoding="UTF-8")

    stop_list_snapshot_text = Path(stop_list_snapshot_file).read_text(encoding="UTF-8")
    route_list_snapshot_text = Path(route_list_snapshot_file).read_text(
        encoding="UTF-8"
    )

    stop_list_data = json.loads(stop_list_data_text)
    route_list_data = json.loads(route_list_data_text)
    stop_list_snapshot = json.loads(stop_list_snapshot_text)
    route_list_snapshot = json.loads(route_list_snapshot_text)

    for stop_id, stop_data in stop_list_data.items():
        # data_timestamp could be inconsistent
        del stop_data["data_timestamp"]
        del stop_list_snapshot[stop_id]["data_timestamp"]
        assert stop_data == stop_list_snapshot[stop_id]
    for idx, route in enumerate(route_list_data):
        assert route == route_list_snapshot[idx]
