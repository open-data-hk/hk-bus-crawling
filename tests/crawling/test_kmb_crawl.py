from crawling.kmb_crawl import route_stops_url, routes_url, stops_url


def test_stops_url():
    assert stops_url() == "https://data.etabus.gov.hk/v1/transport/kmb/stop"


def test_routes_url():
    assert routes_url() == "https://data.etabus.gov.hk/v1/transport/kmb/route/"


def test_route_stops_url():
    assert (
        route_stops_url() == "https://data.etabus.gov.hk/v1/transport/kmb/route-stop/"
    )
