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
