from crawling.nlb_crawl import route_stop_url, routes_url


def test_routes_url():
    assert (
        routes_url() == "https://rt.data.gov.hk/v2/transport/nlb/route.php?action=list"
    )


def test_route_stop_url():
    assert (
        route_stop_url("1")
        == "https://rt.data.gov.hk/v2/transport/nlb/stop.php?action=list&routeId=1"
    )
