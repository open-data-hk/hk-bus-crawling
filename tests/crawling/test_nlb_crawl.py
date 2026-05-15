from crawling.nlb_crawl import (
    get_stop_from_route_stop,
    get_stop_list,
    route_stop_url,
    routes_url,
)


def test_routes_url():
    assert (
        routes_url() == "https://rt.data.gov.hk/v2/transport/nlb/route.php?action=list"
    )


def test_route_stop_url():
    assert (
        route_stop_url("1")
        == "https://rt.data.gov.hk/v2/transport/nlb/stop.php?action=list&routeId=1"
    )


def test_get_stop_from_route_stop_discards_route_related_fields():
    route_stop = {
        "stopId": "1",
        "stopName_c": "梅窩碼頭",
        "stopName_s": "梅窝码头",
        "stopName_e": "Mui Wo Ferry Pier",
        "latitude": "22.26466400",
        "longitude": "114.00155400",
        "fare": "12.7",
        "fareHoliday": "21.4",
        "someDepartureObserveOnly": 0,
    }

    assert get_stop_from_route_stop(route_stop) == {
        "stopId": "1",
        "stopName_c": "梅窩碼頭",
        "stopName_s": "梅窝码头",
        "stopName_e": "Mui Wo Ferry Pier",
        "latitude": "22.26466400",
        "longitude": "114.00155400",
    }


def test_get_stop_list_deduplicates_stops_by_stop_id():
    route_stop_list = {
        "1": [
            {
                "stopId": "1",
                "stopName_e": "Mui Wo Ferry Pier",
                "fare": "12.7",
            }
        ],
        "2": [
            {
                "stopId": "1",
                "stopName_e": "Mui Wo Ferry Pier",
                "fare": "13.4",
            },
            {
                "stopId": "2",
                "stopName_e": "Silvermine Bay",
                "fareHoliday": "21.4",
            },
        ],
    }

    assert get_stop_list(route_stop_list) == {
        "1": {
            "stopId": "1",
            "stopName_e": "Mui Wo Ferry Pier",
        },
        "2": {
            "stopId": "2",
            "stopName_e": "Silvermine Bay",
        },
    }
