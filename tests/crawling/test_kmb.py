from crawling.kmb import get_route_list, get_stop_list, route_key


def test_route_key():
    assert route_key("1", "1", "O") == "1+1+O"


def test_get_stop_list_renames_long_to_lng():
    raw_stop_list = [
        {
            "stop": "A",
            "name_en": "Alpha",
            "lat": "22.1",
            "long": "114.1",
        }
    ]

    assert get_stop_list(raw_stop_list) == {
        "A": {
            "stop": "A",
            "name_en": "Alpha",
            "lat": "22.1",
            "lng": "114.1",
        }
    }


def test_get_route_list_builds_stops_and_clones_missing_service_type():
    raw_route_list = [
        {
            "route": "1",
            "service_type": "1",
            "bound": "O",
            "orig_en": "Origin",
            "dest_en": "Destination",
        }
    ]
    raw_route_stop_list = [
        {
            "route": "1",
            "service_type": "1",
            "bound": "O",
            "seq": "2",
            "stop": "B",
        },
        {
            "route": "1",
            "service_type": "1",
            "bound": "O",
            "seq": "1",
            "stop": "A",
        },
        {
            "route": "1",
            "service_type": "2",
            "bound": "O",
            "seq": "1",
            "stop": "B",
        },
    ]
    stop_list = {
        "A": {"stop": "A"},
        "B": {"stop": "B"},
    }

    assert get_route_list(raw_route_list, raw_route_stop_list, stop_list) == [
        {
            "route": "1",
            "service_type": "1",
            "bound": "O",
            "orig_en": "Origin",
            "dest_en": "Destination",
            "stops": ["A", "B"],
            "co": "kmb",
        },
        {
            "route": "1",
            "service_type": "1",
            "bound": "O",
            "orig_en": "Origin",
            "dest_en": "Destination",
            "stops": ["B"],
            "co": "kmb",
        },
    ]


def test_get_route_list_filters_k_prefixed_route_keys_and_missing_stops():
    raw_route_list = [
        {
            "route": "K12",
            "service_type": "1",
            "bound": "O",
        },
        {
            "route": "1",
            "service_type": "1",
            "bound": "O",
        },
    ]
    raw_route_stop_list = [
        {
            "route": "K12",
            "service_type": "1",
            "bound": "O",
            "seq": "1",
            "stop": "A",
        },
        {
            "route": "1",
            "service_type": "1",
            "bound": "O",
            "seq": "1",
            "stop": "missing",
        },
    ]

    assert get_route_list(raw_route_list, raw_route_stop_list, {}) == [
        {
            "route": "1",
            "service_type": "1",
            "bound": "O",
            "stops": [],
            "co": "kmb",
        }
    ]
