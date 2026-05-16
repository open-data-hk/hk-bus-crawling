import pytest

from crawling.route_fare_key import build_route_fare_dict, get_route_unique_key


def test_get_route_fare_key_uses_route_identity_fields():
    route = {
        "co": ["kmb", "ctb"],
        "route": "102",
        "bound": "I",
        "service_type": "1",
        "orig_en": "Mei Foo",
        "dest_en": "Shau Kei Wan",
        "stops": ["001685", "001436", "001207"],
        "gtfs_route_id": "1484",
        "gtfs_route_seq": "2",
    }

    assert (
        get_route_unique_key(route)
        == "ctb,kmb|102|I|1|Mei Foo|Shau Kei Wan|001685|001207|3|1484|2"
    )


def test_get_route_fare_key_falls_back_to_gtfs_list():
    route = {
        "co": ["ctb"],
        "route": "722",
        "bound": "OI",
        "orig_en": "Yiu Tung Estate",
        "dest_en": "Yiu Tung Estate",
        "stops": ["002676", "001359", "002676"],
        "gtfs": ["1686"],
        "gtfs_route_seq": "1",
    }

    assert (
        get_route_unique_key(route)
        == "ctb|722|OI|1|Yiu Tung Estate|Yiu Tung Estate|002676|002676|3|1686|1"
    )


def test_build_route_fare_dict_rejects_duplicate_keys():
    route = {
        "co": ["mtr"],
        "route": "AEL",
        "bound": "DT",
        "service_type": "1",
        "orig_en": "AsiaWorld-Expo",
        "dest_en": "Hong Kong",
    }

    with pytest.raises(ValueError, match="Duplicate routeFare key"):
        build_route_fare_dict([route, route.copy()], source="mtr")


def test_build_route_fare_dict_stores_route_key_inside_route():
    route = {
        "co": ["mtr"],
        "route": "AEL",
        "bound": "DT",
        "service_type": "1",
        "orig_en": "AsiaWorld-Expo",
        "dest_en": "Hong Kong",
        "stops": ["AWE", "AIR", "HOK"],
    }

    route_fare_dict = build_route_fare_dict([route], source="mtr")
    route_key = "mtr|AEL|DT|1|AsiaWorld-Expo|Hong Kong|AWE|HOK|3||"

    assert route_fare_dict == {route_key: route}
    assert route["route_key"] == route_key
