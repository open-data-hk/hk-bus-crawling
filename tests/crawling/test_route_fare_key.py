import pytest

from crawling.operator import OPERATOR_CLASSES
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
        "co": ["unknown"],
        "route": "AEL",
        "bound": "DT",
        "service_type": "1",
        "orig_en": "AsiaWorld-Expo",
        "dest_en": "Hong Kong",
    }

    with pytest.raises(ValueError, match="Duplicate routeFare key"):
        build_route_fare_dict([route, route.copy()], source="unknown")


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
    route_key = "mtr|AEL|DT"

    assert route_fare_dict == {route_key: route}
    assert route["route_key"] == route_key


def test_build_route_fare_dict_uses_operator_route_key(monkeypatch):
    class CustomOperator:
        @classmethod
        def route_key(cls, route):
            return f"custom|{route['route']}|{route['bound']}"

    monkeypatch.setattr(
        "crawling.route_fare_key.get_operator_class",
        lambda operator_code: CustomOperator if operator_code == "custom" else None,
    )
    route = {
        "co": ["custom"],
        "route": "1",
        "bound": "O",
    }

    route_fare_dict = build_route_fare_dict([route], source="custom")

    assert route_fare_dict == {"custom|1|O": route}
    assert route["route_key"] == "custom|1|O"


def test_build_route_fare_dict_uses_kmb_route_key():
    route = {
        "co": ["kmb"],
        "route": "1",
        "bound": "O",
        "service_type": "1",
        "orig_en": "Star Ferry",
        "dest_en": "Chuk Yuen Estate",
        "stops": ["A", "B", "C"],
    }

    route_fare_dict = build_route_fare_dict([route], source="kmb")
    route_key = "kmb|1|O|1"

    assert route_fare_dict == {route_key: route}
    assert route["route_key"] == route_key


def test_build_route_fare_dict_falls_back_when_operator_route_key_collides():
    first_route = {
        "co": ["kmb"],
        "route": "84M",
        "bound": "O",
        "service_type": "1",
        "orig_en": "First Origin",
        "dest_en": "First Dest",
        "stops": ["A", "B", "C"],
    }
    second_route = {
        "co": ["kmb"],
        "route": "84M",
        "bound": "O",
        "service_type": "1",
        "orig_en": "Second Origin",
        "dest_en": "Second Dest",
        "stops": ["D", "E", "F"],
    }

    route_fare_dict = build_route_fare_dict([first_route, second_route], source="kmb")
    first_route_key = "kmb|84M|O|1"
    second_route_key = "kmb|84M|O|1|Second Origin|Second Dest|D|F|3||"

    assert route_fare_dict == {
        first_route_key: first_route,
        second_route_key: second_route,
    }
    assert first_route["route_key"] == first_route_key
    assert second_route["route_key"] == second_route_key


def test_all_provider_operator_classes_are_registered():
    assert sorted(OPERATOR_CLASSES) == [
        "ctb",
        "fortuneferry",
        "gmb",
        "hkkf",
        "kmb",
        "lightRail",
        "lrtfeeder",
        "lwb",
        "mtr",
        "nlb",
        "sunferry",
    ]


@pytest.mark.parametrize(
    ("source", "route", "route_key"),
    [
        (
            "ctb",
            {
                "route": "1",
                "bound": "O",
                "gtfs_route_id": "1480",
                "gtfs_route_seq": "1",
            },
            "ctb|1|O|1480|1",
        ),
        ("fortuneferry", {"route": "7059", "bound": "I"}, "fortuneferry|7059|I"),
        (
            "gmb",
            {
                "route": "1",
                "gtfs_route_id": "2006408",
                "gtfs_route_seq": "1",
            },
            "gmb|1|2006408|1",
        ),
        ("hkkf", {"route": "KF1", "bound": "O"}, "hkkf|KF1|O"),
        ("lightRail", {"route": "505", "bound": "I"}, "lightRail|505|I"),
        (
            "lrtfeeder",
            {
                "route": "K75S",
                "service_type": "1",
                "gtfs_route_id": "1870",
                "gtfs_route_seq": "1",
            },
            "lrtfeeder|K75S|1|1870|1",
        ),
        ("lwb", {"route": "A31", "bound": "O", "service_type": "1"}, "lwb|A31|O|1"),
        ("mtr", {"route": "AEL", "bound": "DT"}, "mtr|AEL|DT"),
        (
            "nlb",
            {
                "route": "1",
                "id": "1",
                "gtfs_route_id": "1723",
                "gtfs_route_seq": "1",
            },
            "nlb|1|1|1723|1",
        ),
        ("sunferry", {"route": "CECC"}, "sunferry|CECC"),
    ],
)
def test_operator_route_keys_are_compact(source, route, route_key):
    route["co"] = [source]

    route_fare_dict = build_route_fare_dict([route], source=source)

    assert route_fare_dict == {route_key: route}
    assert route["route_key"] == route_key
