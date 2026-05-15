from crawling.nlb import get_route_obj, get_stop_obj


def test_get_route_obj():
    route = {
        "routeId": "2",
        "routeNo": "1",
        "routeName_c": "大澳 > 梅窩碼頭",
        "routeName_s": "大澳 > 梅窝码头",
        "routeName_e": "Tai O > Mui Wo Ferry Pier",
        "overnightRoute": 0,
        "specialRoute": 0,
    }
    route_stops = [
        {
            "stopId": "221",
            "fare": "12.7",
            "fareHoliday": "21.4",
        },
        {
            "stopId": "305",
            "fare": "0.0",
            "fareHoliday": "0.0",
        },
    ]

    assert get_route_obj("nlb", route, route_stops) == {
        "id": "2",
        "route": "1",
        "bound": "O",
        "orig_en": "Tai O",
        "orig_tc": "大澳",
        "orig_sc": "大澳",
        "dest_en": "Mui Wo Ferry Pier",
        "dest_tc": "梅窩碼頭",
        "dest_sc": "梅窝码头",
        "service_type": "1",
        "stops": ["221", "305"],
        "fares": ["12.7"],
        "faresHoliday": ["21.4"],
        "co": "nlb",
    }


def test_get_route_obj_special_overnight_service_type():
    route = {
        "routeId": "99",
        "routeNo": "N1",
        "routeName_c": "起點 > 終點",
        "routeName_s": "起点 > 终点",
        "routeName_e": "Origin > Destination",
        "overnightRoute": 1,
        "specialRoute": 1,
    }

    assert get_route_obj("nlb", route, [])["service_type"] == "7"


def test_get_stop_obj():
    stop = {
        "stopId": "221",
        "stopName_c": "大澳",
        "stopName_s": "大澳",
        "stopName_e": "Tai O",
        "stopLocation_c": "大澳道",
        "stopLocation_s": "大澳道",
        "stopLocation_e": "Tai O Road",
        "latitude": "22.25278300",
        "longitude": "113.86216000",
    }

    assert get_stop_obj(stop) == {
        "stop": "221",
        "name_en": "Tai O",
        "name_tc": "大澳",
        "name_sc": "大澳",
        "lat": "22.25278300",
        "lng": "113.86216000",
    }
