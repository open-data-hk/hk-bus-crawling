import json
from sys import stderr

from haversine import Unit, haversine
from utils import DATA_DIR

routeList = []
stopList = {}
stopMap = {}
PROVIDERS = [
    "kmb",
    "ctb",
    "nlb",
    "lrtfeeder",
    "gmb",
    "lightRail",
    "mtr",
    "sunferry",
    "fortuneferry",
    "hkkf",
]


def getRouteObj(
    route,
    co,
    stops,
    bound,
    orig,
    dest,
    seq,
    fares,
    faresHoliday,
    freq,
    jt,
    nlbId,
    gtfsId,
    serviceType=1,
):
    return {
        "route": route,
        "co": co,
        "stops": stops,
        "serviceType": serviceType,
        "bound": bound,
        "orig": orig,
        "dest": dest,
        "fares": fares,
        "faresHoliday": faresHoliday,
        "freq": freq,
        "jt": jt,
        "nlbId": nlbId,
        "gtfsId": gtfsId,
        "seq": seq,
    }


def isGtfsMatch(knownRoute, newRoute):
    if knownRoute["gtfsId"] is None:
        return True
    if "gtfs" not in newRoute:
        return True

    return knownRoute["gtfsId"] in newRoute["gtfs"]


def importRouteListJson(co, route_list, stop_list):
    _routeList = json.load(
        open(DATA_DIR / ("routeFareList.%s.cleansed.json" % co), "r", encoding="UTF-8")
    )
    _stopList = json.load(
        open(DATA_DIR / ("stopList.%s.json" % co), "r", encoding="UTF-8")
    )
    for stopId, stop in _stopList.items():
        if stopId not in stop_list:
            try:
                stop_list[stopId] = {
                    "name": {"en": stop["name_en"], "tc": stop["name_tc"]},
                    "location": {"lat": float(stop["lat"]), "lng": float(stop["lng"])},
                }
            except BaseException:
                print("Problematic stop: ", stopId, file=stderr)

    for _route in _routeList:
        found = False
        special_type = 1
        orig = {
            "en": _route["orig_en"].replace("/", "／"),
            "tc": _route["orig_tc"].replace("/", "／"),
        }
        dest = {
            "en": _route["dest_en"].replace("/", "／"),
            "tc": _route["dest_tc"].replace("/", "／"),
        }

        for route in route_list:
            if (
                _route["route"] == route["route"]
                and co in route["co"]
                and isGtfsMatch(route, _route)
            ):
                # skip checking if the bound is not the same
                if co in route["bound"] and route["bound"][co] != _route["bound"]:
                    continue

                if len(_route["stops"]) == route["seq"]:
                    dist = 0
                    merge = True
                    for stop_a, stop_b in zip(_route["stops"], route["stops"][0][1]):
                        stop_a = stop_list[stop_a]
                        stop_b = stop_list[stop_b]
                        dist = haversine(
                            (stop_a["location"]["lat"], stop_a["location"]["lng"]),
                            (stop_b["location"]["lat"], stop_b["location"]["lng"]),
                            unit=Unit.METERS,  # specify that we want distance in metres, default unit is km
                        )
                        merge = merge and dist < 300
                    if merge:
                        found = True
                        route["stops"].append((co, _route["stops"]))
                        route["bound"][co] = _route["bound"]
                elif (
                    _route["orig_en"].upper() == route["orig"]["en"].upper()
                    and _route["dest_en"].upper() == route["dest"]["en"].upper()
                ):
                    special_type = int(route["serviceType"]) + 1
                    if _route["route"] == "606" and _route["dest_tc"].startswith(
                        "彩雲"
                    ):
                        print("Yes", special_type)

        if not found:
            route_list.append(
                getRouteObj(
                    route=_route["route"],
                    co=_route["co"],
                    serviceType=_route.get("service_type", special_type),
                    stops=[(co, _route["stops"])],
                    bound={co: _route["bound"]},
                    orig=orig,
                    dest=dest,
                    fares=_route.get("fares", None),
                    faresHoliday=_route.get("faresHoliday", None),
                    freq=_route.get("freq", None),
                    jt=_route.get("jt", None),
                    nlbId=_route.get("id", None),
                    gtfsId=_route.get("gtfs_id", _route.get("gtfs", [None])[0]),
                    seq=len(_route["stops"]),
                )
            )


def isMatchStops(stops_a, stops_b, debug=False):
    if len(stops_a) != len(stops_b):
        return False
    for v in stops_a:
        if stopMap.get(v, [[None, None]])[0][1] in stops_b:
            return True
    return False


def getRouteId(v):
    return "%s+%s+%s+%s" % (
        v["route"],
        v["serviceType"],
        v["orig"]["en"],
        v["dest"]["en"],
    )


def smartUnique(route_list):
    _routeList = []
    for i, route_i in enumerate(route_list):
        if route_i.get("skip", False):
            continue
        founds = []
        # compare route one-by-one
        for j, route_j in enumerate(route_list[i + 1 :], start=i + 1):
            if (
                route_i["route"] == route_j["route"]
                and len(route_i["stops"]) == len(route_j["stops"])
                and len([co for co in route_i["co"] if co in route_j["co"]]) == 0
                and isMatchStops(route_i["stops"][0][1], route_j["stops"][0][1])
            ):
                founds.append(j)
            elif (
                route_i["route"] == route_j["route"]
                and str(route_i["serviceType"]) == str(route_j["serviceType"])
                and route_i["orig"]["en"] == route_j["orig"]["en"]
                and route_i["dest"]["en"] == route_j["dest"]["en"]
            ):
                route_j["serviceType"] = str(int(route_j["serviceType"]) + 1)

        # update obj
        for found in founds:
            route_i["co"].extend(route_list[found]["co"])
            route_i["stops"].extend(route_list[found]["stops"])
            route_list[found]["skip"] = True

        # append return array
        _routeList.append(route_i)

    return _routeList


def standardizeDict(d):
    return {
        key: value if not isinstance(value, dict) else standardizeDict(value)
        for key, value in sorted(d.items())
    }


def main():
    global routeList

    for co in PROVIDERS:
        importRouteListJson(co, routeList, stopList)

    routeList = smartUnique(routeList)
    for route in routeList:
        route["stops"] = {co: stops for co, stops in route["stops"]}

    holidays = json.load(open(DATA_DIR / "holiday.json", "r", encoding="UTF-8"))
    serviceDayMap = json.load(open(DATA_DIR / "gtfs.json", "r", encoding="UTF-8"))[
        "serviceDayMap"
    ]

    db = standardizeDict(
        {
            "routeList": {getRouteId(v): v for v in routeList},
            "stopList": stopList,
            "stopMap": stopMap,
            "holidays": holidays,
            "serviceDayMap": serviceDayMap,
        }
    )

    with open(DATA_DIR / "routeFareList.mergeRoutes.json", "w", encoding="UTF-8") as f:
        f.write(json.dumps(db, ensure_ascii=False))

    with open(
        DATA_DIR / "routeFareList.mergeRoutes.min.json", "w", encoding="UTF-8"
    ) as f:
        f.write(json.dumps(db, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
