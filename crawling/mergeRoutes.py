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


def loadJson(path):
    with open(path, "r", encoding="UTF-8") as f:
        return json.load(f)


def writeJson(path, data, **kwargs):
    with open(path, "w", encoding="UTF-8") as f:
        json.dump(data, f, ensure_ascii=False, **kwargs)


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


def isSameStopSequence(co_stop_ids, w_stop_ids, whole_stop_list):
    if len(co_stop_ids) != len(w_stop_ids):
        return False

    for co_stop_id, w_stop_id in zip(co_stop_ids, w_stop_ids):
        co_stop = whole_stop_list[co_stop_id]
        w_stop = whole_stop_list[w_stop_id]
        dist = haversine(
            (co_stop["location"]["lat"], co_stop["location"]["lng"]),
            (w_stop["location"]["lat"], w_stop["location"]["lng"]),
            unit=Unit.METERS,  # specify that we want distance in metres, default unit is km
        )
        if dist >= 300:
            return False

    return True


def importRouteListJson(co, whole_route_list, whole_stop_list):
    co_route_list = loadJson(DATA_DIR / f"routeFareList.{co}.cleansed.json")
    co_stop_list = loadJson(DATA_DIR / f"stopList.{co}.json")
    for co_stop_id, co_stop in co_stop_list.items():
        if co_stop_id not in whole_stop_list:
            try:
                whole_stop_list[co_stop_id] = {
                    "name": {"en": co_stop["name_en"], "tc": co_stop["name_tc"]},
                    "location": {
                        "lat": float(co_stop["lat"]),
                        "lng": float(co_stop["lng"]),
                    },
                }
            except BaseException:
                print("Problematic stop: ", co_stop_id, file=stderr)

    for co_route in co_route_list:
        found = False
        special_type = 1
        orig = {
            "en": co_route["orig_en"].replace("/", "／"),
            "tc": co_route["orig_tc"].replace("/", "／"),
        }
        dest = {
            "en": co_route["dest_en"].replace("/", "／"),
            "tc": co_route["dest_tc"].replace("/", "／"),
        }

        for w_route in whole_route_list:
            if (
                co_route["route"] == w_route["route"]
                and co in w_route["co"]
                and isGtfsMatch(w_route, co_route)
            ):
                # skip checking if the bound is not the same
                if co in w_route["bound"] and w_route["bound"][co] != co_route["bound"]:
                    continue

                if isSameStopSequence(
                    co_route["stops"], w_route["stops"][0][1], whole_stop_list
                ):
                    found = True
                    w_route["stops"].append((co, co_route["stops"]))
                    w_route["bound"][co] = co_route["bound"]
                elif (
                    co_route["orig_en"].upper() == w_route["orig"]["en"].upper()
                    and co_route["dest_en"].upper() == w_route["dest"]["en"].upper()
                ):
                    special_type = int(w_route["serviceType"]) + 1
                    if co_route["route"] == "606" and co_route["dest_tc"].startswith(
                        "彩雲"
                    ):
                        print("Yes", special_type)

        if not found:
            whole_route_list.append(
                getRouteObj(
                    route=co_route["route"],
                    co=co_route["co"],
                    serviceType=co_route.get("service_type", special_type),
                    stops=[(co, co_route["stops"])],
                    bound={co: co_route["bound"]},
                    orig=orig,
                    dest=dest,
                    fares=co_route.get("fares", None),
                    faresHoliday=co_route.get("faresHoliday", None),
                    freq=co_route.get("freq", None),
                    jt=co_route.get("jt", None),
                    nlbId=co_route.get("id", None),
                    gtfsId=co_route.get("gtfs_id", co_route.get("gtfs", [None])[0]),
                    seq=len(co_route["stops"]),
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

    holidays = loadJson(DATA_DIR / "holiday.json")
    serviceDayMap = loadJson(DATA_DIR / "gtfs.json")["serviceDayMap"]

    db = standardizeDict(
        {
            "routeList": {getRouteId(v): v for v in routeList},
            "stopList": stopList,
            "stopMap": stopMap,
            "holidays": holidays,
            "serviceDayMap": serviceDayMap,
        }
    )

    writeJson(DATA_DIR / "routeFareList.mergeRoutes.json", db)
    writeJson(
        DATA_DIR / "routeFareList.mergeRoutes.min.json", db, separators=(",", ":")
    )


if __name__ == "__main__":
    main()
