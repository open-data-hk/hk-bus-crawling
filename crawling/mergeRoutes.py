import csv
import io
import json
from sys import stderr

from haversine import Unit, haversine
from utils import DATA_DIR

routeList = []
stopList = {}
# TODO: remove or fix. stopMap is always empty here, isMatchStops is always false
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
    gtfsRouteId,
    gtfsRouteSeq,
    stops_and_alignment=None,
    serviceType=1,
):
    route_obj = {
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
        "gtfsRouteId": gtfsRouteId,
        "gtfsRouteSeq": gtfsRouteSeq,
        "seq": seq,
    }
    if stops_and_alignment:
        route_obj["stops_and_alignment"] = stops_and_alignment
    return route_obj


def addStopAlignmentToRoute(route, co, co_route):
    if "stop_alignment" in co_route:
        route.setdefault("stops_and_alignment", {})[co] = co_route["stop_alignment"]


def addCircularMetadataToRoute(route, co, co_route):
    if "circular_return_point" in co_route:
        route.setdefault("circular_return_point", {})[co] = co_route[
            "circular_return_point"
        ]
    if "circular_sections" in co_route:
        route.setdefault("circular_sections", {})[co] = co_route["circular_sections"]


def isGtfsMatch(knownRoute, newRoute):
    if knownRoute["gtfsRouteId"] is None:
        return True
    if "gtfs" not in newRoute:
        return True

    return knownRoute["gtfsRouteId"] in newRoute["gtfs"]


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


def isSameRouteCandidate(co, co_route, w_route):
    if co_route["route"] != w_route["route"]:
        return False
    if co not in w_route["co"]:
        return False
    if not isGtfsMatch(w_route, co_route):
        return False
    if co in w_route["bound"] and w_route["bound"][co] != co_route["bound"]:
        return False
    return True


def isOrigDestSameEnName(co_route, w_route):
    return (
        co_route["orig_en"].upper() == w_route["orig"]["en"].upper()
        and co_route["dest_en"].upper() == w_route["dest"]["en"].upper()
    )


def getStopObj(co_stop):
    return {
        "name": {
            "en": co_stop["name_en"],
            "tc": co_stop["name_tc"],
            "sc": co_stop["name_sc"],
        },
        "location": {
            "lat": float(co_stop["lat"]),
            "lng": float(co_stop["lng"]),
        },
    }


def getRouteNameObj(co_route, prefix):
    return {
        "en": co_route[f"{prefix}_en"].replace("/", "／"),
        "tc": co_route[f"{prefix}_tc"].replace("/", "／"),
        "sc": co_route[f"{prefix}_sc"].replace("/", "／"),
    }


def importRouteListJson(co, whole_route_list, whole_stop_list):
    co_route_list = loadJson(DATA_DIR / f"routeFareList.{co}.cleansed.json")
    co_stop_list = loadJson(DATA_DIR / f"stopList.{co}.json")
    for co_stop_id, co_stop in co_stop_list.items():
        if co_stop_id not in whole_stop_list:
            try:
                whole_stop_list[co_stop_id] = getStopObj(co_stop)
            except BaseException:
                print("Problematic stop: ", co_stop_id, file=stderr)

    for co_route in co_route_list:
        found = False
        special_type = 1
        orig = getRouteNameObj(co_route, "orig")
        dest = getRouteNameObj(co_route, "dest")

        for w_route in whole_route_list:
            if not isSameRouteCandidate(co, co_route, w_route):
                continue

            if isSameStopSequence(
                co_route["stops"], w_route["stops"][0][1], whole_stop_list
            ):
                found = True
                w_route["stops"].append((co, co_route["stops"]))
                w_route["bound"][co] = co_route["bound"]
                addStopAlignmentToRoute(w_route, co, co_route)
                addCircularMetadataToRoute(w_route, co, co_route)
            elif isOrigDestSameEnName(co_route, w_route):
                special_type = int(w_route["serviceType"]) + 1
                if co_route["route"] == "606" and co_route["dest_tc"].startswith(
                    "彩雲"
                ):
                    print("Yes", special_type)

        if not found:
            route_obj = getRouteObj(
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
                gtfsRouteId=co_route.get(
                    "gtfs_route_id", co_route.get("gtfs", [None])[0]
                ),
                gtfsRouteSeq=co_route.get("gtfs_route_seq"),
                stops_and_alignment=(
                    {co: co_route["stop_alignment"]}
                    if "stop_alignment" in co_route
                    else None
                ),
                seq=len(co_route["stops"]),
            )
            addCircularMetadataToRoute(route_obj, co, co_route)
            whole_route_list.append(route_obj)


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
            if "stops_and_alignment" in route_list[found]:
                route_i.setdefault("stops_and_alignment", {}).update(
                    route_list[found]["stops_and_alignment"]
                )
            if "circular_return_point" in route_list[found]:
                route_i.setdefault("circular_return_point", {}).update(
                    route_list[found]["circular_return_point"]
                )
            if "circular_sections" in route_list[found]:
                route_i.setdefault("circular_sections", {}).update(
                    route_list[found]["circular_sections"]
                )
            route_list[found]["skip"] = True

        # append return array
        _routeList.append(route_i)

    return _routeList


def get_stop_alignment_csv_value(value):
    if value is None:
        return "/"
    if isinstance(value, (int, float)):
        if value == 0:
            return "0"
        return f"{value:.1f}"
    return value


def get_stop_alignment_csv_row(cos, gtfs_stop=None):
    row = {"gtfs": get_stop_alignment_csv_value(gtfs_stop)}
    for co in cos:
        row[co] = "/"
        row[f"{co}_d"] = "/"
    return row


def compressStopAlignment(stop_alignment, cos):
    fieldnames = ["gtfs"]
    for co in cos:
        if co in stop_alignment:
            fieldnames.extend([co, f"{co}_d"])

    rows = []
    rows_by_gtfs = {}
    for co in cos:
        if co not in stop_alignment:
            continue
        gtfs_occurrences = {}
        for alignment in stop_alignment[co]:
            gtfs_stop = alignment["gtfs_stop"]
            if gtfs_stop is None:
                row = get_stop_alignment_csv_row(cos)
                rows.append(row)
            else:
                occurrence = gtfs_occurrences.get(gtfs_stop, 0)
                gtfs_occurrences[gtfs_stop] = occurrence + 1
                gtfs_rows = rows_by_gtfs.setdefault(gtfs_stop, [])
                if occurrence < len(gtfs_rows):
                    row = gtfs_rows[occurrence]
                else:
                    row = get_stop_alignment_csv_row(cos, gtfs_stop)
                    rows.append(row)
                    gtfs_rows.append(row)

            row[co] = get_stop_alignment_csv_value(alignment["co_stop"])
            row[f"{co}_d"] = get_stop_alignment_csv_value(alignment["distance"])

    csv_file = io.StringIO()
    writer = csv.DictWriter(
        csv_file, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(rows)
    return csv_file.getvalue()


def compressRouteStopAlignments(route_list):
    for route in route_list:
        if "stops_and_alignment" in route:
            route["stops_and_alignment"] = compressStopAlignment(
                route["stops_and_alignment"], route["co"]
            )


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
    # TODO: low priority, align sequence of all operators and GTFS together, currently they are aligned separately
    # extra stop from one operator will append row individually
    # if 3 operators have the same extra stop, their will be 3 rows appended
    compressRouteStopAlignments(routeList)

    holidays = loadJson(DATA_DIR / "holiday.json")
    serviceDayMap = loadJson(DATA_DIR / "gtfs.json")["serviceDayMap"]

    db = standardizeDict(
        {
            "routeList": {getRouteId(v): v for v in routeList},
            "stopList": stopList,
            # TODO: simply set it is empty dict
            "stopMap": stopMap,
            "holidays": holidays,
            "serviceDayMap": serviceDayMap,
        }
    )

    writeJson(
        DATA_DIR / "routeFareList.mergeRoutes.min.json", db, separators=(",", ":")
    )


if __name__ == "__main__":
    main()
