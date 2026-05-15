# -*- coding: utf-8 -*-

import json

from crawl_utils import dump_provider_data
from schemas import ProviderRoute, ProviderStop
from utils import DATA_DIR


def main():
    if (DATA_DIR / "routeList.fortuneferry.json").exists() and (
        DATA_DIR / "stopList.fortuneferry.json"
    ).exists():
        return

    with open(DATA_DIR / "gtfs.json", "r", encoding="UTF-8") as f:
        gtfs = json.load(f)
        gtfsRoutes = gtfs["routeList"]
        gtfsStops = gtfs["stopList"]

    routes = {
        "7059": ["中環", "紅磡"],
        "7021": ["北角", "啟德"],
        "7056": ["北角", "觀塘"],
        "7025": ["屯門", "大澳"],
        "7000004": ["東涌", "大澳"],
    }

    co = "fortuneferry"
    routeList: list[ProviderRoute] = []
    stopList: dict[str, ProviderStop] = {}

    for [route_code, [orig, dest]] in routes.items():
        for route_id, gtfsRoute in gtfsRoutes.items():
            if "ferry" in gtfsRoute["co"]:
                if (
                    orig.lower() == gtfsRoute["orig"]["tc"].lower()
                    and dest.lower() == gtfsRoute["dest"]["tc"].lower()
                ):
                    routeList.append(
                        {
                            "co": co,
                            "gtfs_route_id": route_id,
                            "gtfs_route_seq": "1",
                            "route": route_code,
                            "orig_tc": gtfsRoute["orig"]["tc"],
                            "orig_sc": gtfsRoute["orig"]["sc"],
                            "orig_en": gtfsRoute["orig"]["en"],
                            "dest_tc": gtfsRoute["dest"]["tc"],
                            "dest_sc": gtfsRoute["dest"]["sc"],
                            "dest_en": gtfsRoute["dest"]["en"],
                            "service_type": 1,
                            "bound": "O",
                            "stops": gtfsRoute["stops"]["1"],
                            "freq": gtfsRoute["freq"]["1"],
                        }
                    )
                    if "2" in gtfsRoute["freq"]:
                        routeList.append(
                            {
                                "co": co,
                                "gtfs_route_id": route_id,
                                "gtfs_route_seq": "2",
                                "route": route_code,
                                "dest_tc": gtfsRoute["orig"]["tc"],
                                "dest_sc": gtfsRoute["orig"]["sc"],
                                "dest_en": gtfsRoute["orig"]["en"],
                                "orig_tc": gtfsRoute["dest"]["tc"],
                                "orig_sc": gtfsRoute["dest"]["sc"],
                                "orig_en": gtfsRoute["dest"]["en"],
                                "service_type": 1,
                                "bound": "I",
                                "stops": (
                                    gtfsRoute["stops"]["2"]
                                    if "2" in gtfsRoute["stops"]
                                    else gtfsRoute["stops"]["1"][::-1]
                                ),
                                "freq": (
                                    gtfsRoute["freq"]["2"]
                                    if "2" in gtfsRoute["freq"]
                                    else {}
                                ),
                            }
                        )

    for route in routeList:
        for stopId in route["stops"]:
            stopList[stopId] = {
                "stop": stopId,
                "name_en": gtfsStops[stopId]["stopName"]["ferry"]["en"],
                "name_tc": gtfsStops[stopId]["stopName"]["ferry"]["tc"],
                "name_sc": gtfsStops[stopId]["stopName"]["ferry"]["sc"],
                "lat": gtfsStops[stopId]["lat"],
                "lng": gtfsStops[stopId]["lng"],
            }

    dump_provider_data(co, routeList, stopList)


if __name__ == "__main__":
    main()
