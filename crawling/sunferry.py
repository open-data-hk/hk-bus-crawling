# -*- coding: utf-8 -*-

import json

from crawl_utils import dump_provider_data
from schemas import ProviderRoute, ProviderStop
from utils import DATA_DIR


def main():
    if (DATA_DIR / "routeList.sunferry.json").exists() and (
        DATA_DIR / "stopList.sunferry.json"
    ).exists():
        return

    with open(DATA_DIR / "gtfs.json", "r", encoding="utf-8") as f:
        gtfs = json.load(f)
        gtfsRoutes = gtfs["routeList"]
        gtfsStops = gtfs["stopList"]

    routes = {
        "CECC": ["Central", "Cheung Chau"],
        "CCCE": ["Cheung Chau", "Central"],
        "CEMW": ["Central", "Mui Wo"],
        "MWCE": ["Mui Wo", "Central"],
        "NPHH": ["North Point", "Hung Hom"],
        "HHNP": ["Hung Hom", "North Point"],
        "NPKC": ["North Point", "Kowloon City"],
        "KCNP": ["Kowloon City", "North Point"],
        "IIPECMUW": ["Peng Chau", "Mui Wo"],
        "IIMUWPEC": ["Mui Wo", "Peng Chau"],
        "IIMUWCMW": ["Mui Wo", "Chi Ma Wan"],
        "IICMWMUW": ["Chi Ma Wan", "Mui Wo"],
        "IICMWCHC": ["Chi Ma Wan", "Cheung Chau"],
        "IICHCCMW": ["Cheung Chau", "Chi Ma Wan"],
        "IICHCMUW": ["Cheung Chau", "Mui Wo"],
        "IIMUWCHC": ["Mui Wo", "Cheung Chau "],
    }

    co = "sunferry"
    routeList: list[ProviderRoute] = []
    stopList: dict[str, ProviderStop] = {}

    for [route_code, [orig, dest]] in routes.items():
        for route_id, gtfsRoute in gtfsRoutes.items():
            if "ferry" in gtfsRoute["co"]:
                if (
                    orig.lower() == gtfsRoute["orig"]["en"].lower()
                    and dest.lower() == gtfsRoute["dest"]["en"].lower()
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
                elif (
                    dest.lower() == gtfsRoute["orig"]["en"].lower()
                    and orig.lower() == gtfsRoute["dest"]["en"].lower()
                ):
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
