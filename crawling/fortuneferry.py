# -*- coding: utf-8 -*-

import json

from utils import DATA_DIR


def main():
    if (DATA_DIR / "routeList.fortuneferry.json").exists() and (
        DATA_DIR / "stopList.fortuneferry.json"
    ).exists():
        return

    with open(DATA_DIR / "gtfs-tc.json", "r", encoding="UTF-8") as f:
        gtfs = json.load(f)
        gtfsRoutes = gtfs["routeList"]
        gtfsStops = gtfs["stopList"]

    with open(DATA_DIR / "gtfs-en.json", "r", encoding="UTF-8") as f:
        gtfsEn = json.load(f)

    routes = {
        "7059": ["中環", "紅磡"],
        "7021": ["北角", "啟德"],
        "7056": ["北角", "觀塘"],
        "7025": ["屯門", "大澳"],
        "7000004": ["東涌", "大澳"],
    }

    routeList = []
    stopList = {}

    for [route_code, [orig, dest]] in routes.items():
        for route_id, gtfsRoute in gtfsRoutes.items():
            if "ferry" in gtfsRoute["co"]:
                if (
                    orig.lower() == gtfsRoute["orig"]["zh"].lower()
                    and dest.lower() == gtfsRoute["dest"]["zh"].lower()
                ):
                    routeList.append(
                        {
                            "gtfsId": route_id,
                            "route": route_code,
                            "orig_tc": gtfsRoute["orig"]["zh"],
                            "orig_en": gtfsEn["routeList"][route_id]["orig"]["en"],
                            "dest_tc": gtfsRoute["dest"]["zh"],
                            "dest_en": gtfsEn["routeList"][route_id]["dest"]["en"],
                            "service_type": 1,
                            "bound": "O",
                            "stops": gtfsRoute["stops"]["1"],
                            "freq": gtfsRoute["freq"]["1"],
                        }
                    )
                    if "2" in gtfsRoute["freq"]:
                        routeList.append(
                            {
                                "gtfsId": route_id,
                                "route": route_code,
                                "dest_tc": gtfsRoute["orig"]["zh"],
                                "dest_en": gtfsEn["routeList"][route_id]["orig"]["en"],
                                "orig_tc": gtfsRoute["dest"]["zh"],
                                "orig_en": gtfsEn["routeList"][route_id]["dest"]["en"],
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
                "name_en": gtfsEn["stopList"][stopId]["stopName"]["unknown"],
                "name_tc": gtfsStops[stopId]["stopName"]["unknown"],
                "lat": gtfsStops[stopId]["lat"],
                "long": gtfsStops[stopId]["lng"],
            }

    with open(DATA_DIR / "routeList.fortuneferry.json", "w", encoding="UTF-8") as f:
        f.write(json.dumps(routeList, ensure_ascii=False))

    with open(DATA_DIR / "stopList.fortuneferry.json", "w", encoding="UTF-8") as f:
        f.write(json.dumps(stopList, ensure_ascii=False))


if __name__ == "__main__":
    main()
