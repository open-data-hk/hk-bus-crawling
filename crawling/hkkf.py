# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import time

import httpx
from crawl_utils import emitRequest
from utils import DATA_DIR

BASE_URL = "https://www.hkkfeta.com/opendata"


def routes_url():
    return BASE_URL + "/route/"


def pier_url(stopId):
    return BASE_URL + "/pier/" + str(stopId)


routes = {
    "1": ["Central Pier 4", "Sok Kwu Wan"],
    "2": ["Central Pier 4", "Yung Shue Wan"],
    "3": ["Central Pier 6", "Peng Chau"],
    "4": ["Peng Chau", "Hei Ling Chau"],
}


def parseStop(name_en, apiStops):
    for stop in apiStops:
        if stop["name_en"].startswith(name_en):
            return stop
    raise Exception("Undefined stop")


async def getRouteStop(co):
    if (DATA_DIR / f"routeList.{co}.json").exists() and (
        DATA_DIR / f"stopList.{co}.json"
    ).exists():
        return
    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))
    routeList = []
    stopList = {}

    r = await emitRequest(routes_url(), a_client)
    apiRoutes = r.json()["data"]
    apiStops = []
    for stopId in [1, 2, 3, 4, 5, 6]:
        stop = (await emitRequest(pier_url(stopId), a_client)).json()["data"]
        apiStops.append(stop)

    with open(DATA_DIR / "gtfs.json", "r", encoding="utf-8") as f:
        gtfsZh = json.load(f)

    with open(DATA_DIR / "gtfs-en.json", "r", encoding="utf-8") as f:
        gtfs = json.load(f)
        gtfsRoutes = gtfs["routeList"]
        gtfsStops = gtfs["stopList"]

    for apiRoute in apiRoutes:
        orig = parseStop(routes[str(apiRoute["route_id"])][0], apiStops)
        dest = parseStop(routes[str(apiRoute["route_id"])][1], apiStops)
        routeList.append(
            {
                "route": "KF" + str(apiRoute["route_id"]),
                "orig_tc": orig["name_tc"],
                "orig_en": orig["name_en"],
                "dest_tc": dest["name_tc"],
                "dest_en": dest["name_en"],
                "service_type": 1,
                "bound": "O",
                "stops": [
                    "KF" + str(orig["pier_id"]),
                    "KF" + str(dest["pier_id"]),
                ],
                "co": "hkkf",
            }
        )
        routeList.append(
            {
                "route": "KF" + str(apiRoute["route_id"]),
                "orig_tc": dest["name_tc"],
                "orig_en": dest["name_en"],
                "dest_tc": orig["name_tc"],
                "dest_en": orig["name_en"],
                "service_type": 1,
                "bound": "I",
                "stops": [
                    "KF" + str(dest["pier_id"]),
                    "KF" + str(orig["pier_id"]),
                ],
                "co": "hkkf",
            }
        )

    for apiStop in apiStops:
        stopList["KF" + str(apiStop["pier_id"])] = {
            "stop": "KF" + str(apiStop["pier_id"]),
            "name_en": apiStop["name_en"],
            "name_tc": apiStop["name_tc"],
            "lat": apiStop["lat"],
            "long": apiStop["long"],
        }

    with open(DATA_DIR / "routeList.hkkf.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(routeList, ensure_ascii=False))

    with open(DATA_DIR / "stopList.hkkf.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(stopList, ensure_ascii=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(getRouteStop("hkkf"))
