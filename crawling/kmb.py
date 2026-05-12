import asyncio
import copy
import json
import logging
import sys
from os import path

import httpx
from crawl_utils import dump_provider_data, emitRequest
from schemas import ProviderRoute, ProviderStop
from utils import DATA_DIR

BASE_URL = "https://data.etabus.gov.hk/v1/transport/kmb"


def stops_url():
    return BASE_URL + "/stop"


def routes_url():
    return BASE_URL + "/route/"


def route_stops_url():
    return BASE_URL + "/route-stop/"


async def getRouteStop():
    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))
    # define output name
    ROUTE_LIST = DATA_DIR / "routeList.kmb.json"
    STOP_LIST = DATA_DIR / "stopList.kmb.json"

    if path.isfile(ROUTE_LIST):
        return

    stopList: dict[str, ProviderStop] = {}
    if path.isfile(STOP_LIST):
        with open(STOP_LIST, "r", encoding="UTF-8") as f:
            stopList = json.load(f)
    else:
        # load stops
        r = await emitRequest(stops_url(), a_client)
        _stopList = r.json()["data"]
        for stop in _stopList:
            stopList[stop["stop"]] = stop
            stop["lng"] = stop.pop("long")

    def isStopExist(stopId):
        if stopId not in stopList:
            print("Not exist stop: ", stopId, file=sys.stderr)
        return stopId in stopList

    # load route list
    routeList = {}
    # load routes
    r = await emitRequest(routes_url(), a_client)
    for route in r.json()["data"]:
        route["stops"] = {}
        route["co"] = "kmb"
        routeList["+".join([route["route"], route["service_type"], route["bound"]])] = (
            route
        )

    # load route stops
    r = await emitRequest(route_stops_url(), a_client)
    for stop in r.json()["data"]:
        routeKey = "+".join([stop["route"], stop["service_type"], stop["bound"]])
        if routeKey in routeList:
            routeList[routeKey]["stops"][int(stop["seq"])] = stop["stop"]
        else:
            # if route not found, clone it from service type = 1
            _routeKey = "+".join([stop["route"], str("1"), stop["bound"]])
            routeList[routeKey] = copy.deepcopy(routeList[_routeKey])
            routeList[routeKey]["stops"] = {}
            routeList[routeKey]["stops"][int(stop["seq"])] = stop["stop"]

    # flatten the route stops back to array
    for routeKey in routeList.keys():
        stops = [
            routeList[routeKey]["stops"][seq]
            for seq in sorted(routeList[routeKey]["stops"].keys())
        ]
        # filter non-exist stops
        stops = list(filter(isStopExist, stops))
        routeList[routeKey]["stops"] = stops

    # flatten the routeList back to array
    routeList: list[ProviderRoute] = [
        routeList[routeKey]
        for routeKey in routeList.keys()
        if not routeKey.startswith("K")
    ]

    dump_provider_data("kmb", routeList, stopList)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(getRouteStop())
