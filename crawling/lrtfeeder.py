# -*- coding: utf-8 -*-
# MTR Bus fetching

import asyncio
import csv
import logging

import httpx
from crawl_utils import dump_provider_data, emitRequest
from schemas import ProviderRoute, ProviderStop
from utils import DATA_DIR

BASE_URL = "https://opendata.mtr.com.hk/data"


def routes_url():
    return BASE_URL + "/mtr_bus_routes.csv"


def stops_url():
    return BASE_URL + "/mtr_bus_stops.csv"


async def getRouteStop(co="lrtfeeder"):
    if (DATA_DIR / f"routeList.{co}.json").exists() and (
        DATA_DIR / f"stopList.{co}.json"
    ).exists():
        return
    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))
    routeList = {}
    stopList: dict[str, ProviderStop] = {}

    r = await emitRequest(routes_url(), a_client)
    r.encoding = "utf-8"
    reader = csv.reader(r.text.split("\n"))
    headers = next(reader, None)
    routes = [route for route in reader if len(route) == 7]
    for [route, chn, eng, isCircular, lineUp, lineDown, referenceId] in routes:
        if route == "":
            continue
        serviceType = (
            "1"
            if len(referenceId.split("-")) == 1
            else ((int)(referenceId.split("-")[1]) + 1)
        )
        start = {
            "tc": chn.split("至")[0],
            "sc": chn.split("至")[0],
            "en": eng.split(" to ")[0],
        }
        end = {
            "tc": chn.split("至")[1],
            "sc": chn.split("至")[1],
            "en": eng.split(" to ")[1],
        }
        for bound in ["I", "O"]:
            routeList[referenceId + "_" + bound] = {
                "route": route,
                "bound": bound,
                "service_type": serviceType,
                "orig_tc": start["tc"] if bound == "O" else end["tc"],
                "orig_sc": start["sc"] if bound == "O" else end["sc"],
                "dest_tc": end["tc"] if bound == "O" else start["tc"],
                "dest_sc": end["sc"] if bound == "O" else start["sc"],
                "orig_en": start["en"] if bound == "O" else end["en"],
                "dest_en": end["en"] if bound == "O" else start["en"],
                "stops": [],
                "co": co,
            }

    # Parse stops
    r = await emitRequest(stops_url(), a_client)
    r.encoding = "utf-8"
    reader = csv.reader(r.text.split("\n"))
    headers = next(reader, None)
    stops = [stop for stop in reader if len(stop) >= 8]
    for [
        route,
        bound,
        seq,
        stationId,
        lat,
        lng,
        name_zh,
        name_en,
        referenceId,
    ] in stops:
        routeKey = referenceId + "_" + bound
        if routeKey in routeList:
            routeList[routeKey]["stops"].append(stationId)
        else:
            print("error", routeKey)
        stopList[stationId] = {
            "stop": stationId,
            "name_en": name_en,
            "name_tc": name_zh,
            "name_sc": name_zh,
            "lat": lat,
            "lng": lng,
        }

    routes: list[ProviderRoute] = [
        route for route in routeList.values() if len(route["stops"]) > 0
    ]
    dump_provider_data(co, routes, stopList)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    asyncio.run(getRouteStop())
