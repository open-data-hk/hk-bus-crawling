# -*- coding: utf-8 -*-
# MTR Bus fetching

import asyncio
import csv
import json
import logging

import httpx
from crawl_utils import emitRequest
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
    stopList = {}

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
        start = {"zh": chn.split("至")[0], "en": eng.split(" to ")[0]}
        end = {"zh": chn.split("至")[1], "en": eng.split(" to ")[1]}
        for bound in ["I", "O"]:
            routeList[referenceId + "_" + bound] = {
                "route": route,
                "bound": bound,
                "service_type": serviceType,
                "orig_tc": start["zh"] if bound == "O" else end["zh"],
                "dest_tc": end["zh"] if bound == "O" else start["zh"],
                "orig_en": start["en"] if bound == "O" else end["en"],
                "dest_en": end["en"] if bound == "O" else start["en"],
                "stops": [],
                "co": "lrtfeeder",
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
            "lat": lat,
            "long": lng,
        }

    with open(DATA_DIR / "routeList.lrtfeeder.json", "w", encoding="UTF-8") as f:
        f.write(
            json.dumps(
                [route for route in routeList.values() if len(route["stops"]) > 0],
                ensure_ascii=False,
            )
        )
    with open(DATA_DIR / "stopList.lrtfeeder.json", "w", encoding="UTF-8") as f:
        f.write(json.dumps(stopList, ensure_ascii=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    asyncio.run(getRouteStop())
