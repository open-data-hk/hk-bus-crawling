# -*- coding: utf-8 -*-
# Check route latest update time

import asyncio
import json
import logging
import os
import re
import time

import httpx
import xxhash
from crawl_utils import emitRequest
from utils import DATA_DIR


def load_split_db(data_dir):
    with open(data_dir / "integrated_routes.json", encoding="UTF-8") as f:
        route_list = json.load(f)
    with open(data_dir / "operators_stops.json", encoding="UTF-8") as f:
        stop_list = json.load(f)
    return {
        "routeList": route_list,
        "stopList": stop_list,
    }


async def routeCompare():
    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))
    oldDb = {
        "routeList": (
            await emitRequest("https://data.hkbus.app/integrated_routes.json", a_client)
        ).json(),
        "stopList": (
            await emitRequest("https://data.hkbus.app/operators_stops.json", a_client)
        ).json(),
    }
    newDb = load_split_db(DATA_DIR)
    changedStops = set()

    os.makedirs(DATA_DIR / "route-ts", exist_ok=True)

    def isRouteEqual(a, b):
        return xxhash.xxh3_64(str(a)).hexdigest() == xxhash.xxh3_64(str(b)).hexdigest()

    for newStop in newDb["stopList"]:
        if newStop not in oldDb["stopList"] or not isRouteEqual(
            oldDb["stopList"][newStop], newDb["stopList"][newStop]
        ):
            changedStops.add(newStop)

    for oldStop in oldDb["stopList"]:
        if oldStop not in newDb["stopList"]:
            changedStops.add(oldStop)

    for newKey in newDb["routeList"]:
        busStopsinRoute = set()
        for provider in newDb["routeList"][newKey]["stops"]:
            busStopsinRoute.update(newDb["routeList"][newKey]["stops"][provider])
        if (
            newKey not in oldDb["routeList"]
            or bool(changedStops & busStopsinRoute)
            or not isRouteEqual(oldDb["routeList"][newKey], newDb["routeList"][newKey])
        ):
            filename = re.sub(r"[\\\/\:\*\?\"\<\>\|]", "", newKey).upper()
            with open(DATA_DIR / "route-ts" / filename, "w", encoding="utf-8") as f:
                f.write(str(int(time.time())))

    for oldKey in oldDb["routeList"]:
        if oldKey not in newDb["routeList"]:
            filename = re.sub(r"[\\\/\:\*\?\"\<\>\|]", "", oldKey).upper()
            with open(DATA_DIR / "route-ts" / filename, "w", encoding="utf-8") as f:
                f.write(str(int(time.time())))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    asyncio.run(routeCompare())
