# -*- coding: utf-8 -*-
# Check route latest update time

import asyncio
import csv
import io
import json
import logging
import os
import re
import time

import httpx
import xxhash

try:
    from .constants import GH_PAGE_DOMAIN
    from .crawl_utils import emitRequest
    from .utils import DATA_DIR
except ImportError:
    from constants import GH_PAGE_DOMAIN
    from crawl_utils import emitRequest
    from utils import DATA_DIR


def load_split_db(data_dir):
    with open(data_dir / "integrated_routes.json", encoding="UTF-8") as f:
        route_list = json.load(f)
    with open(data_dir / "operators_stops.json", encoding="UTF-8") as f:
        stop_list = json.load(f)
    with open(data_dir / "operators_routes.json", encoding="UTF-8") as f:
        operator_routes = json.load(f)
    return {
        "routeList": route_list,
        "stopList": stop_list,
        "operatorRoutes": operator_routes,
    }


def get_route_operator_stops(db, route):
    if "stops" in route:
        return route["stops"].values()

    operator_stops = []
    for route_key in route.get("operator_routes", []):
        operator_route = db["operatorRoutes"][route_key]
        if "stops" in operator_route:
            operator_stops.append(operator_route["stops"])
            continue

        co = route_key.split("|", 1)[0]
        stop_alignment = operator_route.get("stop_alignment")
        if isinstance(stop_alignment, str):
            operator_stops.append(
                [
                    row[co]
                    for row in csv.DictReader(io.StringIO(stop_alignment))
                    if row.get(co) not in (None, "", "/")
                ]
            )
    return operator_stops


async def routeCompare():
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None)) as a_client:
        oldDb = {
            "routeList": (
                await emitRequest(
                    f"{GH_PAGE_DOMAIN}/integrated_routes.json",
                    a_client,
                )
            ).json(),
            "stopList": (
                await emitRequest(
                    f"{GH_PAGE_DOMAIN}/operators_stops.json",
                    a_client,
                )
            ).json(),
            "operatorRoutes": (
                await emitRequest(
                    f"{GH_PAGE_DOMAIN}/operators_routes.json",
                    a_client,
                )
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
        for operator_stops in get_route_operator_stops(
            newDb, newDb["routeList"][newKey]
        ):
            busStopsinRoute.update(operator_stops)
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
