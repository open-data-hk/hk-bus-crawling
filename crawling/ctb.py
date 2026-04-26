import asyncio
import json
import logging
from os import path
from typing import Literal

import httpx

from .crawl_utils import emitRequest, get_request_limit
from .utils import DATA_DIR

logger = logging.getLogger(__name__)

COMPANY_CODE = "ctb"

# define output name
ROUTE_LIST = DATA_DIR / ("routeList." + COMPANY_CODE + ".json")
STOP_LIST = DATA_DIR / ("stopList." + COMPANY_CODE + ".json")

BASE_URL = "https://rt.data.gov.hk/v2/transport/citybus"


def routes_url(co: str = "ctb"):
    return BASE_URL + "/route/" + co


def stop_url(stopId: str):
    return BASE_URL + "/stop/" + stopId


def route_stop_url(
    route: str, direction: Literal["inbound", "outbound"], co: str = "ctb"
):
    return BASE_URL + "/route-stop/" + co + "/" + route + "/" + direction


req_route_stop_limit = asyncio.Semaphore(get_request_limit())
req_stop_list_limit = asyncio.Semaphore(get_request_limit())

# methods of single API request


async def get_route_list(co, a_client) -> list[dict]:
    logger.info(f"Fetching route list of {COMPANY_CODE}")
    r = await emitRequest(routes_url(co), a_client)
    return r.json()["data"]


async def get_stop(stopId, a_client) -> dict:
    async with req_stop_list_limit:
        r = await emitRequest(stop_url(stopId), a_client)
    return r.json()["data"]


async def get_route_stop(co: str, route: dict, a_client) -> dict:
    if route.get("bound", 0) != 0 or route.get("stops", {}):
        return route
    route["stops"] = {}
    for direction in ["inbound", "outbound"]:
        r = await emitRequest(
            route_stop_url(route["route"], direction, co),
            a_client,
        )
        route["stops"][direction] = [stop["stop"] for stop in r.json()["data"]]
    return route


# methods of multiple API requests


async def get_stop_list(stops, a_client) -> list[dict]:
    logger.info(f"Fetching stop list of {COMPANY_CODE}")
    ret = await asyncio.gather(*[get_stop(stop, a_client) for stop in stops])
    return ret


async def get_route_stop_list(co: str, route_list: list[dict], a_client) -> list[dict]:
    logger.info(f"Fetching route stop list of {COMPANY_CODE}")
    ret = await asyncio.gather(
        *[get_route_stop(co, route, a_client) for route in route_list]
    )
    return ret


async def getRouteStop(co):
    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))

    # load route list and stop list if exist
    route_list: list[dict] = []
    # TODO: check why return if route_list exists
    if path.isfile(ROUTE_LIST):
        logger.info(f"{ROUTE_LIST} already exist, skipping...")
        return
    else:
        # load routes
        route_list = await get_route_list(co, a_client)

    _stop_ids = []
    stop_list = {}
    if path.isfile(STOP_LIST):
        with open(STOP_LIST, "r", encoding="UTF-8") as f:
            stop_list = json.load(f)

    route_list = await get_route_stop_list(co, route_list, a_client)

    logger.info(f"Preparing data of {COMPANY_CODE}")

    for route in route_list:
        for direction, stops in route["stops"].items():
            for stopId in stops:
                _stop_ids.append(stopId)

    # load stops for this route aync
    _stop_ids = sorted(set(_stop_ids))

    stopInfos = list(zip(_stop_ids, await get_stop_list(_stop_ids, a_client)))
    for stopId, stopInfo in stopInfos:
        stop_list[stopId] = stopInfo

    _routeList = []
    for route in route_list:
        if route.get("bound", 0) != 0:
            _routeList.append(route)
            continue
        for bound in ["inbound", "outbound"]:
            if len(route["stops"][bound]) > 0:
                _routeList.append(
                    {
                        "co": co,
                        "route": route["route"],
                        "bound": "O" if bound == "outbound" else "I",
                        "orig_en": (
                            route["orig_en"]
                            if bound == "outbound"
                            else route["dest_en"]
                        ),
                        "orig_tc": (
                            route["orig_tc"]
                            if bound == "outbound"
                            else route["dest_tc"]
                        ),
                        "dest_en": (
                            route["dest_en"]
                            if bound == "outbound"
                            else route["orig_en"]
                        ),
                        "dest_tc": (
                            route["dest_tc"]
                            if bound == "outbound"
                            else route["orig_tc"]
                        ),
                        "stops": list(
                            filter(
                                lambda stopId: bool(stop_list[stopId]),
                                route["stops"][bound],
                            )
                        ),
                        "serviceType": 0,
                    }
                )

    with open(ROUTE_LIST, "w", encoding="UTF-8") as f:
        f.write(json.dumps(_routeList, ensure_ascii=False))
    with open(STOP_LIST, "w", encoding="UTF-8") as f:
        f.write(json.dumps(stop_list, ensure_ascii=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(getRouteStop("ctb"))
