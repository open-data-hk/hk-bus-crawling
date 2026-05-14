import asyncio
import logging
from typing import Literal

from crawl_utils import emitRequest, get_request_limit
from utils import DATA_DIR

logger = logging.getLogger(__name__)

# Raw list files
RAW_ROUTE_LIST = DATA_DIR / ("ctb.raw.routeList.json")
RAW_ROUTE_STOP_LIST = DATA_DIR / ("ctb.raw.routeStopList.json")
RAW_STOP_LIST = DATA_DIR / ("ctb.raw.stopList.json")


BASE_URL = "https://rt.data.gov.hk/v2/transport/citybus"


def routes_url():
    return BASE_URL + "/route/ctb"


def stop_url(stopId: str):
    return BASE_URL + "/stop/" + stopId


def route_stop_url(route: str, direction: Literal["inbound", "outbound"]):
    return BASE_URL + "/route-stop/ctb/" + route + "/" + direction


req_route_stop_limit = asyncio.Semaphore(get_request_limit())
req_stop_list_limit = asyncio.Semaphore(get_request_limit())

# methods of single API request


async def get_route_list(a_client) -> list[dict]:
    logger.info("Fetching route list of ctb")
    r = await emitRequest(routes_url(), a_client)
    return r.json()["data"]


async def get_stop(stopId, a_client) -> dict:
    async with req_stop_list_limit:
        r = await emitRequest(stop_url(stopId), a_client)
    return r.json()["data"]


async def get_route_stop(route: str, a_client) -> dict[str, list[dict]]:
    # TODO: remove this commented code if found useless
    # if route.get("bound", 0) != 0 or route.get("stops", {}):
    #     return route

    route_stops = {}
    for direction in ["inbound", "outbound"]:
        r = await emitRequest(
            route_stop_url(route, direction),
            a_client,
        )
        result = r.json()["data"]
        route_key = f"{route}-{direction}"

        route_stops[route_key] = result
    return route_stops


# methods of multiple API requests


async def get_stop_list(stops, a_client) -> list[dict]:
    logger.info("Fetching stop list of ctb")
    ret = await asyncio.gather(*[get_stop(stop, a_client) for stop in stops])
    return ret


async def get_route_stop_list(route_list: list[dict], a_client) -> dict[str, list]:
    logger.info("Fetching route stop list of ctb")
    route_stop_list = await asyncio.gather(
        *[get_route_stop(route["route"], a_client) for route in route_list]
    )

    route_stops = {}
    for single_route_stops in route_stop_list:
        for route_key, route_stop in single_route_stops.items():
            route_stops[route_key] = route_stop

    return route_stops
