import asyncio
import json
import logging
from pathlib import Path

import httpx
from crawl_utils import dump_provider_data
from schemas import ProviderRoute, ProviderStop
from utils import DATA_DIR

logger = logging.getLogger(__name__)

# Raw list files
RAW_ROUTE_LIST = DATA_DIR / ("ctb.raw.routeList.json")
RAW_ROUTE_STOP_LIST = DATA_DIR / ("ctb.raw.routeStopList.json")
RAW_STOP_LIST = DATA_DIR / ("ctb.raw.stopList.json")

# define output name
ROUTE_LIST = DATA_DIR / "routeList.ctb.json"
STOP_LIST = DATA_DIR / "stopList.ctb.json"


async def prepare_data():

    stopInfos = list(zip(_stop_ids, stop_list_raw))
    for stopId, stopInfo in stopInfos:
        stop_list[stopId] = stopInfo
        stopInfo["lng"] = stopInfo.pop("long")

    _routeList: list[ProviderRoute] = []
    for route in route_list:
        if route.get("bound", 0) != 0:
            _routeList.append(route)
            continue
        for bound in ["inbound", "outbound"]:
            if len(route["stops"][bound]) > 0:
                _routeList.append(
                    {
                        "co": "ctb",
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
                        "orig_sc": (
                            route["orig_sc"]
                            if bound == "outbound"
                            else route["dest_sc"]
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
                        "dest_sc": (
                            route["dest_sc"]
                            if bound == "outbound"
                            else route["orig_sc"]
                        ),
                        "stops": list(
                            filter(
                                lambda stopId: bool(stop_list[stopId]),
                                route["stops"][bound],
                            )
                        ),
                        "service_type": 0,
                    }
                )

    dump_provider_data("ctb", _routeList, stop_list)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(prepare_data())
