import asyncio
import json
import logging
from pathlib import Path

try:
    from .crawl_utils import dump_provider_data
    from .ctb_crawl import (
        RAW_ROUTE_LIST,
        RAW_ROUTE_STOP_LIST,
        RAW_STOP_LIST,
    )
    from .schemas import ProviderRoute, ProviderStop
    from .utils import DATA_DIR
except ImportError:
    from crawl_utils import dump_provider_data
    from ctb_crawl import (
        RAW_ROUTE_LIST,
        RAW_ROUTE_STOP_LIST,
        RAW_STOP_LIST,
    )
    from schemas import ProviderRoute, ProviderStop
    from utils import DATA_DIR

logger = logging.getLogger(__name__)

# define output name
ROUTE_LIST = DATA_DIR / "routeList.ctb.json"
STOP_LIST = DATA_DIR / "stopList.ctb.json"


def load_raw_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(
            f"{path} does not exist. Run crawling/ctb_crawl.py first."
        )
    return json.loads(path.read_text("utf-8"))


def ensure_raw_files_exist():
    missing_files = [
        path
        for path in [RAW_ROUTE_LIST, RAW_ROUTE_STOP_LIST, RAW_STOP_LIST]
        if not path.exists()
    ]
    if missing_files:
        missing_file_list = ", ".join(str(path) for path in missing_files)
        raise FileNotFoundError(
            f"Missing CTB raw file(s): {missing_file_list}. "
            "Run crawling/ctb_crawl.py first."
        )


def build_route_stop_ids(
    route_list: list[dict],
    route_stop_list: dict[str, list[dict]],
) -> list[str]:
    _stop_ids = []
    for route in route_list:
        route["stops"] = {}
        for direction in ["inbound", "outbound"]:
            route_code = f"{route['route']}-{direction}"
            route_stop = route_stop_list[route_code]
            route["stops"][direction] = [stop["stop"] for stop in route_stop]
            _stop_ids.extend(route["stops"][direction])

    return sorted(set(_stop_ids))


async def prepare_data():
    ensure_raw_files_exist()

    if ROUTE_LIST.exists():
        logger.info(f"{ROUTE_LIST} already exist, skipping...")
        return

    route_list = load_raw_json(RAW_ROUTE_LIST)
    route_stop_list = load_raw_json(RAW_ROUTE_STOP_LIST)
    stop_list_raw = load_raw_json(RAW_STOP_LIST)

    logger.info("Preparing data of ctb")

    _stop_ids = build_route_stop_ids(route_list, route_stop_list)
    stop_list: dict[str, ProviderStop] = {}
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
