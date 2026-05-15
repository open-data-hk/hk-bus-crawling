import asyncio
import copy
import json
import logging
from pathlib import Path

try:
    from .crawl_utils import dump_provider_data
    from .kmb_crawl import (
        RAW_ROUTE_LIST,
        RAW_ROUTE_STOP_LIST,
        RAW_STOP_LIST,
    )
    from .schemas import ProviderRoute, ProviderStop
except ImportError:
    from crawl_utils import dump_provider_data
    from kmb_crawl import (
        RAW_ROUTE_LIST,
        RAW_ROUTE_STOP_LIST,
        RAW_STOP_LIST,
    )
    from schemas import ProviderRoute, ProviderStop

logger = logging.getLogger(__name__)


def load_raw_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(
            f"{path} does not exist. Run crawling/kmb_crawl.py first."
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
            f"Missing KMB raw file(s): {missing_file_list}. "
            "Run crawling/kmb_crawl.py first."
        )


def get_stop_list(raw_stop_list: list[dict]) -> dict[str, ProviderStop]:
    stop_list = {}
    for stop in raw_stop_list:
        stop = {**stop}
        stop["lng"] = stop.pop("long")
        stop_list[stop["stop"]] = stop
    return stop_list


def route_key(route: str, service_type: str, bound: str) -> str:
    return "+".join([route, service_type, bound])


def get_route_list(
    raw_route_list: list[dict],
    raw_route_stop_list: list[dict],
    stop_list: dict[str, ProviderStop],
) -> list[ProviderRoute]:
    route_map = {}
    for route in raw_route_list:
        route = {**route, "stops": {}, "co": "kmb"}
        route_map[route_key(route["route"], route["service_type"], route["bound"])] = (
            route
        )

    for stop in raw_route_stop_list:
        key = route_key(stop["route"], stop["service_type"], stop["bound"])
        if key not in route_map:
            base_key = route_key(stop["route"], "1", stop["bound"])
            route_map[key] = copy.deepcopy(route_map[base_key])
            route_map[key]["stops"] = {}
        route_map[key]["stops"][int(stop["seq"])] = stop["stop"]

    for key, route in route_map.items():
        stops = [route["stops"][seq] for seq in sorted(route["stops"].keys())]
        route["stops"] = [
            stop_id for stop_id in stops if is_stop_exist(stop_id, stop_list)
        ]

    return [route_map[key] for key in route_map.keys() if not key.startswith("K")]


def is_stop_exist(stop_id: str, stop_list: dict[str, ProviderStop]) -> bool:
    if stop_id not in stop_list:
        logger.warning(f"Not exist stop: {stop_id}")
    return stop_id in stop_list


async def getRouteStop():
    ensure_raw_files_exist()

    raw_stop_list = load_raw_json(RAW_STOP_LIST)
    raw_route_list = load_raw_json(RAW_ROUTE_LIST)
    raw_route_stop_list = load_raw_json(RAW_ROUTE_STOP_LIST)

    logger.info("Preparing data of kmb")

    stop_list = get_stop_list(raw_stop_list)
    route_list = get_route_list(raw_route_list, raw_route_stop_list, stop_list)

    dump_provider_data("kmb", route_list, stop_list)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(getRouteStop())
