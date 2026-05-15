import argparse
import asyncio
import json
import logging
from pathlib import Path

import httpx

try:
    from .crawl_utils import emitRequest, get_request_limit
    from .utils import DATA_DIR
except ImportError:
    from crawl_utils import emitRequest, get_request_limit
    from utils import DATA_DIR

logger = logging.getLogger(__name__)

# Raw list files
RAW_ROUTE_LIST = DATA_DIR / "nlb.raw.routeList.json"
RAW_ROUTE_STOP_LIST = DATA_DIR / "nlb.raw.routeStopList.json"
RAW_STOP_LIST = DATA_DIR / "nlb.raw.stopList.json"

BASE_URL = "https://rt.data.gov.hk/v2/transport/nlb"

ROUTE_STOP_ONLY_FIELDS = {"fare", "fareHoliday", "someDepartureObserveOnly"}


def routes_url():
    return BASE_URL + "/route.php?action=list"


def route_stop_url(route_id: str):
    return BASE_URL + "/stop.php?action=list&routeId=" + route_id


req_route_stop_limit = asyncio.Semaphore(get_request_limit())


async def get_route_list(a_client) -> list[dict]:
    logger.info("Fetching route list of nlb")
    r = await emitRequest(routes_url(), a_client)
    return r.json()["routes"]


async def get_route_stop(route_id: str, a_client) -> list[dict]:
    async with req_route_stop_limit:
        r = await emitRequest(route_stop_url(route_id), a_client)
    return r.json()["stops"]


async def get_route_stop_list(route_list: list[dict], a_client) -> dict[str, list]:
    logger.info("Fetching route stop list of nlb")
    route_stop_items = await asyncio.gather(
        *[get_route_stop(str(route["routeId"]), a_client) for route in route_list]
    )
    return {
        str(route["routeId"]): route_stop
        for route, route_stop in zip(route_list, route_stop_items)
    }


def get_stop_from_route_stop(route_stop: dict) -> dict:
    return {
        key: value
        for key, value in route_stop.items()
        if key not in ROUTE_STOP_ONLY_FIELDS
    }


def get_stop_list(route_stop_list: dict[str, list]) -> dict[str, dict]:
    stop_list = {}
    for route_stops in route_stop_list.values():
        for route_stop in route_stops:
            stop_id = str(route_stop["stopId"])
            stop = get_stop_from_route_stop(route_stop)
            if stop_id in stop_list and stop_list[stop_id] != stop:
                logger.warning(f"NLB stop {stop_id} has inconsistent stop data")
            stop_list[stop_id] = stop
    return stop_list


async def prepare_raw_data(force: bool = False):
    async with httpx.AsyncClient() as a_client:
        raw_route_list_path = Path(RAW_ROUTE_LIST)
        if raw_route_list_path.exists() and not force:
            logger.info(f"{RAW_ROUTE_LIST} already exists, loading...")
            route_list = json.loads(raw_route_list_path.read_text("utf-8"))
        else:
            route_list = await get_route_list(a_client)
            raw_route_list_path.write_text(
                json.dumps(route_list, ensure_ascii=False), encoding="UTF-8"
            )

        raw_route_stop_list_path = Path(RAW_ROUTE_STOP_LIST)
        if raw_route_stop_list_path.exists() and not force:
            logger.info(f"{RAW_ROUTE_STOP_LIST} already exists, loading...")
            route_stop_list = json.loads(raw_route_stop_list_path.read_text("utf-8"))
        else:
            route_stop_list = await get_route_stop_list(route_list, a_client)
            raw_route_stop_list_path.write_text(
                json.dumps(route_stop_list, ensure_ascii=False), encoding="UTF-8"
            )

        raw_stop_list_path = Path(RAW_STOP_LIST)
        if raw_stop_list_path.exists() and not force:
            logger.info(f"{RAW_STOP_LIST} already exists, skipping...")
            return

        stop_list = get_stop_list(route_stop_list)
        raw_stop_list_path.write_text(
            json.dumps(stop_list, ensure_ascii=False), encoding="UTF-8"
        )


def parse_args():
    parser = argparse.ArgumentParser(description="Crawl raw NLB data files")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Fetch raw data again even when local raw files already exist",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    args = parse_args()
    asyncio.run(prepare_raw_data(force=args.force))
