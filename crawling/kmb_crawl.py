import argparse
import asyncio
import json
import logging
from pathlib import Path

import httpx

try:
    from .crawl_utils import emitRequest
    from .utils import DATA_DIR
except ImportError:
    from crawl_utils import emitRequest
    from utils import DATA_DIR

logger = logging.getLogger(__name__)

# Raw list files
RAW_ROUTE_LIST = DATA_DIR / "kmb.raw.routeList.json"
RAW_ROUTE_STOP_LIST = DATA_DIR / "kmb.raw.routeStopList.json"
RAW_STOP_LIST = DATA_DIR / "kmb.raw.stopList.json"

BASE_URL = "https://data.etabus.gov.hk/v1/transport/kmb"


def stops_url():
    return BASE_URL + "/stop"


def routes_url():
    return BASE_URL + "/route/"


def route_stops_url():
    return BASE_URL + "/route-stop/"


async def get_stop_list(a_client) -> list[dict]:
    logger.info("Fetching stop list of kmb")
    r = await emitRequest(stops_url(), a_client)
    return r.json()["data"]


async def get_route_list(a_client) -> list[dict]:
    logger.info("Fetching route list of kmb")
    r = await emitRequest(routes_url(), a_client)
    return r.json()["data"]


async def get_route_stop_list(a_client) -> list[dict]:
    logger.info("Fetching route stop list of kmb")
    r = await emitRequest(route_stops_url(), a_client)
    return r.json()["data"]


async def prepare_raw_data(force: bool = False):
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None)) as a_client:
        raw_stop_list_path = Path(RAW_STOP_LIST)
        if raw_stop_list_path.exists() and not force:
            logger.info(f"{RAW_STOP_LIST} already exists, skipping...")
        else:
            stop_list = await get_stop_list(a_client)
            raw_stop_list_path.write_text(
                json.dumps(stop_list, ensure_ascii=False), encoding="UTF-8"
            )

        raw_route_list_path = Path(RAW_ROUTE_LIST)
        if raw_route_list_path.exists() and not force:
            logger.info(f"{RAW_ROUTE_LIST} already exists, skipping...")
        else:
            route_list = await get_route_list(a_client)
            raw_route_list_path.write_text(
                json.dumps(route_list, ensure_ascii=False), encoding="UTF-8"
            )

        raw_route_stop_list_path = Path(RAW_ROUTE_STOP_LIST)
        if raw_route_stop_list_path.exists() and not force:
            logger.info(f"{RAW_ROUTE_STOP_LIST} already exists, skipping...")
        else:
            route_stop_list = await get_route_stop_list(a_client)
            raw_route_stop_list_path.write_text(
                json.dumps(route_stop_list, ensure_ascii=False), encoding="UTF-8"
            )


def parse_args():
    parser = argparse.ArgumentParser(description="Crawl raw KMB data files")
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
