# -*- coding: utf-8 -*-
import argparse
import asyncio
import datetime
import json
import logging
from typing import Literal
from zoneinfo import ZoneInfo

import httpx

try:
    from .crawl_utils import emitRequest, get_request_limit
    from .utils import DATA_DIR
except ImportError:
    from crawl_utils import emitRequest, get_request_limit
    from utils import DATA_DIR

logger = logging.getLogger(__name__)

# Raw list files
RAW_ROUTE_NO_LIST = DATA_DIR / ("gmb.raw.routeNoList.json")
RAW_ROUTE_LIST = DATA_DIR / ("gmb.raw.routeList.json")
RAW_ROUTE_STOP_LIST = DATA_DIR / ("gmb.raw.routeStopList.json")
RAW_STOP_LIST = DATA_DIR / ("gmb.raw.stopList.json")

BASE_URL = "https://data.etagmb.gov.hk"
REGIONS = ["HKI", "KLN", "NT"]


def route_stop_url(route_id, route_seq):
    return BASE_URL + "/route-stop/" + str(route_id) + "/" + str(route_seq)


def region_routes_url(region: str):
    return BASE_URL + "/route/" + region


def route_url_by_region_route_no(region: str, route_no: str):
    return BASE_URL + "/route/" + region + "/" + route_no


def route_url_by_route_id(route_id: int):
    return BASE_URL + "/route/" + str(route_id)


def stop_url(stop_id):
    return BASE_URL + "/stop/" + str(stop_id)


def last_update_url(data_type: Literal["route", "stop", "route-stop"]) -> str:
    return BASE_URL + "/last-update/" + data_type


req_route_region_limit = asyncio.Semaphore(get_request_limit())
req_route_limit = asyncio.Semaphore(get_request_limit())
req_route_stops_limit = asyncio.Semaphore(get_request_limit())
req_stops_limit = asyncio.Semaphore(get_request_limit())


async def get_routes_region(region: str, route_nos_by_region: dict, a_client) -> None:
    async with req_route_region_limit:
        r = await emitRequest(region_routes_url(region), a_client)
        route_nos_by_region[region] = r.json()["data"]


async def get_route_by_region_route_no(
    region: str, route_no: str, all_routes: dict, a_client
) -> None:
    async with req_route_limit:
        r = await emitRequest(route_url_by_region_route_no(region, route_no), a_client)
        routes = r.json()["data"]
        for route in routes:
            all_routes[str(route["route_id"])] = route


async def get_route_by_id(route_id: int, all_routes: dict, a_client) -> None:
    async with req_route_limit:
        r = await emitRequest(route_url_by_route_id(route_id), a_client)
        result = r.json()["data"]
        all_routes[str(route_id)] = result[0]


async def get_route_stops(
    route_id: int, route_seq: int, all_route_stops: dict, a_client
) -> None:
    async with req_route_stops_limit:
        r = await emitRequest(
            route_stop_url(route_id, route_seq),
            a_client,
        )
    result = r.json()["data"]
    key = f"{route_id}-{route_seq}"
    all_route_stops[key] = result


async def get_stop(stop_id: int, stops_fetch: dict, a_client) -> None:
    async with req_stops_limit:
        r = await emitRequest(stop_url(stop_id), a_client)
        result = r.json()["data"]
        stops_fetch[str(stop_id)] = result


async def get_last_update(
    data_type: Literal["route", "stop", "route-stop"], a_client
) -> None:
    async with req_stops_limit:
        r = await emitRequest(last_update_url(data_type), a_client)
        return r.json()["data"]


async def prepare_route_no_list(a_client, force: bool) -> dict[str, dict]:
    if RAW_ROUTE_NO_LIST.exists() and not force:
        logger.info(f"{RAW_ROUTE_NO_LIST} already exists, loading...")
        return json.loads(RAW_ROUTE_NO_LIST.read_text("utf-8"))

    route_nos_by_region: dict[str, dict] = {}
    await asyncio.gather(
        *[
            get_routes_region(region, route_nos_by_region, a_client)
            for region in REGIONS
        ]
    )
    RAW_ROUTE_NO_LIST.write_text(
        json.dumps(route_nos_by_region, ensure_ascii=False), encoding="UTF-8"
    )
    return route_nos_by_region


async def prepare_route_list(a_client, route_nos_by_region: dict, force: bool):
    all_region_route_no = [
        (region, route_no)
        for region, region_data in route_nos_by_region.items()
        for route_no in region_data["routes"]
    ]

    all_routes: dict[str, dict] = {}

    if RAW_ROUTE_LIST.exists() and not force:
        logger.info(f"{RAW_ROUTE_LIST} already exists, loading...")
        all_routes = json.loads(RAW_ROUTE_LIST.read_text("utf-8"))

        # handle deprecated all_routes format
        # routes key by "{region}-{route_no}", e.g. KLN-82 : list of routes
        first_key = list(all_routes.keys())[0]
        if first_key[0].isalpha():
            all_routes = {
                str(route["route_id"]): route
                for routes in all_routes.values()
                for route in routes
            }

        local_last_update = {
            str(route_id): route["data_timestamp"]
            for route_id, route in all_routes.items()
        }

        remote_record = await get_last_update("route", a_client)
        remote_last_update = {
            str(route["route_id"]): route["last_update_date"] for route in remote_record
        }

        local_route_ids = set(local_last_update.keys())
        remote_route_ids = set(remote_last_update.keys())

        delete_route_ids = local_route_ids - remote_route_ids
        create_route_ids = remote_route_ids - local_route_ids
        common_route_ids = local_route_ids & remote_route_ids
        fetch_route_ids = create_route_ids

        for route_id in delete_route_ids:
            del all_routes[route_id]

        for route_id in common_route_ids:
            local_ts = datetime.datetime.fromisoformat(local_last_update[route_id])
            remote_ts = datetime.datetime.fromisoformat(
                remote_last_update[route_id]
            ).replace(tzinfo=ZoneInfo("Asia/Hong_Kong"))
            if remote_ts > local_ts:
                fetch_route_ids.add(route_id)

        if fetch_route_ids:
            logger.info(f"Fetching routes of ids: {fetch_route_ids}")
            await asyncio.gather(
                *[
                    get_route_by_id(route_id, all_routes, a_client)
                    for route_id in fetch_route_ids
                ]
            )
    else:
        await asyncio.gather(
            *[
                get_route_by_region_route_no(region, route, all_routes, a_client)
                for region, route in all_region_route_no
            ]
        )

    RAW_ROUTE_LIST.write_text(
        json.dumps(all_routes, ensure_ascii=False), encoding="UTF-8"
    )
    return all_routes


async def prepare_route_stop_list(a_client, all_routes: dict, force: bool):
    all_route_stops = {}
    if RAW_ROUTE_STOP_LIST.exists() and not force:
        logger.info(f"{RAW_ROUTE_STOP_LIST} already exists, loading...")
        all_route_stops = json.loads(RAW_ROUTE_STOP_LIST.read_text("utf-8"))

        local_last_update = {
            key: route_stops["data_timestamp"]
            for key, route_stops in all_route_stops.items()
        }

        remote_record = await get_last_update("route-stop", a_client)
        remote_last_update = {
            f"{entry['route_id']}-{entry['route_seq']}": entry["last_update_date"]
            for entry in remote_record
        }

        local_keys = set(local_last_update.keys())
        remote_keys = set(remote_last_update.keys())
        required_keys = {
            f"{route_id}-{direction['route_seq']}"
            for route_id, route in all_routes.items()
            for direction in route["directions"]
        }

        delete_keys = local_keys - remote_keys
        fetch_keys = required_keys - local_keys

        for key in delete_keys:
            del all_route_stops[key]

        for key in local_keys & remote_keys:
            local_ts = datetime.datetime.fromisoformat(local_last_update[key])
            remote_ts = datetime.datetime.fromisoformat(
                remote_last_update[key]
            ).replace(tzinfo=ZoneInfo("Asia/Hong_Kong"))
            if remote_ts > local_ts:
                fetch_keys.add(key)

        if fetch_keys:
            logger.info(f"Fetching route stops of keys: {fetch_keys}")
            fetch_directions = [
                (route_id, int(route_seq))
                for key in fetch_keys
                for route_id, route_seq in [key.split("-")]
            ]
            await asyncio.gather(
                *[
                    get_route_stops(route_id, route_seq, all_route_stops, a_client)
                    for route_id, route_seq in fetch_directions
                ]
            )
    else:
        all_route_directions = [
            (route_id, direction["route_seq"])
            for route_id, route in all_routes.items()
            for direction in route["directions"]
        ]
        await asyncio.gather(
            *[
                get_route_stops(route_id, route_seq, all_route_stops, a_client)
                for route_id, route_seq in all_route_directions
            ]
        )

    RAW_ROUTE_STOP_LIST.write_text(
        json.dumps(all_route_stops, ensure_ascii=False), encoding="UTF-8"
    )
    return all_route_stops


async def prepare_stop_list(a_client, force: bool):
    remote_record = await get_last_update("stop", a_client)

    all_stops = {}
    if RAW_STOP_LIST.exists() and not force:
        logger.info(f"{RAW_STOP_LIST} already exists, loading...")
        all_stops = json.loads(RAW_STOP_LIST.read_text("utf-8"))

        local_last_update = {
            stop_id: stop["data_timestamp"] for stop_id, stop in all_stops.items()
        }

        remote_last_update = {
            str(entry["stop_id"]): entry["last_update_date"] for entry in remote_record
        }

        local_keys = set(local_last_update.keys())
        remote_keys = set(remote_last_update.keys())

        delete_keys = local_keys - remote_keys
        fetch_keys = remote_keys - local_keys

        for stop_id in delete_keys:
            del all_stops[stop_id]

        for stop_id in local_keys & remote_keys:
            local_ts = datetime.datetime.fromisoformat(local_last_update[stop_id])
            remote_ts = datetime.datetime.fromisoformat(
                remote_last_update[stop_id]
            ).replace(tzinfo=ZoneInfo("Asia/Hong_Kong"))
            if remote_ts > local_ts:
                fetch_keys.add(stop_id)

        if fetch_keys:
            logger.info(f"Fetching stops of ids: {fetch_keys}")
            await asyncio.gather(
                *[get_stop(stop_id, all_stops, a_client) for stop_id in fetch_keys]
            )
    else:
        await asyncio.gather(
            *[get_stop(stop["stop_id"], all_stops, a_client) for stop in remote_record]
        )

    RAW_STOP_LIST.write_text(
        json.dumps(all_stops, ensure_ascii=False), encoding="UTF-8"
    )
    return all_stops


async def prepare_raw_data(force: bool = False):
    async with httpx.AsyncClient() as a_client:
        route_nos_by_region = await prepare_route_no_list(a_client, force)
        all_routes = await prepare_route_list(a_client, route_nos_by_region, force)
        await prepare_route_stop_list(a_client, all_routes, force)
        await prepare_stop_list(a_client, force)


def parse_args():
    parser = argparse.ArgumentParser(description="Crawl raw GMB data files")
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
