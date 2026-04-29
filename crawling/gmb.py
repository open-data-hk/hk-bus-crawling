# -*- coding: utf-8 -*-
import asyncio
import csv
import datetime
import json
import logging
import time
from typing import Any, Literal
from zoneinfo import ZoneInfo

import httpx
from crawl_utils import emitRequest, get_request_limit
from utils import DATA_DIR

logger = logging.getLogger(__name__)

# Raw list files
RAW_ROUTE_NO_LIST = DATA_DIR / ("gmb.raw.routeNoList.json")
RAW_ROUTE_LIST = DATA_DIR / ("gmb.raw.routeList.json")
RAW_ROUTE_STOP_LIST = DATA_DIR / ("gmb.raw.routeStopList.json")
RAW_STOP_LIST = DATA_DIR / ("gmb.raw.stopList.json")

BASE_URL = "https://data.etagmb.gov.hk"


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


# Single API request
req_route_region_limit = asyncio.Semaphore(get_request_limit())


async def get_routes_region(region: str, route_nos_by_region: dict, a_client) -> None:
    async with req_route_region_limit:
        r = await emitRequest(region_routes_url(region), a_client)
        route_nos_by_region[region] = r.json()["data"]


req_route_limit = asyncio.Semaphore(get_request_limit())


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


req_route_stops_limit = asyncio.Semaphore(get_request_limit())


async def get_route_stops(
    route_id: int, route_seq: int, all_route_stops: dict, a_client
) -> None:
    """
    Provided route's `route_id` and `route_seq`, request route_stop
    and store result in `all_route_stops`
    """
    async with req_route_stops_limit:
        r = await emitRequest(
            route_stop_url(route_id, route_seq),
            a_client,
        )
    result = r.json()["data"]
    # if not result["route_stops"]:
    #     # don't save if empty route_stops
    #     return
    key = f"{route_id}-{route_seq}"
    all_route_stops[key] = result


req_stops_limit = asyncio.Semaphore(get_request_limit())


async def get_stop(stop_id: int, stops_fetch: dict, a_client) -> None:
    async with req_stops_limit:
        r = await emitRequest(stop_url(stop_id), a_client)
        result = r.json()["data"]
        stops_fetch[stop_id] = result


async def get_last_update(
    data_type: Literal["route", "stop", "route-stop"], a_client
) -> None:
    async with req_stops_limit:
        r = await emitRequest(last_update_url(data_type), a_client)
        return r.json()["data"]


async def getRouteStop(co):
    if (DATA_DIR / f"routeList.{co}.json").exists() and (
        DATA_DIR / f"stopList.{co}.json"
    ).exists():
        return
    a_client = httpx.AsyncClient()
    # parse gtfs service_id
    serviceIdMap = {}
    with open(DATA_DIR / "gtfs-tc/calendar.txt", "r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader, None)
        for [service_id, mon, tue, wed, thur, fri, sat, sun, *tmp] in reader:
            serviceIdMap[service_id] = [
                mon == "1",
                tue == "1",
                wed == "1",
                thur == "1",
                fri == "1",
                sat == "1",
                sun == "1",
            ]
        serviceIdMap["111"] = [True, True, False, True, True, True, True]

    def mapServiceId(weekdays, serviceIdMap_a):
        for service_id in serviceIdMap_a:
            if all(i == j for i, j in zip(serviceIdMap_a[service_id], weekdays)):
                return service_id
        return 999
        # raise Exception("No service ID for weekdays: "+json.dumps(weekdays))

    def getFreq(headways, serviceIdMap_a):
        freq = {}
        for headway in headways:
            service_id = mapServiceId(headway["weekdays"], serviceIdMap_a)
            if service_id not in freq:
                freq[service_id] = {}
            freq[service_id][headway["start_time"].replace(":", "")[:4]] = (
                [
                    headway["end_time"].replace(":", "")[:4],
                    str(headway["frequency"] * 60),
                ]
                if headway["frequency"] is not None
                else None
            )
        return freq

    routeList = []
    stops = {}

    stopCandidates = {}

    def process_route_directions(route, route_no, all_route_stops):
        service_type = 2
        for direction in route["directions"]:
            key = f'{route["route_id"]}-{direction["route_seq"]}'
            route_stops = all_route_stops[key]["route_stops"]
            for stop in route_stops:
                stop_id = stop["stop_id"]

                # GMB ETA API Spec: "A stop may have different names under different routes"
                # While hk-bus-crawling only allows one name per stop
                # Try to strategically and deterministically pick a stop name
                oldNameEn = (
                    stops[str(stop_id)]["name_en"] if str(stop_id) in stops else ""
                )
                oldNameTc = (
                    stops[str(stop_id)]["name_tc"] if str(stop_id) in stops else ""
                )
                newNameEn = stop["name_en"].strip()
                newNameTc = stop["name_tc"].strip()
                useNameEn = oldNameEn
                useNameTc = oldNameTc
                toReplace = False

                # Prefer longer Chinese names. They are usually more specific
                # e.g. "常安街, 柴灣消防局對面" over "常安街77號"
                # e.g. "小西灣道, 香港學術及職業資歷評審局外" over "小西灣道, 近曉翠街"
                # e.g. "暢運道, 近國際都會都會大廈" over "暢運道, 近紅磡站", "紅磡站, 暢運道"
                # Note: "柴灣道, 筲箕灣官立中學外" over "柴灣道, 筲箕灣官立中學"
                # Note: "大坑東道, 大坑東遊樂場外" over "大坑東道,大坑東遊樂場外"
                # Note: "亞皆老街113號, 太平道" over "亞皆老街, 嘉麗園", "亞皆老街, 近嘉麗園", "亞皆老街113號"
                # Note: "貿業路, 寶琳港鐵站外" "Po Lam Station" over "貿業路, 近寶琳站" "Mau Yip Road,
                # near Po Lam Station"
                if len(newNameTc) > len(oldNameTc):
                    toReplace = True
                elif len(newNameTc) == len(oldNameTc):
                    if newNameTc > oldNameTc:
                        toReplace = True
                    elif newNameTc == oldNameTc:
                        # Prefer English names with more words
                        if len(newNameEn.split()) > len(oldNameEn.split()):
                            toReplace = True
                        elif len(newNameEn.split()) == len(oldNameEn.split()):
                            if len(newNameEn) > len(oldNameEn):
                                toReplace = True
                            elif len(newNameEn) == len(oldNameEn):
                                if newNameEn > oldNameEn:
                                    toReplace = True
                if toReplace:
                    useNameTc = newNameTc
                    useNameEn = newNameEn

                if oldNameEn.upper() == newNameEn.upper():
                    # Prefer fewer uppercase letters
                    # e.g. "Pok Fu Lam Road" over "POK FU LAM ROAD"
                    # e.g. "Tsing Yi Heung Sze Wui Road, Near Greenfield Garden Block 3"
                    # over "TSING YI HEUNG SZE WUI ROAD, near Greenfield Garden Block 3"
                    useNameEn = (
                        newNameEn
                        if sum(1 for c in newNameEn if c.isupper())
                        < sum(1 for c in oldNameEn if c.isupper())
                        else oldNameEn
                    )

                if str(stop_id) not in stopCandidates:
                    stopCandidates[str(stop_id)] = {
                        "en_used": "",
                        "en_others": set(),
                        "tc_used": "",
                        "tc_others": set(),
                    }
                stopCandidates[str(stop_id)]["en_used"] = useNameEn
                stopCandidates[str(stop_id)]["en_others"].add(newNameEn)
                stopCandidates[str(stop_id)]["tc_used"] = useNameTc
                stopCandidates[str(stop_id)]["tc_others"].add(newNameTc)

                stops[str(stop_id)] = {
                    "stop": str(stop_id),
                    "name_en": useNameEn,
                    "name_tc": useNameTc,
                }
            routeList.append(
                {
                    "gtfsId": str(route["route_id"]),
                    "route": route_no,
                    "orig_tc": direction["orig_tc"],
                    "orig_en": direction["orig_en"],
                    "dest_tc": direction["dest_tc"],
                    "dest_en": direction["dest_en"],
                    "bound": "O" if direction["route_seq"] == 1 else "I",
                    "service_type": (
                        1
                        if route["description_tc"].strip() == "正常班次"
                        else service_type
                    ),
                    "stops": [str(stop["stop_id"]) for stop in route_stops],
                    "freq": getFreq(direction["headways"], serviceIdMap),
                }
            )
            # print(routeList)
            if route["description_tc"].strip() != "正常班次":
                service_type += 1

    REGIONS = ["HKI", "KLN", "NT"]

    route_nos_by_region: dict[str, dict] = {}

    if RAW_ROUTE_NO_LIST.exists():
        logger.info(f"{RAW_ROUTE_NO_LIST} already exists, loading...")
        route_nos_by_region = json.loads(RAW_ROUTE_NO_LIST.read_text("utf-8"))
    else:
        await asyncio.gather(
            *[
                get_routes_region(region, route_nos_by_region, a_client)
                for region in REGIONS
            ]
        )
        RAW_ROUTE_NO_LIST.write_text(
            json.dumps(route_nos_by_region, ensure_ascii=False), encoding="UTF-8"
        )

    all_region_route_no = [
        (region, route_no)
        for region, region_data in route_nos_by_region.items()
        for route_no in region_data["routes"]
    ]

    # routes key by route_id, e.g. 2001558 (str)
    all_routes: dict[str, dict] = {}

    if RAW_ROUTE_LIST.exists():
        logger.info(f"{RAW_ROUTE_LIST} already exists, loading...")
        all_routes = json.loads(RAW_ROUTE_LIST.read_text("utf-8"))

        # handle deprecate all_routes format
        # routes key by "{region}-{route_no}", e.g. KLN-82 : list of routes
        first_key = list(all_routes.keys())[0]
        if first_key[0].isalpha():
            # flatten to key by route_id
            all_routes = {
                route["route_id"]: route
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
            # timezone in last-update endpoint is +0, must set +8 before comparison
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

    # key: {route_id}-{route_seq}, e.g. 2006408-1
    if RAW_ROUTE_STOP_LIST.exists():
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
        # remote_key does not imply all are running (i.e. contain outdate route stops)
        # empty route_stops found for some keys
        # use all_routes instead
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
        all_route_stops = {}
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

    for route_id, route in all_routes.items():
        process_route_directions(route, route["route_code"], all_route_stops)

    routeList.sort(key=lambda a: a["gtfsId"])

    with open(DATA_DIR / f"routeList.{co}.json", "w", encoding="UTF-8") as f:
        json.dump(routeList, f, ensure_ascii=False)
    logger.info("Route done")

    with open(DATA_DIR / "gtfs.json", "r", encoding="UTF-8") as f:
        gtfs = json.load(f)
        gtfsStops = gtfs["stopList"]

    # this block only has context of API
    # ignore any other requirements, e.g. gtfsStops

    # must fetch in any case
    remote_record = await get_last_update("stop", a_client)

    if RAW_STOP_LIST.exists():
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
            # for stop_id in gtfsStops_missing_stop_ids & local_keys & remote_keys:
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
        all_stops = {}
        await asyncio.gather(
            *[get_stop(stop["stop_id"], all_stops, a_client) for stop in remote_record]
        )
    RAW_STOP_LIST.write_text(
        json.dumps(all_stops, ensure_ascii=False), encoding="UTF-8"
    )

    stop_ids_need_fetch = set()
    for stop_id in stops.keys():
        if (stop_id not in gtfsStops) and (stop_id not in all_stops):
            stop_ids_need_fetch.add(stop_id)

    additional_stops_fetch = {}
    if stop_ids_need_fetch:
        logger.info(f"Fetching missing stop ids: {stop_ids_need_fetch}")
        await asyncio.gather(
            *[
                get_stop(stop_id, additional_stops_fetch, a_client)
                for stop_id in stop_ids_need_fetch
            ]
        )

    all_stops = {**all_stops, **additional_stops_fetch}

    for stop_id, _ in stops.items():
        if stop_id not in gtfsStops:
            stops[stop_id]["lat"] = all_stops[stop_id]["coordinates"]["wgs84"][
                "latitude"
            ]
            stops[stop_id]["long"] = all_stops[stop_id]["coordinates"]["wgs84"][
                "longitude"
            ]
        else:
            stops[stop_id]["lat"] = gtfsStops[stop_id]["lat"]
            stops[stop_id]["long"] = gtfsStops[stop_id]["lng"]

    with open(DATA_DIR / f"stopList.{co}.json", "w", encoding="UTF-8") as f:
        json.dump(stops, f, ensure_ascii=False)
    for stop in stopCandidates:
        stopCandidates[stop]["tc_others"].discard(stopCandidates[stop]["tc_used"])
        stopCandidates[stop]["tc_others"] = sorted(stopCandidates[stop]["tc_others"])
        stopCandidates[stop]["en_others"].discard(stopCandidates[stop]["en_used"])
        stopCandidates[stop]["en_others"] = sorted(stopCandidates[stop]["en_others"])
    with open(DATA_DIR / f"stopCandidates.{co}.json", "w", encoding="UTF-8") as f:

        def set_default(obj):
            if isinstance(obj, set):
                return list(obj)
            raise TypeError

        json.dump(stopCandidates, f, ensure_ascii=False, default=set_default)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(getRouteStop("gmb"))
