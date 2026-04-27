import asyncio
import csv
import datetime
import json
import logging
import re
import sys
import zipfile
from os import path
from typing import Literal
from zoneinfo import ZoneInfo

import httpx
from crawl_utils import emitRequest, store_version
from utils import DATA_DIR

LANG_CONFIG = {
    "tc": {
        "url": "https://static.data.gov.hk/td/pt-headway-tc/gtfs.zip",
        # TODO: append "-tc"
        "zip": "gtfs.zip",
        "dir": "gtfs",
        "version_key": "GTFS",
        # TODO: change to "tc"
        "lang_key": "zh",
        "output": "gtfs.json",
    },
    "en": {
        "url": "https://static.data.gov.hk/td/pt-headway-en/gtfs.zip",
        "zip": "gtfs-en.zip",
        "dir": "gtfs-en",
        "version_key": "GTFS-EN",
        "lang_key": "en",
        "output": "gtfs-en.json",
    },
    "sc": {
        "url": "https://static.data.gov.hk/td/pt-headway-sc/gtfs.zip",
        "zip": "gtfs-sc.zip",
        "dir": "gtfs-sc",
        "version_key": "GTFS-SC",
        "lang_key": "sc",
        "output": "gtfs-sc.json",
    },
}


def takeFirst(elem):
    return int(elem[0])


def orig_dest(
    route_long_name: str, lang: Literal["tc", "sc", "en"]
) -> tuple[dict, dict]:
    orig_langs = {
        "zh": "",
        "en": "",
        # TODO: add sc
        # "sc": "",
    }
    dest_langs = {
        "zh": "",
        "en": "",
        # "sc": "",
    }

    # TODO: inspect if more than 1 hyphen
    name_split = route_long_name.split(" - ")
    orig = name_split[0]
    dest = name_split[1]

    if lang == "tc":
        # TODO: tc version should not contains English?
        dest = dest.replace(" (CIRCULAR)", "")

        orig_langs["zh"] = orig
        dest_langs["zh"] = dest
    elif lang == "sc":
        # follow tc atm, TODO: use sc data
        # TODO: tc version should not contains English?
        dest = dest.replace(" (CIRCULAR)", "")

        orig_langs["sc"] = orig
        dest_langs["sc"] = dest
    elif lang == "en":
        dest = dest.replace(" (CIRCULAR)", "")

        orig_langs["en"] = orig
        dest_langs["en"] = dest

    return orig_langs, dest_langs


def route_no(
    route_short_name: str, route_id: str, lang: Literal["tc", "sc", "en"]
) -> str:
    if lang == "tc":
        return route_short_name
    elif lang == "sc":
        # follow tc behaviour
        # TODO: use sc
        return route_short_name
    elif lang == "en":
        return route_short_name if route_short_name != "" else route_id


async def parseGtfs(lang="tc"):
    cfg = LANG_CONFIG[lang]
    # TODO: remove lang_key if useless
    lang_key = cfg["lang_key"]
    gtfs_dir = DATA_DIR / cfg["dir"]

    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))
    if not path.isfile(DATA_DIR / cfg["zip"]):
        r = await emitRequest(cfg["url"], a_client)
        open(DATA_DIR / cfg["zip"], "wb").write(r.content)

    with zipfile.ZipFile(DATA_DIR / cfg["zip"], "r") as zip_ref:
        zip_ref.extractall(gtfs_dir)
        version = min([f.date_time for f in zip_ref.infolist()])
        version = datetime.datetime(*version, tzinfo=ZoneInfo("Asia/Hong_Kong"))
        store_version(cfg["version_key"], version.isoformat())

    routeList = {}
    stopList = {}
    serviceDayMap = {}
    routeJourneyTime = json.load(
        open(DATA_DIR / "routeTime.json", "r", encoding="UTF-8")
    )

    with open(gtfs_dir / "routes.txt", "r", encoding="UTF-8") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader, None)
        for [
            route_id,
            agency_id,
            route_short_name,
            route_long_name,
            route_type,
            route_url,
        ] in reader:
            orig_langs, dest_langs = orig_dest(route_long_name, lang)
            routeList[route_id] = {
                "co": agency_id.replace("LWB", "KMB").lower().split("+"),
                "route": route_no(route_short_name, route_id, lang),
                "stops": {},
                "fares": {},
                "freq": {},
                "orig": orig_langs,
                "dest": dest_langs,
                "jt": (
                    routeJourneyTime[route_id]["journeyTime"]
                    if route_id in routeJourneyTime
                    else None
                ),
            }

    # parse timetable
    with open(gtfs_dir / "trips.txt", "r", encoding="UTF-8") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader, None)
        for [route_id, service_id, trip_id] in reader:
            [route_id, bound, calendar, start_time] = trip_id.split("-")
            if bound not in routeList[route_id]["freq"]:
                routeList[route_id]["freq"][bound] = {}
            if calendar not in routeList[route_id]["freq"][bound]:
                routeList[route_id]["freq"][bound][calendar] = {}
            if start_time not in routeList[route_id]["freq"][bound][calendar]:
                routeList[route_id]["freq"][bound][calendar][start_time] = None

    with open(gtfs_dir / "frequencies.txt", "r", encoding="UTF-8") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader, None)
        for [trip_id, _start_time, end_time, headway_secs] in reader:
            [route_id, bound, calendar, start_time] = trip_id.split("-")
            routeList[route_id]["freq"][bound][calendar][start_time] = (
                end_time[0:5].replace(":", ""),
                headway_secs,
            )

    # parse stop seq
    with open(gtfs_dir / "stop_times.txt", "r", encoding="UTF-8") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader, None)
        for [
            trip_id,
            arrival_time,
            departure_time,
            stop_id,
            stop_sequence,
            pickup_type,
            drop_off_type,
            timepoint,
        ] in reader:
            [route_id, bound, service_id, tmp] = trip_id.split("-")
            if bound not in routeList[route_id]["stops"]:
                routeList[route_id]["stops"][bound] = {}
            routeList[route_id]["stops"][bound][stop_sequence] = stop_id

    # parse fares
    with open(gtfs_dir / "fare_attributes.txt", "r", encoding="UTF-8") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader, None)
        for [
            fare_id,
            price,
            currency_type,
            payment_method,
            transfers,
            agency_id,
        ] in reader:
            [route_id, bound, on, off] = fare_id.split("-")
            if bound not in routeList[route_id]["fares"]:
                routeList[route_id]["fares"][bound] = {}
            if on not in routeList[route_id]["fares"][bound] or routeList[route_id][
                "fares"
            ][bound][on][1] < int(off):
                routeList[route_id]["fares"][bound][on] = (
                    "0" if price == "0.0000" else price,
                    int(off),
                )

    for route_id in routeList.keys():
        for bound in routeList[route_id]["stops"].keys():
            _tmp = list(routeList[route_id]["stops"][bound].items())
            _tmp.sort(key=takeFirst)
            routeList[route_id]["stops"][bound] = [v for k, v in _tmp]
        for bound in routeList[route_id]["fares"].keys():
            _tmp = list(routeList[route_id]["fares"][bound].items())
            _tmp.sort(key=takeFirst)
            routeList[route_id]["fares"][bound] = [v[0] for k, v in _tmp]

    nameReg = re.compile("\\[(.*)\\] (.*)")

    def parseStopName(name):
        ret = {}
        for str in name.split("|"):
            matches = nameReg.findall(str)
            if len(matches) == 0:
                return {"unknown": str}
            for co, gtfsName in matches:
                x, y = co.split("+"), gtfsName.split("/<BR>")
                for i in range(len(x)):
                    ret[x[i].lower().replace("lwb", "kmb")] = y[i if i < len(y) else 0]
        return ret

    with open(gtfs_dir / "stops.txt", "r", encoding="UTF-8") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader, None)
        for [
            stop_id,
            stop_name,
            stop_lat,
            stop_lon,
            zone_id,
            location_type,
            stop_timezone,
        ] in reader:
            stopList[stop_id] = {
                "stopId": stop_id,
                "stopName": parseStopName(stop_name),
                "lat": float(stop_lat),
                "lng": float(stop_lon),
            }

    with open(gtfs_dir / "calendar.txt", "r", encoding="UTF-8") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader, None)
        for line in reader:
            [service_id, mon, tue, wed, thur, fri, sat, sun, start_date, end_date] = (
                line
            )
            serviceDayMap[service_id] = [sun, mon, tue, wed, thur, fri, sat]

    with open(DATA_DIR / cfg["output"], "w", encoding="UTF-8") as f:
        f.write(
            json.dumps(
                {
                    "routeList": routeList,
                    "stopList": stopList,
                    "serviceDayMap": serviceDayMap,
                },
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    lang = sys.argv[1] if len(sys.argv) > 1 else "tc"
    asyncio.run(parseGtfs(lang))
