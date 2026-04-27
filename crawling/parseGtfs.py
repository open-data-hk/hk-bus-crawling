import asyncio
import csv
import datetime
import json
import logging
import re
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
        "zip": "gtfs-tc.zip",
        "dir": "gtfs-tc",
        "version_key": "GTFS-TC",
        # TODO: change to "tc"
        "lang_key": "zh",
    },
    "en": {
        "url": "https://static.data.gov.hk/td/pt-headway-en/gtfs.zip",
        "zip": "gtfs-en.zip",
        "dir": "gtfs-en",
        "version_key": "GTFS-EN",
        "lang_key": "en",
    },
    "sc": {
        "url": "https://static.data.gov.hk/td/pt-headway-sc/gtfs.zip",
        "zip": "gtfs-sc.zip",
        "dir": "gtfs-sc",
        "version_key": "GTFS-SC",
        "lang_key": "sc",
    },
}

ALL_LANGS = list(LANG_CONFIG.keys())


def takeFirst(elem):
    return int(elem[0])


def orig_dest(
    route_long_name: str, lang: Literal["tc", "sc", "en"]
) -> tuple[dict, dict]:
    # TODO: inspect if more than 1 hyphen
    name_split = route_long_name.split(" - ")
    orig = name_split[0]
    # TODO: correctly handle tc sc
    dest = name_split[1].replace(" (CIRCULAR)", "")

    if lang == "tc":
        # TODO: tc version should not contains English?
        return {"zh": orig}, {"zh": dest}
    elif lang == "sc":
        return {"sc": orig}, {"sc": dest}
    elif lang == "en":
        return {"en": orig}, {"en": dest}


def route_no(
    route_short_name: str, route_id: str, lang: Literal["tc", "sc", "en"]
) -> str:
    if lang in ("tc", "sc"):
        return route_short_name
    elif lang == "en":
        return route_short_name if route_short_name != "" else route_id


async def fetchAndExtract(lang: str, a_client: httpx.AsyncClient):
    cfg = LANG_CONFIG[lang]
    if not path.isfile(DATA_DIR / cfg["zip"]):
        r = await emitRequest(cfg["url"], a_client)
        open(DATA_DIR / cfg["zip"], "wb").write(r.content)
    gtfs_dir = DATA_DIR / cfg["dir"]
    with zipfile.ZipFile(DATA_DIR / cfg["zip"], "r") as zip_ref:
        zip_ref.extractall(gtfs_dir)
        version = min([f.date_time for f in zip_ref.infolist()])
        version = datetime.datetime(*version, tzinfo=ZoneInfo("Asia/Hong_Kong"))
        store_version(cfg["version_key"], version.isoformat())
    return lang, gtfs_dir


async def parseGtfs():
    langs = ALL_LANGS

    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))

    results = await asyncio.gather(*[fetchAndExtract(lang, a_client) for lang in langs])
    gtfs_dirs = dict(results)
    primary_dir = gtfs_dirs[langs[0]]

    routeList = {}
    stopList = {}
    serviceDayMap = {}
    routeJourneyTime = json.load(
        open(DATA_DIR / "routeTime.json", "r", encoding="UTF-8")
    )

    # Initialize route structure from primary lang
    with open(primary_dir / "routes.txt", "r", encoding="UTF-8") as csvfile:
        for row in csv.DictReader(csvfile):
            route_id = row["route_id"]
            routeList[route_id] = {
                "co": row["agency_id"].replace("LWB", "KMB").lower().split("+"),
                "route": route_no(row["route_short_name"], route_id, langs[0]),
                "stops": {},
                "fares": {},
                "freq": {},
                "orig": {},
                "dest": {},
                "jt": (
                    routeJourneyTime[route_id]["journeyTime"]
                    if route_id in routeJourneyTime
                    else None
                ),
            }

    # Merge route names from each lang
    for lang in langs:
        with open(gtfs_dirs[lang] / "routes.txt", "r", encoding="UTF-8") as csvfile:
            for row in csv.DictReader(csvfile):
                route_id = row["route_id"]
                orig_l, dest_l = orig_dest(row["route_long_name"], lang)
                # TODO: cleaner update?
                routeList[route_id]["orig"].update(
                    {k: v for k, v in orig_l.items() if v}
                )
                routeList[route_id]["dest"].update(
                    {k: v for k, v in dest_l.items() if v}
                )

    # parse timetable
    with open(primary_dir / "trips.txt", "r", encoding="UTF-8") as f:
        for row in csv.DictReader(f):
            [route_id, bound, calendar, start_time] = row["trip_id"].split("-")
            if bound not in routeList[route_id]["freq"]:
                routeList[route_id]["freq"][bound] = {}
            if calendar not in routeList[route_id]["freq"][bound]:
                routeList[route_id]["freq"][bound][calendar] = {}
            if start_time not in routeList[route_id]["freq"][bound][calendar]:
                routeList[route_id]["freq"][bound][calendar][start_time] = None

    with open(primary_dir / "frequencies.txt", "r", encoding="UTF-8") as f:
        for row in csv.DictReader(f):
            [route_id, bound, calendar, start_time] = row["trip_id"].split("-")
            routeList[route_id]["freq"][bound][calendar][start_time] = (
                row["end_time"][0:5].replace(":", ""),
                row["headway_secs"],
            )

    # parse stop seq
    with open(primary_dir / "stop_times.txt", "r", encoding="UTF-8") as f:
        for row in csv.DictReader(f):
            [route_id, bound, _, _] = row["trip_id"].split("-")
            if bound not in routeList[route_id]["stops"]:
                routeList[route_id]["stops"][bound] = {}
            routeList[route_id]["stops"][bound][row["stop_sequence"]] = row["stop_id"]

    # parse fares
    with open(primary_dir / "fare_attributes.txt", "r", encoding="UTF-8") as f:
        for row in csv.DictReader(f):
            [route_id, bound, on, off] = row["fare_id"].split("-")
            price = row["price"]
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

    # TODO: understand what is nameReg
    nameReg = re.compile("\\[(.*)\\] (.*)")

    def parseStopName(name):
        ret = {}
        for stop_name_raw in name.split("|"):
            matches = nameReg.findall(stop_name_raw)
            if len(matches) == 0:
                return {"unknown": stop_name_raw}
            for co, gtfsName in matches:
                # TODO: improve code clarity
                x, y = co.split("+"), gtfsName.split("/<BR>")
                for i in range(len(x)):
                    ret[x[i].lower().replace("lwb", "kmb")] = y[i if i < len(y) else 0]
        return ret

    # Initialize stopList with coords from primary lang
    with open(primary_dir / "stops.txt", "r", encoding="UTF-8") as f:
        for row in csv.DictReader(f):
            stop_id = row["stop_id"]
            stopList[stop_id] = {
                "stopId": stop_id,
                "stopName": {},
                "lat": float(row["stop_lat"]),
                "lng": float(row["stop_lon"]),
            }

    # Merge stop names from each lang: stopName[co][lang_key] = name
    for lang in langs:
        lang_key = LANG_CONFIG[lang]["lang_key"]
        with open(gtfs_dirs[lang] / "stops.txt", "r", encoding="UTF-8") as f:
            for row in csv.DictReader(f):
                for co, name in parseStopName(row["stop_name"]).items():
                    if co not in stopList[row["stop_id"]]["stopName"]:
                        stopList[row["stop_id"]]["stopName"][co] = {}
                    # breaking changes: multiple lang names per co
                    # TODO: make sure not breaking depending script
                    stopList[row["stop_id"]]["stopName"][co][lang_key] = name

    with open(primary_dir / "calendar.txt", "r", encoding="UTF-8") as f:
        for row in csv.DictReader(f):
            serviceDayMap[row["service_id"]] = [
                row["sunday"],
                row["monday"],
                row["tuesday"],
                row["wednesday"],
                row["thursday"],
                row["friday"],
                row["saturday"],
            ]

    with open(DATA_DIR / "gtfs.json", "w", encoding="UTF-8") as f:
        f.write(
            json.dumps(
                {
                    "routeList": routeList,
                    "stopList": stopList,
                    "serviceDayMap": serviceDayMap,
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)

    asyncio.run(parseGtfs())
