import asyncio
import csv
import datetime
import json
import logging
import re
import zipfile
from collections import namedtuple
from os import path
from typing import Literal
from zoneinfo import ZoneInfo

import httpx
from crawl_utils import emitRequest, store_version
from gtfs_fare import compress_fares, fares_to_csv
from utils import DATA_DIR

LANG_CONFIG = {
    "tc": {
        "url": "https://static.data.gov.hk/td/pt-headway-tc/gtfs.zip",
        "zip": "gtfs-tc.zip",
        "dir": "gtfs-tc",
        "version_key": "GTFS-TC",
        "lang_key": "tc",
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


def compress_freq_entries(entries: dict) -> str:
    parts = []
    prev_freq_end = None

    for start in sorted(entries.keys()):
        val = entries[start]

        if val is None:
            if prev_freq_end is not None and prev_freq_end != start:
                parts.append(prev_freq_end)
            prev_freq_end = None
            parts.append(start)
        else:
            end_time, headway_secs = val
            if prev_freq_end is not None and prev_freq_end != start:
                parts.append(prev_freq_end)
            parts.append(f"{start},{int(headway_secs) // 60}")
            prev_freq_end = end_time

    if prev_freq_end is not None:
        parts.append(prev_freq_end)

    return "|".join(parts)


def has_circular_wording(route_name_part: str) -> bool:
    return any(
        circular_wording in route_name_part.lower()
        for circular_wording in ("循環線", "循环线", "circular")
    )


def store_circular_flag(gtfs_route: dict, is_circular: bool) -> None:
    gtfs_route["is_circular"] = gtfs_route.get("is_circular", False) or is_circular


def store_circular_evidence(gtfs_route: dict, evidence_type: str, found: bool) -> None:
    evidence = gtfs_route.setdefault("_circular_evidence", {})
    evidence[evidence_type] = evidence.get(evidence_type, False) or found
    store_circular_flag(gtfs_route, found)


def has_circular_stop_ids(route_seq_stops: list[str]) -> bool:
    return len(route_seq_stops) > 1 and route_seq_stops[0] == route_seq_stops[-1]


def format_route_for_log(route_id: str, gtfs_route: dict) -> str:
    return (
        f"{gtfs_route['route']} gtfs_id={route_id} "
        f"{gtfs_route['orig'].get('tc', '?')} -> {gtfs_route['dest'].get('tc', '?')}"
    )


def refresh_circular_flags(routeList: dict) -> None:
    for route_id, gtfs_route in routeList.items():
        circular_route_seqs = [
            route_seq
            for route_seq, route_seq_stops in gtfs_route["stops"].items()
            if has_circular_stop_ids(route_seq_stops)
        ]
        store_circular_evidence(gtfs_route, "stop_id", len(circular_route_seqs) > 0)

        evidence = gtfs_route.get("_circular_evidence", {})
        if gtfs_route["is_circular"]:
            evidence_types = [
                evidence_type for evidence_type, found in evidence.items() if found
            ]
            logger.info(
                "Circular route guessed by %s: %s",
                ",".join(evidence_types),
                format_route_for_log(route_id, gtfs_route),
            )

        if gtfs_route["is_circular"] and len(gtfs_route["stops"]) > 1:
            logger.warning(
                "Circular route has more than 1 route_seq: route_seqs=%s %s",
                ",".join(gtfs_route["stops"].keys()),
                format_route_for_log(route_id, gtfs_route),
            )

        if evidence.get("name") and not evidence.get("stop_id"):
            logger.warning(
                "Circular evidence mismatch: name=true stop_id=false %s",
                format_route_for_log(route_id, gtfs_route),
            )
        elif evidence.get("stop_id") and not evidence.get("name"):
            logger.warning(
                "Circular evidence mismatch: name=false stop_id=true %s",
                format_route_for_log(route_id, gtfs_route),
            )

        gtfs_route.pop("_circular_evidence", None)


def orig_dest_circular(
    route_long_name: str, lang: Literal["tc", "sc", "en"]
) -> tuple[dict, dict, bool]:
    name_split = route_long_name.split(" - ")
    if len(name_split) > 2:
        logger.warning(
            f"{route_long_name} has more than 1 hyphen, orig & dest may parse wrongly"
        )
    orig = name_split[0]
    raw_dest = name_split[1]
    is_circular = has_circular_wording(raw_dest)

    # TODO: fix dataset error, e.g. (循環線) in sc version
    # should ask TD to fix it

    if lang == "tc":
        dest = raw_dest.replace("(循環線)", "").rstrip()
        return {"tc": orig}, {"tc": dest}, is_circular
    elif lang == "sc":
        dest = raw_dest.replace("(循环线)", "").rstrip()
        return {"sc": orig}, {"sc": dest}, is_circular
    elif lang == "en":
        dest = raw_dest.replace("(CIRCULAR)", "").rstrip()
        return {"en": orig}, {"en": dest}, is_circular


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
                "co": row["agency_id"].lower().split("+"),
                "route": route_no(row["route_short_name"], route_id, langs[0]),
                "stops": {},
                "fares": {},
                "freq": {},
                "orig": {},
                "dest": {},
                "is_circular": False,
                "jt": routeJourneyTime[route_id],
            }

    # Merge route names from each lang
    for lang in langs:
        with open(gtfs_dirs[lang] / "routes.txt", "r", encoding="UTF-8") as csvfile:
            for row in csv.DictReader(csvfile):
                route_id = row["route_id"]
                orig_l, dest_l, is_circular = orig_dest_circular(
                    row["route_long_name"], lang
                )
                routeList[route_id]["orig"].update(orig_l)
                routeList[route_id]["dest"].update(dest_l)
                store_circular_evidence(routeList[route_id], "name", is_circular)

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

    # trips.txt is required because not all trips have frequency
    with open(primary_dir / "frequencies.txt", "r", encoding="UTF-8") as f:
        for row in csv.DictReader(f):
            [route_id, bound, calendar, start_time] = row["trip_id"].split("-")
            routeList[route_id]["freq"][bound][calendar][start_time] = (
                row["end_time"][0:5].replace(":", ""),
                row["headway_secs"],
            )

    for route_id in routeList:
        for bound in routeList[route_id]["freq"]:
            for calendar in routeList[route_id]["freq"][bound]:
                entries = routeList[route_id]["freq"][bound][calendar]
                routeList[route_id]["freq"][bound][calendar] = compress_freq_entries(
                    entries
                )

    # parse stop seq
    with open(primary_dir / "stop_times.txt", "r", encoding="UTF-8") as f:
        for row in csv.DictReader(f):
            [route_id, bound, _, _] = row["trip_id"].split("-")
            if bound not in routeList[route_id]["stops"]:
                routeList[route_id]["stops"][bound] = {}
            routeList[route_id]["stops"][bound][row["stop_sequence"]] = row["stop_id"]

    # parse fares
    _FareRec = namedtuple("_FareRec", ["on_seq", "off_seq", "price"])
    _fare_records = {}
    with open(primary_dir / "fare_attributes.txt", "r", encoding="UTF-8") as f:
        for row in csv.DictReader(f):
            [route_id, bound, on, off] = row["fare_id"].split("-")
            _fare_records.setdefault((route_id, bound), []).append(
                _FareRec(int(on), int(off), row["price"])
            )

    for (route_id, bound), records in _fare_records.items():
        sections, groups = compress_fares(records)
        routeList[route_id]["fares"][bound] = fares_to_csv(sections, groups)

    for route_id in routeList.keys():
        for bound in routeList[route_id]["stops"].keys():
            _tmp = list(routeList[route_id]["stops"][bound].items())
            _tmp.sort(key=takeFirst)
            routeList[route_id]["stops"][bound] = [v for k, v in _tmp]

    refresh_circular_flags(routeList)

    # TODO: understand what is nameReg
    nameReg = re.compile("\\[(.*)\\] (.*)")

    def parseStopName(name: str, stop_id: str):
        ret = {}
        for stop_name_raw in name.split("|"):
            matches = nameReg.findall(stop_name_raw)
            # if it is not a bus stop (no [company] in the name)
            if len(matches) == 0:
                # match the pattern of stop_id
                # it correlated with stop_id in <Routes and fares of public transport> dataset
                if re.match(r"200\d{5}", stop_id):
                    # GMB stop id is 8-digit starts with 2, e.g. 20022900
                    ret["gmb"] = stop_name_raw
                elif re.match(r"99\d{3}", stop_id):
                    # Tram stop id is 5-digit starts with 99, e.g. 99313
                    ret["tram"] = stop_name_raw
                elif re.match(r"80000\d{3}", stop_id) or re.match(r"101\d{3}", stop_id):
                    # Ferry stop id is either: 8-digit starts with 8, or 6-digit starts with 1
                    ret["ferry"] = stop_name_raw
                else:
                    logger.warning(
                        f"Unable to parseStopName, name: {name}, stop_id: {stop_id}"
                    )
            else:
                for co, gtfsName in matches:
                    # e.g. [KMB+CTB] 油麻地碧街/<BR>碧街, 弥敦道
                    # kmb: 油麻地碧街, ctb: 碧街, 弥敦道
                    companies = co.split("+")
                    stop_names = gtfsName.split("/<BR>")

                    if len(companies) != len(stop_names):
                        # e.g. KMB+CTB 頌雅路 -> share the same name
                        if len(stop_names) == 1:
                            stop_names = stop_names * len(companies)
                        # e.g. KMB 跑馬地馬場/<BR>跑馬地馬場, 摩理臣山道 -> pick the first name
                        elif len(companies) == 1:
                            stop_names = stop_names[:1]
                        else:
                            logger.warning(
                                f"Unable to parseStopName, co: {co}, name: {gtfsName}"
                            )

                    for co_code, stop_name in zip(companies, stop_names, strict=True):
                        co_code = co_code.lower()
                        ret[co_code] = stop_name

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
                for co, name in parseStopName(row["stop_name"], row["stop_id"]).items():
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
