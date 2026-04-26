# -*- coding: utf-8 -*-
# MTR Bus fetching

import asyncio
import csv
import json
import logging

import httpx
from crawl_utils import emitRequest
from utils import DATA_DIR, query_igeocom_geojson

BASE_URL = "https://opendata.mtr.com.hk/data"
GEODATA_URL = "https://geodata.gov.hk/gs/api/v1.0.0"


def lines_stations_url():
    return BASE_URL + "/mtr_lines_and_stations.csv"


def filterStops(route):
    route["stops"] = [stop for stop in route["stops"] if stop is not None]
    return route


async def getRouteStop(co="mtr"):
    if (DATA_DIR / f"routeList.{co}.json").exists() and (
        DATA_DIR / f"stopList.{co}.json"
    ).exists():
        return
    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))

    routeList = {}
    stopList = {}

    igeocom_features = query_igeocom_geojson(class_code="TRS", type_code="RSN")
    stations = {}

    for feature in igeocom_features:
        raw_chi_name = feature["properties"]["CHINESENAME"]
        # e.g. 香港鐵路粉嶺站 -> 粉嶺
        chi_name = raw_chi_name.replace("香港鐵路", "").replace("站", "")
        stations[chi_name] = feature

    r = await emitRequest(lines_stations_url(), a_client)
    r.encoding = "utf-8"
    reader = csv.reader(r.text.split("\n"))
    headers = next(reader, None)
    routes = [route for route in reader if len(route) == 7]
    for [route, bound, stopCode, stopId, chn, eng, seq] in routes:
        if route == "":
            continue
        if route + "_" + bound not in routeList:
            routeList[route + "_" + bound] = {
                "gtfsId": None,
                "route": route,
                "bound": bound,
                "service_type": "1",
                "orig_tc": None,
                "orig_en": None,
                "dest_tc": None,
                "dest_en": None,
                "stops": [None] * 100,
                "fare": [],
            }
        if int(float(seq)) == 1:
            routeList[route + "_" + bound]["orig_tc"] = chn
            routeList[route + "_" + bound]["orig_en"] = eng
        routeList[route + "_" + bound]["dest_tc"] = chn
        routeList[route + "_" + bound]["dest_en"] = eng
        routeList[route + "_" + bound]["stops"][int(float(seq))] = stopCode
        if stopCode not in stopList:
            # station name in igeocom is different
            # the 力 vs 刀 part in "Lai"
            chn_map = {"茘景": "荔景", "茘枝角": "荔枝角"}
            search_chn = chn if chn not in chn_map else chn_map[chn]
            feature = stations[search_chn]

            lng, lat = feature["geometry"]["coordinates"]
            stopList[stopCode] = {
                "stop": stopCode,
                "name_en": eng,
                "name_tc": chn,
                "lat": lat,
                "long": lng,
            }

    with open(DATA_DIR / "routeList.mtr.json", "w", encoding="UTF-8") as f:
        f.write(
            json.dumps(
                list(
                    map(
                        filterStops,
                        [
                            route
                            for route in routeList.values()
                            if len(route["stops"]) > 0
                        ],
                    )
                ),
                ensure_ascii=False,
            )
        )
    with open(DATA_DIR / "stopList.mtr.json", "w", encoding="UTF-8") as f:
        f.write(json.dumps(stopList, ensure_ascii=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    asyncio.run(getRouteStop())
