# -*- coding: utf-8 -*-
import asyncio
import csv
import json
import logging
import re
import string

import httpx
from crawl_utils import emitRequest
from pyproj import Transformer
from utils import DATA_DIR, query_igeocom_geojson

res = []
mtrStops = {}
epsgTransformer = Transformer.from_crs("epsg:2326", "epsg:4326")


def checkResult(results, q, stop, exit, barrierFree):
    for result in results:
        if result["nameZH"] == q:
            lat, lng = epsgTransformer.transform(result["y"], result["x"])
            res.append(
                {
                    "name_en": stop["name_en"],
                    "name_zh": stop["name_tc"],
                    "name": {
                        "en": stop["name_en"],
                        "zh": stop["name_tc"],
                    },
                    "exit": exit,
                    "lat": lat,
                    "lng": lng,
                    "barrierFree": barrierFree,
                }
            )
            return True
    return False


async def main():
    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))

    # exits[chinese station name][exit code]
    exits = {}
    igeocom_features = query_igeocom_geojson(class_code="TRS", type_code="MTA")

    for feature in igeocom_features:
        raw_chi_name = feature["properties"]["CHINESENAME"]
        # general pattern 香港鐵路九龍站-D2進出口
        # accept 港鐵xx站, e.g. 港鐵羅湖站-A出口
        # ignore: with no exit code, e.g. 香港鐵路啟德站進出口
        m = re.search(r"(?:香)?港鐵(?:路)?(\S+)站-([A-Z0-9]+)(?:進)?出口", raw_chi_name)
        if not m:
            continue
        chi_name = m.group(1)
        exit_code = m.group(2)
        if chi_name not in exits:
            exits[chi_name] = {}

        if exit_code in exits[chi_name]:
            raise ValueError(f"More than one {chi_name} station {exit_code} exit")

        exits[chi_name][exit_code] = feature

    r = await emitRequest(
        "https://opendata.mtr.com.hk/data/mtr_lines_and_stations.csv", a_client
    )
    r.encoding = "utf-8-sig"
    reader = csv.DictReader(r.text.strip().split("\n"))
    for entry in reader:
        mtrStops[entry["Station ID"]] = {
            "name_tc": entry["Chinese Name"],
            "name_en": entry["English Name"],
            "barrierFreeExits": [],
        }

    r = await emitRequest(
        "https://opendata.mtr.com.hk/data/barrier_free_facilities.csv", a_client
    )
    r.encoding = "utf-8-sig"
    reader = csv.DictReader(r.text.strip().split("\n"))
    for entry in reader:
        if entry["Value"] == "Y" and entry["AJTextEn"] != "":
            for exit in re.findall(" [A-Z][\\d]*", entry["AJTextEn"]):
                if entry["Station_No"] in mtrStops:
                    mtrStops[entry["Station_No"]]["barrierFreeExits"].append(exit)

    # crawl exit geolocation
    for key, stop in mtrStops.items():
        q = "港鐵" + stop["name_tc"] + "站進出口"
        r = await emitRequest(
            "https://geodata.gov.hk/gs/api/v1.0.0/locationSearch?q=" + q, a_client
        )
        for char in string.ascii_uppercase:
            q = "港鐵" + stop["name_tc"] + "站-" + str(char) + "進出口"
            checkResult(r.json(), q, stop, char, str(char) in stop)
            for i in range(1, 10):
                q = "港鐵" + stop["name_tc"] + "站-" + char + str(i) + "進出口"
                checkResult(
                    r.json(), q, stop, char + str(i), (char + str(char)) in stop
                )

    with open(DATA_DIR / "exits.mtr.json", "w", encoding="UTF-8") as f:
        f.write(
            json.dumps(
                list({(v["name"]["zh"] + v["exit"]): v for v in res}.values()),
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(main())
