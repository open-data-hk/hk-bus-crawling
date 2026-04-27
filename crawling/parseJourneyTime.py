import asyncio
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from os import path

import httpx
from crawl_utils import emitRequest, store_version
from utils import DATA_DIR

BASE_URL = "https://static.data.gov.hk/td/routes-fares-xml"
ROUTE_TYPES = ["ROUTE_BUS", "ROUTE_TRAM", "ROUTE_PTRAM", "ROUTE_GMB", "ROUTE_FERRY"]


async def download_route_file(name, a_client):
    if not path.isfile(DATA_DIR / f"{name}.xml"):
        r = await emitRequest(f"{BASE_URL}/{name}.xml", a_client)
        r.encoding = "utf-8"
        with open(DATA_DIR / f"{name}.xml", "w", encoding="UTF-8") as f:
            f.write(r.text)


async def parseJourneyTime():
    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))

    await asyncio.gather(*[download_route_file(name, a_client) for name in ROUTE_TYPES])

    routeTimeList = {}
    for name in ROUTE_TYPES:
        tree = ET.parse(DATA_DIR / f"{name}.xml")
        root = tree.getroot()
        version = datetime.fromisoformat(root.attrib["generated"] + "+08:00")
        store_version(f"routes-fares-xml/{name}", version.isoformat())
        for route in root.iter("ROUTE"):
            routeTimeList[route.find("ROUTE_ID").text] = route.find("JOURNEY_TIME").text
            # Was dict below before, found only jt is used in parseGtfs.py
            # {
            #     "co": route.find("COMPANY_CODE")
            #     .text.replace("LWB", "KMB")
            #     .lower()
            #     .split("+"),
            #     "route": route.find("ROUTE_NAMEC").text,
            #     "journeyTime": route.find("JOURNEY_TIME").text,
            # }

    with open(DATA_DIR / "routeTime.json", "w", encoding="UTF-8") as f:
        f.write(json.dumps(routeTimeList, ensure_ascii=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    asyncio.run(parseJourneyTime())
