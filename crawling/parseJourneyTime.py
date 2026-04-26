import asyncio
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from os import path

import httpx
from crawl_utils import emitRequest, store_version
from utils import DATA_DIR


async def parseJourneyTime():
    a_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, pool=None))
    if not path.isfile(DATA_DIR / "ROUTE_BUS.xml"):
        r = await emitRequest(
            "https://static.data.gov.hk/td/routes-fares-xml/ROUTE_BUS.xml", a_client
        )
        r.encoding = "utf-8"
        with open(DATA_DIR / "ROUTE_BUS.xml", "w", encoding="UTF-8") as f:
            f.write(r.text)

    routeTimeList = {}
    tree = ET.parse(DATA_DIR / "ROUTE_BUS.xml")
    root = tree.getroot()
    version = datetime.fromisoformat(root.attrib["generated"] + "+08:00")
    store_version("routes-fares-xml/ROUTE_BUS", version.isoformat())
    for route in root.iter("ROUTE"):
        if route.find("ROUTE_TYPE").text == "1":
            routeTimeList[route.find("ROUTE_ID").text] = {
                "co": route.find("COMPANY_CODE")
                .text.replace("LWB", "KMB")
                .lower()
                .split("+"),
                "route": route.find("ROUTE_NAMEC").text,
                "journeyTime": route.find("JOURNEY_TIME").text,
            }

    with open(DATA_DIR / "routeTime.json", "w", encoding="UTF-8") as f:
        f.write(json.dumps(routeTimeList, ensure_ascii=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    asyncio.run(parseJourneyTime())
