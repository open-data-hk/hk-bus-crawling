import asyncio
import json
import logging
from os import path

import httpx
from crawl_utils import dump_provider_data, emitRequest
from schemas import ProviderRoute, ProviderStop
from utils import DATA_DIR

logger = logging.getLogger(__name__)

BASE_URL = "https://rt.data.gov.hk/v2/transport/nlb"


def routes_url():
    return BASE_URL + "/route.php?action=list"


def stop_url(routeId: str):
    return BASE_URL + "/stop.php?action=list&routeId=" + routeId


async def getRouteStop(co):
    # define output name
    ROUTE_LIST = DATA_DIR / ("routeList." + co + ".json")
    STOP_LIST = DATA_DIR / ("stopList." + co + ".json")

    a_client = httpx.AsyncClient()
    # load route list and stop list if exist
    routeList: list[ProviderRoute] = []
    if path.isfile(ROUTE_LIST):
        logger.warning(f"{ROUTE_LIST} already exist, skipping...")
        return
    else:
        # load routes
        r = await emitRequest(routes_url(), a_client)
        for route in r.json()["routes"]:
            routeList.append(
                {
                    "id": route["routeId"],
                    "route": route["routeNo"],
                    "bound": "O",
                    "orig_en": route["routeName_e"].split(" > ")[0],
                    "orig_tc": route["routeName_c"].split(" > ")[0],
                    "orig_sc": route["routeName_s"].split(" > ")[0],
                    "dest_en": route["routeName_e"].split(" > ")[1],
                    "dest_tc": route["routeName_c"].split(" > ")[1],
                    "dest_sc": route["routeName_s"].split(" > ")[1],
                    "service_type": str(
                        1 + route["overnightRoute"] * 2 + route["specialRoute"] * 4
                    ),
                    "stops": [],
                    "co": co,
                }
            )
        logger.info("Digested route list")

    stopList: dict[str, ProviderStop] = {}
    if path.isfile(STOP_LIST):
        with open(STOP_LIST, "r", encoding="UTF-8") as f:
            stopList = json.load(f)

    async def getRouteStop(routeId):
        r = await emitRequest(stop_url(routeId), a_client)
        try:
            return r.json()["stops"]
        except Exception as err:
            print(r)
            raise err

    async def addRouteStop(route):
        stops = await getRouteStop(route["id"])
        stopIds = []
        fares = []
        faresHoliday = []
        for stop in stops:
            if stop["stopId"] not in stopList:
                stopList[stop["stopId"]] = {
                    "stop": stop["stopId"],
                    "name_en": stop["stopName_e"],
                    "name_tc": stop["stopName_c"],
                    "name_sc": stop["stopName_s"],
                    "lat": stop["latitude"],
                    "lng": stop["longitude"],
                }
            stopIds.append(stop["stopId"])
            fares.append(stop["fare"])
            faresHoliday.append(stop["fareHoliday"])
        route["stops"] = stopIds
        route["fares"] = fares[0:-1]
        route["faresHoliday"] = faresHoliday[0:-1]

    async def getRouteStopList():
        await asyncio.gather(*[addRouteStop(r) for r in routeList])
        logger.info("Digested stop list")
        return routeList

    await getRouteStopList()

    dump_provider_data(co, routeList, stopList)
    logger.info("Dumped lists")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(getRouteStop("nlb"))
