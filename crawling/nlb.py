import asyncio
import json
import logging
from pathlib import Path

try:
    from .crawl_utils import dump_provider_data
    from .gtfs_fare import fare_list_to_csv
    from .nlb_crawl import (
        RAW_ROUTE_LIST,
        RAW_ROUTE_STOP_LIST,
        RAW_STOP_LIST,
    )
    from .schemas import ProviderRoute, ProviderStop
except ImportError:
    from crawl_utils import dump_provider_data
    from gtfs_fare import fare_list_to_csv
    from nlb_crawl import (
        RAW_ROUTE_LIST,
        RAW_ROUTE_STOP_LIST,
        RAW_STOP_LIST,
    )
    from schemas import ProviderRoute, ProviderStop

logger = logging.getLogger(__name__)


def load_raw_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(
            f"{path} does not exist. Run crawling/nlb_crawl.py first."
        )
    return json.loads(path.read_text("utf-8"))


def ensure_raw_files_exist():
    missing_files = [
        path
        for path in [RAW_ROUTE_LIST, RAW_ROUTE_STOP_LIST, RAW_STOP_LIST]
        if not path.exists()
    ]
    if missing_files:
        missing_file_list = ", ".join(str(path) for path in missing_files)
        raise FileNotFoundError(
            f"Missing NLB raw file(s): {missing_file_list}. "
            "Run crawling/nlb_crawl.py first."
        )


def get_route_obj(co: str, route: dict, route_stops: list[dict]) -> ProviderRoute:
    stop_ids = [stop["stopId"] for stop in route_stops]
    fares = [stop["fare"] for stop in route_stops]
    fares_holiday = [stop["fareHoliday"] for stop in route_stops]
    some_departure_observe_only = [
        stop["someDepartureObserveOnly"] for stop in route_stops
    ]

    return {
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
        "stops": stop_ids,
        "fares": fare_list_to_csv(fares[0:-1]),
        "faresHoliday": fare_list_to_csv(fares_holiday[0:-1]),
        "someDepartureObserveOnly": some_departure_observe_only[0:-1],
        "co": co,
    }


def get_stop_obj(stop: dict) -> ProviderStop:
    return {
        "stop": stop["stopId"],
        "name_en": stop["stopName_e"],
        "name_tc": stop["stopName_c"],
        "name_sc": stop["stopName_s"],
        "lat": stop["latitude"],
        "lng": stop["longitude"],
    }


async def getRouteStop(co):
    ensure_raw_files_exist()

    raw_route_list = load_raw_json(RAW_ROUTE_LIST)
    raw_route_stop_list = load_raw_json(RAW_ROUTE_STOP_LIST)
    raw_stop_list = load_raw_json(RAW_STOP_LIST)

    logger.info("Preparing data of nlb")

    route_list: list[ProviderRoute] = [
        get_route_obj(co, route, raw_route_stop_list[str(route["routeId"])])
        for route in raw_route_list
    ]
    stop_list: dict[str, ProviderStop] = {
        stop_id: get_stop_obj(stop) for stop_id, stop in raw_stop_list.items()
    }

    dump_provider_data(co, route_list, stop_list)
    logger.info("Dumped lists")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(getRouteStop("nlb"))
