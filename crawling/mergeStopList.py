import json
import logging
import math
import time
from typing import TypedDict, cast

from haversine import Unit, haversine
from utils import DATA_DIR


class Location(TypedDict):
    lat: float
    lng: float


class Stop(TypedDict):
    location: Location


class RouteListEntry(TypedDict, total=False):
    stops: dict[str, list[str]]


class RouteStop(TypedDict):
    routeKey: str
    co: str
    seq: int
    bearing: float


class StopSeqEntry(TypedDict):
    routeStops: list[RouteStop]
    co: str
    bearings: list[float]


class NearbyStopEntry(TypedDict):
    id: str
    co: str


type RouteList = dict[str, RouteListEntry]
type StopList = dict[str, Stop]
type StopSeqMapping = dict[str, StopSeqEntry]
type StopListGrid = dict[str, list[str]]
type StopListGridKey = dict[str, str]
type StopGroup = list[list[str]]
type DistanceCache = dict[tuple[str, str], float]
type BearingRange = tuple[float, float]


def get_stops_haversine_distance(stop_a: Stop, stop_b: Stop) -> float:
    if (
        stop_a["location"]["lat"] == stop_b["location"]["lat"]
        and stop_a["location"]["lng"] == stop_b["location"]["lng"]
    ):
        return 0
    return haversine(
        (stop_a["location"]["lat"], stop_a["location"]["lng"]),
        (stop_b["location"]["lat"], stop_b["location"]["lng"]),
        unit=Unit.METERS,  # specify that we want distance in meter, default is km
    )


def get_stop_group(
    stop_list: StopList,
    stop_seq_mapping: StopSeqMapping,
    stop_list_grid: StopListGrid,
    stop_list_grid_key: StopListGridKey,
    distance_cache: DistanceCache,
    stop_id: str,
) -> StopGroup:
    DISTANCE_THRESHOLD = 50  # in metres
    BEARING_THRESHOLD = 45  # in degrees
    STOP_LIST_LIMIT = 50  # max number of stops in a group

    bearing_targets = stop_seq_mapping.get(stop_id, {}).get("bearings", [])
    bearing_ranges: list[BearingRange] = []
    for target in bearing_targets:
        bearing_min = target - BEARING_THRESHOLD
        bearing_max = target + BEARING_THRESHOLD
        if bearing_min < 0:
            bearing_min += 360
        if bearing_max > 360:
            bearing_max -= 360
        bearing_ranges.append((bearing_min, bearing_max))

    def is_bearing_in_range(bearing: float) -> bool:
        if BEARING_THRESHOLD >= 180 or not bearing_ranges:
            return True
        for bearing_min, bearing_max in bearing_ranges:
            if bearing_min <= bearing <= bearing_max or (
                bearing_min > bearing_max
                and (bearing <= bearing_max or bearing >= bearing_min)
            ):
                return True
        return False

    def get_cached_stop_distance(stop_id_a: str, stop_id_b: str) -> float:
        cache_key = (
            (stop_id_a, stop_id_b) if stop_id_a <= stop_id_b else (stop_id_b, stop_id_a)
        )
        if cache_key not in distance_cache:
            distance_cache[cache_key] = get_stops_haversine_distance(
                stop_list[stop_id_a], stop_list[stop_id_b]
            )
        return distance_cache[cache_key]

    def search_nearby_stops(
        target_stop_id: str, excluded_stop_ids: set[str]
    ) -> list[NearbyStopEntry]:
        nearby_stops = []
        for stop_id in stop_list_grid.get(stop_list_grid_key[target_stop_id], []):
            if stop_id in excluded_stop_ids:
                continue

            stop_seq_entry = stop_seq_mapping.get(stop_id, {})
            bearings = stop_seq_entry.get("bearings", [])

            if not any(is_bearing_in_range(b) for b in bearings):
                continue

            if get_cached_stop_distance(target_stop_id, stop_id) > DISTANCE_THRESHOLD:
                continue

            nearby_stops.append(
                {
                    "id": stop_id,
                    "co": stop_seq_entry.get("co", ""),
                }
            )
        return nearby_stops

    stop_group: StopGroup = []
    stop_list_entries = search_nearby_stops(stop_id, {stop_id})
    discovered_stop_ids = {entry["id"] for entry in stop_list_entries}
    discovered_stop_ids.add(stop_id)

    # recursively search for nearby stops within thresholds (distance and bearing)
    # stop searching when no new stops are found within range, or when stop
    # list is getting too large
    i = 0
    while i < len(stop_list_entries):
        entry = stop_list_entries[i]
        stop_group.append([entry["co"], entry["id"]])
        i += 1
        if len(stop_list_entries) < STOP_LIST_LIMIT:
            new_entries = search_nearby_stops(entry["id"], discovered_stop_ids)
            discovered_stop_ids.update(entry["id"] for entry in new_entries)
            stop_list_entries.extend(new_entries)

    return stop_group
    # return stop_group


def get_bearing(a: Location, b: Location) -> float:
    φ1 = math.radians(a["lat"])
    φ2 = math.radians(b["lat"])
    λ1 = math.radians(a["lng"])
    λ2 = math.radians(b["lng"])

    y = math.sin(λ2 - λ1) * math.cos(φ2)
    x = math.cos(φ1) * math.sin(φ2) - math.sin(φ1) * math.cos(φ2) * math.cos(λ2 - λ1)
    θ = math.atan2(y, x)
    brng = (math.degrees(θ) + 360) % 360  # in degrees
    return brng


def get_stop_bearings(route_stops: list[RouteStop]) -> list[float]:
    unique_routes: list[str] = []
    bearings: list[float] = []
    for route_stop in route_stops:
        if route_stop["bearing"] != -1:
            unique_route = f"{route_stop['co']}_{route_stop['routeKey'].split('+')[0]}_{route_stop['bearing']}"
            if unique_route not in unique_routes:
                unique_routes.append(unique_route)
                bearings.append(route_stop["bearing"])

    if not bearings:
        return []

    BEARING_THRESHOLD = 45  # in degrees
    BEARING_EPSILON = 10e-6  # very small number
    bearing_groups: list[list[float]] = []

    for bearing in bearings:
        if bearing == -1:
            continue
        if not bearing_groups:
            bearing_groups.append([bearing])
            continue

        for group in bearing_groups:
            if any(abs(b - bearing) < BEARING_EPSILON for b in group):
                break
            if any(
                abs(b - bearing) <= BEARING_THRESHOLD
                or abs(b - bearing) >= 360 - BEARING_THRESHOLD
                for b in group
            ):
                group.append(bearing)
                break
        else:
            bearing_groups.append([bearing])

    if len(bearing_groups) == 1:
        return bearing_groups[0]

    longest_length = max(len(group) for group in bearing_groups)
    return [
        b for group in bearing_groups if len(group) == longest_length for b in group
    ]


# Main function to process stops


def merge_stop_list() -> None:
    # Read the result from previous pipeline
    with open(
        DATA_DIR / "routeFareList.mergeRoutes.min.json", "r", encoding="UTF-8"
    ) as f:
        db = json.load(f)

    route_list = cast(RouteList, db["routeList"])
    stop_list = cast(StopList, db["stopList"])
    start_time = time.time()
    stop_seq_mapping: StopSeqMapping = {}

    # Preprocess the list of bearings for each stop
    for route_key, route_list_entry in route_list.items():
        stops = route_list_entry.get("stops", {})
        for co, co_stops in stops.items():
            for stop_pos, stop_id in enumerate(co_stops):
                if stop_id not in stop_seq_mapping:
                    stop_seq_mapping[stop_id] = {
                        "routeStops": [],
                        "co": co,
                        "bearings": [],
                    }
                if stop_pos == len(co_stops) - 1:
                    stop_seq_mapping[stop_id]["routeStops"].append(
                        {
                            "routeKey": route_key,
                            "co": co,
                            "seq": stop_pos,
                            "bearing": -1,
                        }
                    )
                else:
                    bearing = get_bearing(
                        stop_list[stop_id]["location"],
                        stop_list[co_stops[stop_pos + 1]]["location"],
                    )
                    stop_seq_mapping[stop_id]["routeStops"].append(
                        {
                            "routeKey": route_key,
                            "co": co,
                            "seq": stop_pos,
                            "bearing": bearing,
                        }
                    )

    for stop_id in stop_seq_mapping.keys():
        stop_seq_mapping[stop_id]["bearings"] = get_stop_bearings(
            stop_seq_mapping[stop_id]["routeStops"]
        )

    # Just dump the json in case of a need for trouble-shooting, but otherwise
    # we do not need this file
    with open(DATA_DIR / "stopMap.routeStopsSequence.json", "w", encoding="UTF-8") as f:
        json.dump(stop_seq_mapping, f)

    logger.info(
        f"Processed routeStopsSequence in {(time.time() - start_time) * 1000:.2f}ms"
    )

    # Preprocess stopList, organise stops into ~100m x ~100m squares to reduce
    # size of nested loop later
    stop_list_grid: StopListGrid = {}
    stop_list_grid_key: StopListGridKey = {}
    for stop_id, stop in stop_list.items():
        # take lat/lng up to 3 decimal places, that's about 100m x 100m square
        lat = int(stop["location"]["lat"] * 1000)
        lng = int(stop["location"]["lng"] * 1000)
        stop_list_grid_key[stop_id] = f"{lat}_{lng}"
        # add stop into the 9 grid boxes surrounding this stop
        grid = [
            f"{lat - 1}_{lng - 1}",
            f"{lat    }_{lng - 1}",
            f"{lat + 1}_{lng - 1}",
            f"{lat - 1}_{lng    }",
            f"{lat    }_{lng    }",
            f"{lat + 1}_{lng    }",
            f"{lat - 1}_{lng + 1}",
            f"{lat    }_{lng + 1}",
            f"{lat + 1}_{lng + 1}",
        ]
        for grid_id in grid:
            if grid_id not in stop_list_grid:
                stop_list_grid[grid_id] = []
            stop_list_grid[grid_id].append(stop_id)

    target_stop_list = list(stop_list.items())
    stop_map: dict[str, StopGroup] = {}
    distance_cache: DistanceCache = {}
    count = 0
    group_count = 0

    for stop_id, stop in target_stop_list:
        count += 1
        # if count % 1000 == 0:
        #     logger.info(f"Processed {count} stops ({group_count} groups) at {(time.time() - start_time) * 1000:.2f}ms")

        stop_group = get_stop_group(
            stop_list,
            stop_seq_mapping,
            stop_list_grid,
            stop_list_grid_key,
            distance_cache,
            stop_id,
        )
        if len(stop_group) > 0:
            group_count += 1
            stop_map[stop_id] = stop_group

    logger.info(
        f"Processed {count} stops ({group_count} groups) at {(time.time() - start_time) * 1000:.2f}ms"
    )

    with open(DATA_DIR / "stopMap.json", "w", encoding="UTF-8") as f:
        json.dump(stop_map, f)

    db["stopMap"] = stop_map

    with open(DATA_DIR / "routeFareList.json", "w", encoding="UTF-8") as f:
        json.dump(db, f)

    # reduce size of routeFareList.min.json by rounding lat/lng values to 5 decimal places
    # 5 d.p. is roughly one-metre accuracy, it is good enough for this project
    # saves around 50kb in size for 14,000 stops
    for stop_id, stop in target_stop_list:
        stop_list[stop_id]["location"]["lat"] = float(
            "%.5f" % (stop_list[stop_id]["location"]["lat"])
        )
        stop_list[stop_id]["location"]["lng"] = float(
            "%.5f" % (stop_list[stop_id]["location"]["lng"])
        )

    db["stopList"] = stop_list

    logger.info(
        f"Reduced location lat/lng to 5 d.p. at {(time.time() - start_time) * 1000:.2f}ms"
    )

    with open(DATA_DIR / "routeFareList.alpha.json", "w", encoding="UTF-8") as f:
        json.dump(db, f)

    with open(DATA_DIR / "routeFareList.min.json", "w", encoding="UTF-8") as f:
        json.dump(db, f)

    with open(DATA_DIR / "routeFareList.alpha.min.json", "w", encoding="UTF-8") as f:
        json.dump(db, f)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    merge_stop_list()
