from __future__ import annotations

import json
import sys
from typing import Any, TypedDict

from gtfs_fare import get_fare
from haversine import haversine
from utils import DATA_DIR

INFINITY_DIST = 1000000
DIST_DIFF = 600

with open(DATA_DIR / "gtfs.json", "r", encoding="UTF-8") as f:
    gtfs = json.load(f)
    gtfs_routes = gtfs["routeList"]
    gtfs_stops = gtfs["stopList"]


class CoStop(TypedDict):
    """A bus/ferry stop as returned by an operator's API."""

    name_tc: str
    name_en: str
    lat: str
    long: str


class GtfsStopName(TypedDict):
    tc: str
    en: str


class GtfsStop(TypedDict):
    """A stop entry from the government GTFS dataset."""

    stopName: dict[str, GtfsStopName]
    lat: float
    lng: float


class Route(TypedDict, total=False):
    """A bus/ferry route as returned by an operator's API, optionally enriched with GTFS data."""

    co: list[str]
    route: str
    bound: str
    orig_en: str
    orig_tc: str
    dest_en: str
    dest_tc: str
    serviceType: str
    stops: list[str]
    gtfsId: str
    virtual: bool
    found: bool
    gtfs: list[str]
    fares: list[float] | None
    freq: dict[str, Any]
    jt: int


# (gtfs_stop_index, co_stop_index) alignment pair produced by the DP backtrack
StopMatch = tuple[int, int]

# Full best-match tuple: (gtfsId, avgDist, stopPairs, bound, gtfsStopIds, route)
BestMatch = tuple[str, float, list[StopMatch], str, list[str], Route]

# GTFS uses "ferry" as the operator code for all ferry companies.
FERRY_COS: set[str] = {"hkkf"}


def isNameMatch(name_a: str, name_b: str) -> bool:
    """Check whether two stop names match via case-insensitive substring comparison.

    Important: GTFS names and operator names are often slightly different
    (e.g. "旺角站" vs "旺角"). Treating one as a substring of the other lets the
    DP alignment assign zero distance without relying solely on GPS coordinates,
    which improves matching accuracy for stops that are geographically close to
    neighbouring route stops.

    Args:
        name_a: First stop name (Chinese or English).
        name_b: Second stop name to compare against.

    Returns:
        True if either name is a substring of the other (case-insensitive).

    Example:
        >>> isNameMatch("旺角站", "旺角")
        True
        >>> isNameMatch("Mong Kok Station", "mong kok")
        True
        >>> isNameMatch("旺角", "尖沙咀")
        False
    """
    tmp_a = name_a.lower()
    tmp_b = name_b.lower()
    return tmp_a.find(tmp_b) >= 0 or tmp_b.find(tmp_a) >= 0


# ctb routes only give list of stops in topological order
# the actual servicing routes may skip some stop in the coStops
# this DP function is trying to map the coStops back to GTFS stops


def matchStopsByDp(
    co_stops: list[CoStop],
    gtfs_stops: list[GtfsStop],
    co: str,
    debug: bool = False,
) -> tuple[list[StopMatch], float]:
    """Align operator stops to GTFS stops using dynamic programming.

    Important: Operator stop lists (e.g. CTB) list stops in topological order
    for the whole corridor, but a particular service variant may skip some of
    them. This DP finds the best alignment between the authoritative GTFS stop
    sequence and the operator's stop list, allowing extra operator stops to be
    skipped while penalising large GPS deviations.

    The cost of aligning a pair is 0 when the Chinese names match exactly,
    otherwise the haversine distance in metres. The DP table allows skipping
    operator stops (move right in the coStops dimension without consuming a
    GTFS stop) but every GTFS stop must be consumed. A small index-gap penalty
    discourages widely-scattered alignments.

    Args:
        coStops: Operator stop objects, each with ``name_tc``, ``lat``,
            ``long`` keys.
        gtfsStops: GTFS stop objects for one direction, each with
            ``stopName`` (dict keyed by operator code → ``{"tc": ..., "en": ...}``),
            ``lat``, and ``lng``.
        co: Operator code (e.g. ``"ctb"``, ``"kmb"``). Used to look up the
            correct Chinese name inside ``gtfsStop["stopName"]``.
            ``"hkkf"`` is remapped to ``"ferry"`` internally.
        debug: Unused placeholder kept for backward compatibility.

    Returns:
        A 2-tuple ``(matches, avg_dist)`` where:

        - ``matches``: list of ``(gtfs_idx, co_idx)`` index pairs showing
          which operator stop was aligned to each GTFS stop.
        - ``avg_dist``: average per-stop distance in metres plus index-gap
          penalty. Returns ``([], INFINITY_DIST)`` when no acceptable
          alignment exists (avg >= ``DIST_DIFF``).

    Example:
        Operator list has 4 stops; GTFS has 3 stops (one operator stop extra):

        >>> matches, avg = matchStopsByDp(coStops4, gtfsStops3, "ctb")
        >>> matches          # operator stop at index 1 was skipped
        [(0, 0), (1, 2), (2, 3)]
        >>> avg < 600
        True
    """
    if co in FERRY_COS:
        co = "ferry"
    if len(gtfs_stops) > len(co_stops) + 1:
        return [], INFINITY_DIST
    if len(gtfs_stops) - len(co_stops) == 1:
        gtfs_stops = gtfs_stops[:-1]

    # initialization: 2D list
    # co_stops: horizontal (x), gtfs_stops: vertical (y)
    #     c0 c1 c2 c3 ...
    # g0   i  i  i  i
    # g1   i  i  i  i
    # g2   i  i  i  i
    # .
    distSum = [
        [INFINITY_DIST for x in range(len(co_stops) + 1)]
        for y in range(len(gtfs_stops) + 1)
    ]
    # distSum is "1-index", set the first n element of the first row as 0
    for j in range(len(co_stops) - len(gtfs_stops) + 1):
        distSum[0][j] = 0
    #     c0 c1 c2 c3
    # g0   0  0  i  i
    # g1   i  i  i  i
    # g2   i  i  i  i

    # Perform DP
    for i, gtfs_stop in enumerate(gtfs_stops):
        for j, co_stop in enumerate(co_stops):
            # if chinese name exactly match, score 0
            # else calculate distance of two stops in meter as score
            dist = (
                0
                if co_stop["name_tc"] == gtfs_stop["stopName"][co]["tc"]
                else haversine(
                    (float(co_stop["lat"]), float(co_stop["long"])),
                    (gtfs_stop["lat"], gtfs_stop["lng"]),
                )
                * 1000
            )

            # the first gtfs_stop and first co_stop
            #     c0 c1 c2 c3
            # g0   0  0  i  i
            # g1   i  d  i  i
            # g2   i  i  i  i
            # d =  min((0 + dist), i) = dist
            distSum[i + 1][j + 1] = min(
                distSum[i][j] + dist,  # from previous stops of both sides
                distSum[i + 1][j],  # skipping current coStops
            )
            # the first row and first column never change

            # After iteratation of all co_stops in the first gtfs_stop
            #     c0 c1 c2 c3 ...
            # g0   0  0  i  i
            # g1   i  d  d  d
            # g2   i  i  e  e
            # e = d + dist

    # fast return if no good result
    cumulative_dist = min(distSum[len(gtfs_stops)])  # min val in the last row
    avg_dist = cumulative_dist / len(gtfs_stops)
    # average distance per stop more than DIST_DIFF (600m) is not ideal
    if not avg_dist < DIST_DIFF:
        return [], INFINITY_DIST

    # backtracking
    i = len(gtfs_stops)
    j = len(co_stops)
    ret: list[StopMatch] = []
    while i > 0 and j > 0:
        if distSum[i][j] == distSum[i][j - 1]:
            j -= 1
        else:
            ret.append((i - 1, j - 1))
            i -= 1
            j -= 1
    ret.reverse()

    # penalty distance is given for not exact match route
    penalty = sum([abs(a - b) for a, b in ret]) * 0.01

    return ret, avg_dist + penalty


def mergeRouteAsCircularRoute(routeA: Route, routeB: Route) -> Route:
    """Merge two directional route halves into a single virtual circular route.

    Important: Some routes (e.g. circular bus routes) are modelled as two
    separate one-way entries in the operator data but as a single circular
    entry in GTFS. Merging them lets the DP matcher compare the full circular
    stop sequence against the GTFS entry in one pass.

    Args:
        routeA: First direction of the circular route. Provides the origin
            name and the leading portion of the stop list.
        routeB: Second direction. Provides the destination name and the
            trailing stop list.

    Returns:
        A new ``Route`` dict tagged with ``virtual: True`` whose ``stops`` is
        the concatenation of both directions and whose ``bound`` is the
        combined bound strings (e.g. ``"IO"``).

    Example:
        >>> merged = mergeRouteAsCircularRoute(routeA, routeB)
        >>> merged["virtual"]
        True
        >>> merged["stops"] == routeA["stops"] + routeB["stops"]
        True
        >>> merged["bound"] == routeA["bound"] + routeB["bound"]
        True
    """
    return {
        "co": routeA["co"],
        "route": routeA["route"],
        "bound": routeA["bound"] + routeB["bound"],
        "orig_en": routeA["orig_en"],
        "orig_tc": routeA["orig_tc"],
        "dest_en": routeB["dest_en"],
        "dest_tc": routeB["dest_tc"],
        "serviceType": routeA["serviceType"],
        "stops": routeA["stops"] + routeB["stops"],
        "virtual": True,
    }


def getVirtualCircularRoutes(routeList: list[Route], routeNo: str) -> list[Route]:
    """Create virtual circular route variants from a matching pair of one-way routes.

    Important: GTFS may model a circular route as a single entry covering both
    directions, while the operator API exposes it as two separate directional
    entries. This function synthesises both possible circular orderings (A+B
    and B+A) so the DP matcher can try both and pick the better alignment.

    Args:
        routeList: Full list of routes for the operator.
        routeNo: Route number to search for (e.g. ``"46S"``).

    Returns:
        A list of exactly two virtual ``Route`` dicts (both orderings), or an
        empty list if there are not exactly two entries for ``routeNo`` or if
        those entries are missing required fields (``co``, ``serviceType``).

    Example:
        >>> virtuals = getVirtualCircularRoutes(routeList, "46S")
        >>> len(virtuals)
        2
        >>> virtuals[0]["stops"] == routeA["stops"] + routeB["stops"]
        True
        >>> virtuals[1]["stops"] == routeB["stops"] + routeA["stops"]
        True
    """
    indices = []
    for idx, route in enumerate(routeList):
        if route["route"] == routeNo:
            indices.append(idx)
    if len(indices) != 2:
        return []

    routeA = routeList[indices[0]]
    routeB = routeList[indices[1]]
    if "co" not in routeA or "serviceType" not in routeA:
        return []

    return [
        mergeRouteAsCircularRoute(routeA, routeB),
        mergeRouteAsCircularRoute(routeB, routeA),
    ]


def printStopMatches(
    best_match: BestMatch,
    gtfs_stops: dict[str, GtfsStop],
    co_stops: dict[str, CoStop],
    co: str,
) -> None:
    """Print a side-by-side comparison of GTFS and operator stop names for a match.

    Important: This debug helper makes it easy to visually audit the quality
    of a DP alignment by displaying the paired Chinese stop names from both
    data sources alongside the match score and bound direction.

    Args:
        best_match: The best-match 6-tuple from the route-matching loop:
            ``(gtfsId, avgDist, stopIndexPairs, bound, gtfsStopIds, route)``.
        gtfsStops: Full GTFS stop dictionary keyed by stop ID.
        stopList: Operator stop dictionary keyed by stop ID.
        co: Operator code used to look up the stop name inside GTFS data.

    Example output::

        I 1047 0.0
            運輸處              |   ctb
        1  沙田站               |   沙田市中心
        2  大圍站               |   大圍
    """

    gtfsId, avgDist, stopIndexPairs, bound, gtfsStopIds, route = best_match

    stopPair = [
        (gtfsStopIds[gtfsStopIdx], route["stops"][routeStopIdx])
        for gtfsStopIdx, routeStopIdx in stopIndexPairs
    ]
    print(bound, gtfsId, avgDist)
    print("\t|\t".join(["運輸處", co]))
    print(
        "\n".join(
            [
                str(idx + 1)
                + "  "
                + "\t|\t".join(
                    [gtfs_stops[gtfs_id]["stopName"][co], co_stops[stop_id]["name_tc"]]
                )
                for idx, (gtfs_id, stop_id) in enumerate(stopPair)
            ]
        )
    )
    print()


def match_co_routes_with_gtfs(co: str) -> None:
    """Match and enrich one operator's routes with GTFS fares, frequencies, and aligned stops.

    Important: This is the main pipeline step that bridges per-operator crawled
    data with the government GTFS dataset. It:

    1. Loads the operator's route and stop lists from disk.
    2. For each GTFS route belonging to the operator, runs DP stop matching
       against all candidate operator routes (including virtual circular ones).
    3. Attaches ``fares``, ``freq`` (service frequency), ``jt`` (journey time),
       and the aligned ``stops`` subset to the best-matching operator route.
    4. Writes a ``routeFareList.{co}.json`` with every route (matched or not),
       filtering out routes that were only partially matched and carry no fare.

    Special handling:

    - **GMB / ferry operators** (``sunferry``, ``fortuneferry``, ``hkkf``):
      Fare is read directly from GTFS via a pre-existing ``gtfsId`` field on
      each route rather than going through the DP matcher.
    - **MTR**: GTFS-derived candidates are NOT appended to the original route
      list to avoid duplicates; only existing entries are enriched.
    - **HKKF**: Routes are matched by terminal name prefix rather than route
      number because GTFS ferry route numbers differ from HKKF internal codes.

    Args:
        co: Operator code (e.g. ``"kmb"``, ``"ctb"``, ``"gmb"``). Must have
            corresponding ``routeList.{co}.json`` and ``stopList.{co}.json``
            files under ``DATA_DIR``.

    Side effects:
        - Reads ``DATA_DIR/routeList.{co}.json`` and
          ``DATA_DIR/stopList.{co}.json``.
        - Writes ``DATA_DIR/routeFareList.{co}.json``.
        - Mutates the module-level ``gtfsRoutes`` dict by adding a ``_route``
          key to successfully matched entries (consumed later when writing
          ``routeGtfs.all.json``).

    Example:
        >>> matchRoutes("kmb")
        kmb
        kmb 5 out of 732 not match
    """
    print(co)
    with open(DATA_DIR / ("routeList.%s.json" % co), "r", encoding="utf-8") as f:
        co_routes = json.load(f)
    with open(DATA_DIR / ("stopList.%s.json" % co), "r", encoding="utf-8") as f:
        co_stops = json.load(f)

    route_candidates = []
    # one pass to find matches of co vs gtfs by DP
    for gtfs_id, gtfs_route in gtfs_routes.items():
        # "co" of ferry services in gtfs are all "ferry"
        # convert ferry company codes as "ferry" when matching with gtfs
        gtfs_co = co if co not in FERRY_COS else "ferry"
        # skip matching if co not match
        if gtfs_co not in gtfs_route["co"]:
            continue

        debug = False and gtfs_id == "1047" and gtfs_route["orig"]["tc"] == "沙田站"
        if co == "gmb":  # handle for gmb
            for co_route in co_routes:
                if co_route["gtfsId"] == gtfs_id:
                    # it assumes fare of all stops are the same
                    # TODO: inspect the validity of data
                    # there should be sectional fare
                    flat_fare = get_fare(
                        gtfs_route["fares"]["1"], 1, len(gtfs_route["stops"]["1"])
                    )
                    co_route["fares"] = [
                        flat_fare for _ in range(len(co_route["stops"]) - 1)
                    ]
        elif co in ["sunferry", "fortuneferry"]:
            for co_route in co_routes:
                if co_route["gtfsId"] == gtfs_id:
                    # it assumes fare of all stops are the same
                    # TODO: inspect the validity of data
                    flat_fare = get_fare(
                        gtfs_route["fares"]["1"], 1, len(gtfs_route["stops"]["1"])
                    )
                    co_route["fares"] = [
                        flat_fare for _ in range(len(co_route["stops"]) - 1)
                    ]
        # handle for other companies
        else:
            for route_seq, gtfs_route_seq_stops in gtfs_route["stops"].items():
                best_match: BestMatch = ("-1", INFINITY_DIST, [], "", [], {})
                for co_route in co_routes + getVirtualCircularRoutes(
                    co_routes, gtfs_route["route"]
                ):
                    if (
                        co in gtfs_route["co"]
                        and co_route["route"] == gtfs_route["route"]
                    ) or (
                        co == "hkkf"
                        and (
                            (
                                co_route["orig_tc"].startswith(gtfs_route["orig"]["tc"])
                                and co_route["dest_tc"].startswith(
                                    gtfs_route["dest"]["tc"]
                                )
                            )
                            or (
                                co_route["orig_tc"].startswith(gtfs_route["dest"]["tc"])
                                and co_route["dest_tc"].startswith(
                                    gtfs_route["orig"]["tc"]
                                )
                            )
                        )
                    ):
                        ret, avgDist = matchStopsByDp(
                            [co_stops[co_stop_id] for co_stop_id in co_route["stops"]],
                            [
                                gtfs_stops[gtfs_stop_id]
                                for gtfs_stop_id in gtfs_route_seq_stops
                            ],
                            co,
                            debug,
                        )
                        best_match_avgDist = best_match[1]
                        if avgDist < best_match_avgDist:
                            best_match = (
                                gtfs_id,
                                avgDist,
                                ret,
                                route_seq,
                                gtfs_route_seq_stops,
                                co_route,
                            )

                # assume matching to be avg stop distance diff is lower than 100

                best_match_avgDist = best_match[1]
                if best_match_avgDist < DIST_DIFF:
                    _, _, ret, route_seq, gtfs_route_seq_stops, co_route = best_match

                    route_candidate = co_route.copy()
                    if (
                        (
                            len(ret) == len(co_route["stops"])
                            or len(ret) + 1 == len(co_route["stops"])
                        )
                        and "gtfs" not in co_route
                        and "virtual" not in co_route
                    ):
                        _fare_csv = gtfs_route["fares"].get(route_seq, "")
                        route_candidate["fares"] = (
                            [
                                get_fare(_fare_csv, i + 1, ret[-1][0] + 1)
                                for i, _ in ret[:-1]
                            ]
                            if _fare_csv
                            else None
                        )
                        route_candidate["freq"] = gtfs_route["freq"][route_seq]
                        route_candidate["jt"] = gtfs_route["jt"]
                        route_candidate["co"] = (
                            gtfs_route["co"]
                            if co in gtfs_route["co"]
                            else (gtfs_route["co"] + [co])
                        )
                        route_candidate["stops"] = [
                            co_route["stops"][j] for i, j in ret
                        ]
                        route_candidate["gtfs"] = [gtfs_id]
                        co_route["found"] = True
                    else:
                        route_candidate["stops"] = [
                            co_route["stops"][j] for i, j in ret
                        ]
                        _fare_csv = gtfs_route["fares"].get(route_seq, "")
                        route_candidate["fares"] = (
                            [
                                get_fare(_fare_csv, i + 1, ret[-1][0] + 1)
                                for i, _ in ret[:-1]
                            ]
                            if _fare_csv
                            else None
                        )
                        route_candidate["freq"] = gtfs_route["freq"][route_seq]
                        route_candidate["jt"] = gtfs_route["jt"]
                        route_candidate["co"] = gtfs_route["co"]
                        route_candidate["orig_tc"] = co_stops[
                            route_candidate["stops"][0]
                        ]["name_tc"]
                        route_candidate["orig_en"] = co_stops[
                            route_candidate["stops"][0]
                        ]["name_en"]
                        route_candidate["dest_tc"] = co_stops[
                            route_candidate["stops"][-1]
                        ]["name_tc"]
                        route_candidate["dest_en"] = co_stops[
                            route_candidate["stops"][-1]
                        ]["name_en"]
                        route_candidate["service_type"] = (
                            "2" if "found" in co_route else "1"
                        )
                        route_candidate["gtfs"] = [gtfs_id]
                        # mark the route has mapped to GTFS, mainly for ctb routes
                        co_route["found"] = True
                    route_candidates.append(route_candidate)
                    if "_route" not in gtfs_route:
                        gtfs_route["_route"] = {}
                    gtfs_route["_route"][co] = co_route.copy()
                elif co in gtfs_route["co"]:
                    print(
                        co,
                        gtfs_route["route"],
                        "cannot match any in GTFS",
                        file=sys.stderr,
                    )

    for co_route in co_routes:
        if "gtfs" not in co_route:
            co_route["co"] = [co]

    print(
        co,
        len([route for route in co_routes if "gtfs" not in route]),
        "out of",
        len(co_routes),
        "not match",
    )
    if co != "mtr":
        co_routes.extend(route_candidates)
    # skipping routes that just partially mapped to GTFS
    co_routes = [
        route for route in co_routes if "found" not in route or "fares" in route
    ]

    with open(DATA_DIR / ("routeFareList.%s.json" % co), "w", encoding="UTF-8") as f:
        f.write(json.dumps(co_routes, ensure_ascii=False))


match_co_routes_with_gtfs("kmb")
match_co_routes_with_gtfs("ctb")
match_co_routes_with_gtfs("nlb")
match_co_routes_with_gtfs("lrtfeeder")
match_co_routes_with_gtfs("gmb")
match_co_routes_with_gtfs("lightRail")
match_co_routes_with_gtfs("mtr")
match_co_routes_with_gtfs("sunferry")
match_co_routes_with_gtfs("fortuneferry")
match_co_routes_with_gtfs("hkkf")

"""
for routeId, route in gtfsRoutes.items():
  if '_route' not in route and route['co'][0] in ['ctb', 'kmb', 'nlb']:
    print(routeId + ': ' + route['route'] + " " + route['orig']['zh'] + ' - ' + route['dest']['zh'] + ' not found')
"""

routeFareList = {}


with open(DATA_DIR / "routeGtfs.all.json", "w", encoding="UTF-8") as f:
    f.write(json.dumps(gtfs_routes, ensure_ascii=False))
