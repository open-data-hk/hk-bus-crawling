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
    gtfsRoutes = gtfs["routeList"]
    gtfsStops = gtfs["stopList"]


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
BestMatchTuple = tuple[str, float, list[StopMatch], str, list[str], Route]

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
    coStops: list[CoStop],
    gtfsStops: list[GtfsStop],
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
    if len(gtfsStops) > len(coStops) + 1:
        return [], INFINITY_DIST
    if len(gtfsStops) - len(coStops) == 1:
        gtfsStops = gtfsStops[:-1]

    # initialization
    distSum = [
        [INFINITY_DIST for x in range(len(coStops) + 1)]
        for y in range(len(gtfsStops) + 1)
    ]
    for j in range(len(coStops) - len(gtfsStops) + 1):
        distSum[0][j] = 0

    # Perform DP
    for i in range(len(gtfsStops)):
        gtfsStop = gtfsStops[i]
        for j in range(len(coStops)):
            coStop = coStops[j]
            dist = (
                0
                if coStop["name_tc"] == gtfsStop["stopName"][co]["tc"]
                else haversine(
                    (float(coStop["lat"]), float(coStop["long"])),
                    (gtfsStop["lat"], gtfsStop["lng"]),
                )
                * 1000
            )

            distSum[i + 1][j + 1] = min(
                distSum[i][j] + dist,  # from previous stops of both sides
                distSum[i + 1][j],  # skipping current coStops
            )

    # fast return if no good result
    if not min(distSum[len(gtfsStops)]) / len(gtfsStops) < DIST_DIFF:
        return [], INFINITY_DIST

    # backtracking
    i = len(gtfsStops)
    j = len(coStops)
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

    return ret, min(distSum[len(gtfsStops)]) / len(gtfsStops) + penalty


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
    bestMatch: BestMatchTuple,
    gtfsStops: dict[str, GtfsStop],
    stopList: dict[str, CoStop],
    co: str,
) -> None:
    """Print a side-by-side comparison of GTFS and operator stop names for a match.

    Important: This debug helper makes it easy to visually audit the quality
    of a DP alignment by displaying the paired Chinese stop names from both
    data sources alongside the match score and bound direction.

    Args:
        bestMatch: The best-match 6-tuple from the route-matching loop:
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
    stopPair = [
        (bestMatch[4][gtfsStopIdx], bestMatch[5]["stops"][routeStopIdx])
        for gtfsStopIdx, routeStopIdx in bestMatch[2]
    ]
    print(bestMatch[3], bestMatch[0], bestMatch[1])
    print("\t|\t".join(["運輸處", co]))
    print(
        "\n".join(
            [
                str(idx + 1)
                + "  "
                + "\t|\t".join(
                    [gtfsStops[gtfsId]["stopName"][co], stopList[stopId]["name_tc"]]
                )
                for idx, (gtfsId, stopId) in enumerate(stopPair)
            ]
        )
    )
    print()


def matchRoutes(co: str) -> None:
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
        routeList = json.load(f)
    with open(DATA_DIR / ("stopList.%s.json" % co), "r", encoding="utf-8") as f:
        stopList = json.load(f)

    routeCandidates = []
    # one pass to find matches of co vs gtfs by DP
    for gtfsId, gtfsRoute in gtfsRoutes.items():
        debug = False and gtfsId == "1047" and gtfsRoute["orig"]["tc"] == "沙田站"
        if co == "gmb" and co in gtfsRoute["co"]:  # handle for gmb
            for route in routeList:
                if route["gtfsId"] == gtfsId:
                    # it assumes fare of all stops are the same
                    # TODO: inspect the validity of data
                    # there should be sectional fare
                    flat_fare = get_fare(
                        gtfsRoute["fares"]["1"], 1, len(gtfsRoute["stops"]["1"])
                    )
                    route["fares"] = [flat_fare for _ in range(len(route["stops"]) - 1)]
        elif (co == "sunferry" or co == "fortuneferry") and "ferry" in gtfsRoute["co"]:
            for route in routeList:
                if route["gtfsId"] == gtfsId:
                    # it assumes fare of all stops are the same
                    # TODO: inspect the validity of data
                    flat_fare = get_fare(
                        gtfsRoute["fares"]["1"], 1, len(gtfsRoute["stops"]["1"])
                    )
                    route["fares"] = [flat_fare for _ in range(len(route["stops"]) - 1)]
        # handle for other companies
        elif co in gtfsRoute["co"] or (co == "hkkf" and "ferry" in gtfsRoute["co"]):
            for bound, stops in gtfsRoute["stops"].items():
                bestMatch: Any = (-1, INFINITY_DIST)
                for route in routeList + getVirtualCircularRoutes(
                    routeList, gtfsRoute["route"]
                ):
                    if (
                        co in gtfsRoute["co"] and route["route"] == gtfsRoute["route"]
                    ) or (
                        co == "hkkf"
                        and (
                            (
                                route["orig_tc"].startswith(gtfsRoute["orig"]["tc"])
                                and route["dest_tc"].startswith(gtfsRoute["dest"]["tc"])
                            )
                            or (
                                route["orig_tc"].startswith(gtfsRoute["dest"]["tc"])
                                and route["dest_tc"].startswith(gtfsRoute["orig"]["tc"])
                            )
                        )
                    ):
                        ret, avgDist = matchStopsByDp(
                            [stopList[stop] for stop in route["stops"]],
                            [gtfsStops[stop] for stop in stops],
                            co,
                            debug,
                        )
                        if avgDist < bestMatch[1]:
                            bestMatch = (gtfsId, avgDist, ret, bound, stops, route)

                # assume matching to be avg stop distance diff is lower than 100
                if bestMatch[1] < DIST_DIFF:
                    ret, bound, stops, route = bestMatch[2:]

                    routeCandidate = route.copy()
                    if (
                        (
                            len(ret) == len(route["stops"])
                            or len(ret) + 1 == len(route["stops"])
                        )
                        and "gtfs" not in route
                        and "virtual" not in route
                    ):
                        _fare_csv = gtfsRoute["fares"].get(bound, "")
                        routeCandidate["fares"] = (
                            [
                                get_fare(_fare_csv, i + 1, ret[-1][0] + 1)
                                for i, _ in ret[:-1]
                            ]
                            if _fare_csv
                            else None
                        )
                        routeCandidate["freq"] = gtfsRoute["freq"][bound]
                        routeCandidate["jt"] = gtfsRoute["jt"]
                        routeCandidate["co"] = (
                            gtfsRoute["co"]
                            if co in gtfsRoute["co"]
                            else (gtfsRoute["co"] + [co])
                        )
                        routeCandidate["stops"] = [route["stops"][j] for i, j in ret]
                        routeCandidate["gtfs"] = [gtfsId]
                        route["found"] = True
                    else:
                        routeCandidate["stops"] = [route["stops"][j] for i, j in ret]
                        _fare_csv = gtfsRoute["fares"].get(bound, "")
                        routeCandidate["fares"] = (
                            [
                                get_fare(_fare_csv, i + 1, ret[-1][0] + 1)
                                for i, _ in ret[:-1]
                            ]
                            if _fare_csv
                            else None
                        )
                        routeCandidate["freq"] = gtfsRoute["freq"][bound]
                        routeCandidate["jt"] = gtfsRoute["jt"]
                        routeCandidate["co"] = gtfsRoute["co"]
                        routeCandidate["orig_tc"] = stopList[
                            routeCandidate["stops"][0]
                        ]["name_tc"]
                        routeCandidate["orig_en"] = stopList[
                            routeCandidate["stops"][0]
                        ]["name_en"]
                        routeCandidate["dest_tc"] = stopList[
                            routeCandidate["stops"][-1]
                        ]["name_tc"]
                        routeCandidate["dest_en"] = stopList[
                            routeCandidate["stops"][-1]
                        ]["name_en"]
                        routeCandidate["service_type"] = (
                            "2" if "found" in route else "1"
                        )
                        routeCandidate["gtfs"] = [gtfsId]
                        # mark the route has mapped to GTFS, mainly for ctb routes
                        route["found"] = True
                    routeCandidates.append(routeCandidate)
                    if "_route" not in gtfsRoute:
                        gtfsRoute["_route"] = {}
                    gtfsRoute["_route"][co] = route.copy()
                elif co in gtfsRoute["co"]:
                    print(
                        co,
                        gtfsRoute["route"],
                        "cannot match any in GTFS",
                        file=sys.stderr,
                    )

    for route in routeList:
        if "gtfs" not in route:
            route["co"] = [co]

    print(
        co,
        len([route for route in routeList if "gtfs" not in route]),
        "out of",
        len(routeList),
        "not match",
    )
    if co != "mtr":
        routeList.extend(routeCandidates)
    # skipping routes that just partially mapped to GTFS
    routeList = [
        route for route in routeList if "found" not in route or "fares" in route
    ]

    with open(DATA_DIR / ("routeFareList.%s.json" % co), "w", encoding="UTF-8") as f:
        f.write(json.dumps(routeList, ensure_ascii=False))


matchRoutes("kmb")
matchRoutes("ctb")
matchRoutes("nlb")
matchRoutes("lrtfeeder")
matchRoutes("gmb")
matchRoutes("lightRail")
matchRoutes("mtr")
matchRoutes("sunferry")
matchRoutes("fortuneferry")
matchRoutes("hkkf")

"""
for routeId, route in gtfsRoutes.items():
  if '_route' not in route and route['co'][0] in ['ctb', 'kmb', 'nlb']:
    print(routeId + ': ' + route['route'] + " " + route['orig']['zh'] + ' - ' + route['dest']['zh'] + ' not found')
"""

routeFareList = {}


with open(DATA_DIR / "routeGtfs.all.json", "w", encoding="UTF-8") as f:
    f.write(json.dumps(gtfsRoutes, ensure_ascii=False))
