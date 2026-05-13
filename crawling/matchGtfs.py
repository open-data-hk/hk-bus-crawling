from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, TypedDict

from haversine import haversine
from utils import DATA_DIR

INFINITY_DIST = 1000000
DIST_DIFF = 600
logger = logging.getLogger(__name__)

with open(DATA_DIR / "gtfs.json", "r", encoding="UTF-8") as f:
    gtfs = json.load(f)
    gtfs_routes = gtfs["routeList"]
    gtfs_stops = gtfs["stopList"]


class CoStop(TypedDict):
    """A bus/ferry stop as returned by an operator's API."""

    name_tc: str
    name_sc: str
    name_en: str
    lat: str
    lng: str


class GtfsStopName(TypedDict):
    tc: str
    sc: str
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
    orig_sc: str
    dest_en: str
    dest_tc: str
    dest_sc: str
    service_type: str
    stops: list[str]
    gtfs_id: str
    virtual: bool
    found: bool
    gtfs: list[str]
    fares: str | None
    freq: dict[str, Any]
    jt: int


# (gtfs_stop_index, co_stop_index) alignment pair produced by the DP backtrack
StopMatch = tuple[int, int]

# Full best-match tuple: (gtfsId, avgDist, stopPairs, bound, gtfsStopIds, route)
BestMatch = tuple[str, float, list[StopMatch], str, list[str], Route]

# GTFS uses "ferry" as the operator code for all ferry companies.
FERRY_COS: set[str] = {"hkkf", "sunferry", "fortuneferry"}

# Operator & route from operator may not be exactly the same as GTFS
# Hardcode a alias map for matching
RouteMatchKey = tuple[str, str]
ROUTE_GTFS_ALIASES: dict[RouteMatchKey, set[RouteMatchKey]] = {
    ("ctb", "61R"): {("ctb", "NR61")},
    ("ctb", "88R"): {("ctb", "NR88")},
    ("lrtfeeder", "K12"): {("kmb", "K12")},
    ("lrtfeeder", "K14"): {("kmb", "K14")},
    ("lrtfeeder", "K17"): {("kmb", "K17")},
    ("lrtfeeder", "K18"): {("kmb", "K18")},
}


def reverse_route_gtfs_aliases(
    aliases: dict[RouteMatchKey, set[RouteMatchKey]],
) -> dict[RouteMatchKey, set[RouteMatchKey]]:
    """Return GTFS route keys to the provider route keys that should own them."""
    reversed_aliases: dict[RouteMatchKey, set[RouteMatchKey]] = {}
    for source_key, gtfs_keys in aliases.items():
        for gtfs_key in gtfs_keys:
            reversed_aliases.setdefault(gtfs_key, set()).add(source_key)
    return reversed_aliases


REVERSED_ROUTE_GTFS_ALIASES = reverse_route_gtfs_aliases(ROUTE_GTFS_ALIASES)

# Routes that are known to exist in operator data but not in the GTFS route set.
# Event-service route lists are keyed by source operator in the data file.
with open(
    Path(__file__).resolve().parent / "event_special_bus_routes.json",
    "r",
    encoding="UTF-8",
) as event_special_bus_routes_file:
    event_special_bus_routes: dict[str, list[str]] = json.load(
        event_special_bus_routes_file
    )


def load_unmatched_co_route_exemptions() -> dict[str, set[str] | None]:
    """Load operator routes that are intentionally absent from GTFS."""
    exemptions: dict[str, set[str] | None] = {
        "mtr": None,
        "lightRail": None,
        "kmb": {"PB1", "PB2", "PB3", "PB4", "PB5"},
    }
    for co_key, routes in event_special_bus_routes.items():
        for co in co_key.split("+"):
            exemptions.setdefault(co, set())
            exempt_routes = exemptions[co]
            if exempt_routes is not None:
                exempt_routes.update(routes)

    # The current KMB feed includes LWB routes, but keep the source list separate.
    kmb_exempt_routes = exemptions["kmb"]
    lwb_exempt_routes = exemptions.get("lwb")
    if kmb_exempt_routes is not None and lwb_exempt_routes is not None:
        kmb_exempt_routes.update(lwb_exempt_routes)
    return exemptions


UNMATCHED_CO_ROUTE_EXEMPTIONS = load_unmatched_co_route_exemptions()

# Routes that are known to exist in GTFS but not in an operator feed.
UNMATCHED_GTFS_ROUTE_EXEMPTIONS: dict[str, set[str] | None] = {}


def get_gtfs_co_for_operator(co: str) -> str:
    """Return the operator code as represented in GTFS."""
    return "ferry" if co in FERRY_COS else co


def get_gtfs_match_keys(co: str, route_no: str) -> set[RouteMatchKey]:
    """Return GTFS operator/route pairs that can represent an operator route."""
    return {
        (get_gtfs_co_for_operator(co), route_no),
        *ROUTE_GTFS_ALIASES.get((co, route_no), set()),
    }


def get_co_route_nos_for_gtfs_route(co: str, gtfs_route: dict[str, Any]) -> set[str]:
    """Return operator route numbers that could match one GTFS route."""
    route_nos: set[str] = set()
    if get_gtfs_co_for_operator(co) in gtfs_route["co"]:
        route_nos.add(gtfs_route["route"])

    for (source_co, source_route_no), aliases in ROUTE_GTFS_ALIASES.items():
        if source_co != co:
            continue
        if any(
            alias_co in gtfs_route["co"] and alias_route_no == gtfs_route["route"]
            for alias_co, alias_route_no in aliases
        ):
            route_nos.add(source_route_no)
    return route_nos


def can_operator_match_gtfs_route(co: str, gtfs_route: dict[str, Any]) -> bool:
    """Return whether an operator may have a route matching this GTFS route."""
    return bool(get_co_route_nos_for_gtfs_route(co, gtfs_route))


def is_same_gtfs_route(co: str, co_route: Route, gtfs_route: dict[str, Any]) -> bool:
    """Return whether an operator route number matches a GTFS route, including aliases."""
    match_keys = get_gtfs_match_keys(co, co_route["route"])
    return any(
        (gtfs_co, gtfs_route["route"]) in match_keys for gtfs_co in gtfs_route["co"]
    )


def get_matching_gtfs_co(
    co: str, co_route: Route, gtfs_route: dict[str, Any]
) -> str | None:
    """Return the GTFS operator code matched by an operator route."""
    match_keys = get_gtfs_match_keys(co, co_route["route"])
    return next(
        (
            gtfs_co
            for gtfs_co in gtfs_route["co"]
            if (gtfs_co, gtfs_route["route"]) in match_keys
        ),
        None,
    )


def is_unmatched_route_exempt(
    co: str, route_no: str, exemptions: dict[str, set[str] | None]
) -> bool:
    """Return whether an unmatched route should be ignored by diagnostics."""
    if co not in exemptions:
        return False
    exempt_routes = exemptions[co]
    return exempt_routes is None or (
        exempt_routes is not None and route_no in exempt_routes
    )


def format_co_route_for_log(co_route: Route) -> str:
    """Format an operator route compactly for unmatched-route diagnostics."""
    bound = co_route.get("bound", "?")
    service_type = co_route.get("service_type", "?")
    return (
        f"{co_route['route']} bound={bound} service_type={service_type} "
        f"{co_route.get('orig_tc', '?')} -> {co_route.get('dest_tc', '?')}"
    )


def mark_gtfs_route_seq_matched(
    gtfs_route: dict[str, Any], route_seq: str, co: str, co_route: Route
) -> None:
    """Mark one GTFS route sequence as matched by an operator route."""
    # _matched_route_seqs and _route are for checking matched route_seq and co_route
    # it should be cleared if routeGtfs.all.json serves users
    gtfs_route.setdefault("_matched_route_seqs", [])
    if route_seq not in gtfs_route["_matched_route_seqs"]:
        gtfs_route["_matched_route_seqs"].append(route_seq)
    # store matched co_route inside gtfs_route["_route"]
    if "_route" not in gtfs_route:
        gtfs_route["_route"] = {}
    gtfs_route["_route"].setdefault(route_seq, {})
    gtfs_route["_route"][route_seq][co] = co_route.copy()


def format_gtfs_route_for_log(
    gtfs_id: str, gtfs_route: dict[str, Any], route_seqs: list[str]
) -> str:
    """Format a GTFS route compactly for unmatched-route diagnostics."""
    route_seq_text = ",".join(route_seqs)
    return (
        f"{gtfs_route['route']} gtfs_id={gtfs_id} seq={route_seq_text} "
        f"{gtfs_route['orig']['tc']} -> {gtfs_route['dest']['tc']}"
    )


def log_unmatched_gtfs_routes() -> None:
    """Log GTFS route sequences that were not matched by any provider."""
    unmatched_gtfs_routes = sorted(
        [
            (
                gtfs_id,
                gtfs_route,
                [
                    route_seq
                    for route_seq in gtfs_route["stops"]
                    if route_seq not in gtfs_route.get("_matched_route_seqs", [])
                ],
            )
            for gtfs_id, gtfs_route in gtfs_routes.items()
            if any(
                route_seq not in gtfs_route.get("_matched_route_seqs", [])
                for route_seq in gtfs_route["stops"]
            )
            and not any(
                is_unmatched_route_exempt(
                    co, gtfs_route["route"], UNMATCHED_GTFS_ROUTE_EXEMPTIONS
                )
                for co in gtfs_route["co"]
            )
        ],
        key=lambda item: (",".join(item[1]["co"]), int(item[0])),
    )
    for gtfs_id, gtfs_route, route_seqs in unmatched_gtfs_routes:
        logger.warning(
            "Unmatched GTFS route: %s %s",
            ",".join(gtfs_route["co"]),
            format_gtfs_route_for_log(gtfs_id, gtfs_route, route_seqs),
        )


# TODO: validate co_route "I" must = gtfs_route "2"
# Now only used in ferry and gmb
def get_route_seq_for_provider_route(
    gtfs_route: dict[str, Any], co_route: Route
) -> str:
    """Pick the GTFS sequence that corresponds to an operator route."""
    if co_route.get("bound") == "I" and "2" in gtfs_route["stops"]:
        return "2"
    return "1"


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
            gtfs_stop_name = gtfs_stop["stopName"].get(co)
            # if chinese name exactly match, score 0
            # else calculate distance of two stops in meter as score
            dist = (
                0
                if gtfs_stop_name is not None
                and co_stop["name_tc"] == gtfs_stop_name["tc"]
                else haversine(
                    (float(co_stop["lat"]), float(co_stop["lng"])),
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
        "orig_sc": routeA["orig_sc"],
        "dest_en": routeB["dest_en"],
        "dest_tc": routeB["dest_tc"],
        "dest_sc": routeB["dest_sc"],
        "service_type": routeA["service_type"],
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
        those entries are missing required fields (``co``, ``service_type``).

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
    if "co" not in routeA or "service_type" not in routeA:
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
      Fare/frequency data is read directly from GTFS via a pre-existing GTFS
      ID field on each route rather than going through the DP matcher.
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
    matched_co_route_ids: set[int] = set()
    # one pass to find matches of co vs gtfs by DP
    for gtfs_id, gtfs_route in gtfs_routes.items():
        # skip matching if this operator cannot map to the GTFS operator/route
        if not can_operator_match_gtfs_route(co, gtfs_route):
            continue

        debug = False and gtfs_id == "1047" and gtfs_route["orig"]["tc"] == "沙田站"
        if co == "gmb":  # handle for gmb
            for co_route in co_routes:
                if co_route["gtfs_id"] == gtfs_id:
                    route_seq = get_route_seq_for_provider_route(gtfs_route, co_route)
                    co_route["fares"] = gtfs_route["fares"].get(route_seq)
                    co_route["freq"] = gtfs_route["freq"].get(route_seq)
                    co_route["jt"] = gtfs_route["jt"]
                    matched_co_route_ids.add(id(co_route))
                    mark_gtfs_route_seq_matched(gtfs_route, route_seq, co, co_route)
        elif co in ["sunferry", "fortuneferry"]:
            for co_route in co_routes:
                if co_route["gtfs_id"] == gtfs_id:
                    route_seq = get_route_seq_for_provider_route(gtfs_route, co_route)
                    co_route["fares"] = gtfs_route["fares"].get(route_seq)
                    co_route["freq"] = gtfs_route["freq"].get(route_seq)
                    co_route["jt"] = gtfs_route["jt"]
                    matched_co_route_ids.add(id(co_route))
                    mark_gtfs_route_seq_matched(gtfs_route, route_seq, co, co_route)
        # handle for other companies
        else:
            for route_seq, gtfs_route_seq_stops in gtfs_route["stops"].items():
                best_match: BestMatch = ("-1", INFINITY_DIST, [], "", [], {})
                virtual_routes = [
                    virtual_route
                    for route_no in get_co_route_nos_for_gtfs_route(co, gtfs_route)
                    for virtual_route in getVirtualCircularRoutes(co_routes, route_no)
                ]
                for co_route in co_routes + virtual_routes:
                    matching_gtfs_co = get_matching_gtfs_co(co, co_route, gtfs_route)
                    if matching_gtfs_co is not None or (
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
                        stop_name_co = matching_gtfs_co if matching_gtfs_co else co
                        ret, avgDist = matchStopsByDp(
                            [co_stops[co_stop_id] for co_stop_id in co_route["stops"]],
                            [
                                gtfs_stops[gtfs_stop_id]
                                for gtfs_stop_id in gtfs_route_seq_stops
                            ],
                            stop_name_co,
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
                        route_candidate["fares"] = _fare_csv if _fare_csv else None
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
                        matched_co_route_ids.add(id(co_route))
                    else:
                        route_candidate["stops"] = [
                            co_route["stops"][j] for i, j in ret
                        ]
                        _fare_csv = gtfs_route["fares"].get(route_seq, "")
                        route_candidate["fares"] = _fare_csv if _fare_csv else None
                        route_candidate["freq"] = gtfs_route["freq"][route_seq]
                        route_candidate["jt"] = gtfs_route["jt"]
                        route_candidate["co"] = gtfs_route["co"]
                        route_candidate["orig_tc"] = co_stops[
                            route_candidate["stops"][0]
                        ]["name_tc"]
                        route_candidate["orig_sc"] = co_stops[
                            route_candidate["stops"][0]
                        ]["name_sc"]
                        route_candidate["orig_en"] = co_stops[
                            route_candidate["stops"][0]
                        ]["name_en"]
                        route_candidate["dest_tc"] = co_stops[
                            route_candidate["stops"][-1]
                        ]["name_tc"]
                        route_candidate["dest_sc"] = co_stops[
                            route_candidate["stops"][-1]
                        ]["name_sc"]
                        route_candidate["dest_en"] = co_stops[
                            route_candidate["stops"][-1]
                        ]["name_en"]
                        route_candidate["service_type"] = (
                            "2" if "found" in co_route else "1"
                        )
                        route_candidate["gtfs"] = [gtfs_id]
                        # mark the route has mapped to GTFS, mainly for ctb routes
                        co_route["found"] = True
                        matched_co_route_ids.add(id(co_route))
                    route_candidates.append(route_candidate)
                    mark_gtfs_route_seq_matched(gtfs_route, route_seq, co, co_route)
                elif co in gtfs_route["co"]:
                    alias_owners = REVERSED_ROUTE_GTFS_ALIASES.get(
                        (co, gtfs_route["route"])
                    )
                    if alias_owners:
                        alias_owner_names = ", ".join(
                            sorted(source_co for source_co, _ in alias_owners)
                        )
                        print(
                            co.upper(),
                            gtfs_route["route"],
                            f"from GTFS not found in operator {co.upper()} routes, "
                            f"but found in {alias_owner_names}",
                            file=sys.stderr,
                        )
                    else:
                        print(
                            co,
                            gtfs_route["route"],
                            "cannot match any in GTFS",
                            file=sys.stderr,
                        )

    unmatched_co_routes = [
        co_route
        for co_route in co_routes
        if id(co_route) not in matched_co_route_ids
        and not is_unmatched_route_exempt(
            co, co_route["route"], UNMATCHED_CO_ROUTE_EXEMPTIONS
        )
    ]
    for co_route in unmatched_co_routes:
        logger.warning(
            "Unmatched operator route: %s %s",
            co,
            format_co_route_for_log(co_route),
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
log_unmatched_gtfs_routes()

"""
for routeId, route in gtfsRoutes.items():
  if '_route' not in route and route['co'][0] in ['ctb', 'kmb', 'nlb']:
    print(routeId + ': ' + route['route'] + " " + route['orig']['zh'] + ' - ' + route['dest']['zh'] + ' not found')
"""

routeFareList = {}


with open(DATA_DIR / "routeGtfs.all.json", "w", encoding="UTF-8") as f:
    f.write(json.dumps(gtfs_routes, ensure_ascii=False))
