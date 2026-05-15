from __future__ import annotations

from typing import Any, Literal, NotRequired, TypedDict

ProviderCode = Literal[
    "ctb",
    "fortuneferry",
    "gmb",
    "hkkf",
    "kmb",
    "lightRail",
    "lrtfeeder",
    "mtr",
    "nlb",
    "sunferry",
]

RouteBound = Literal["I", "O"]


class ProviderRoute(TypedDict):
    """Common route entry written to data/routeList.{co}.json."""

    co: ProviderCode | str
    route: str
    bound: RouteBound | str
    orig_en: str
    orig_tc: str
    orig_sc: str
    dest_en: str
    dest_tc: str
    dest_sc: str
    stops: list[str]
    id: NotRequired[str]
    service_type: NotRequired[int | str]
    gtfs_route_id: NotRequired[str]
    gtfs_route_seq: NotRequired[str]
    freq: NotRequired[dict[str, Any]]
    fares: NotRequired[str | list[str | float]]
    faresHoliday: NotRequired[list[str | float]]


class ProviderStop(TypedDict):
    """Common stop entry written to data/stopList.{co}.json."""

    stop: str
    name_en: str
    name_tc: str
    name_sc: str
    lat: float | str
    lng: float | str
