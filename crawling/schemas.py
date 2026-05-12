from __future__ import annotations

from typing import Literal, NotRequired, TypedDict

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
    dest_en: str
    dest_tc: str
    stops: list[str]
    serviceType: NotRequired[int | str]
    gtfsId: NotRequired[str]


class ProviderStop(TypedDict):
    """Common stop entry written to data/stopList.{co}.json."""

    stop: str
    name_en: str
    name_tc: str
    lat: float | str
    long: float | str
