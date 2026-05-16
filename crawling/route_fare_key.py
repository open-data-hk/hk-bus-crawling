from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

ROUTE_FARE_KEY_SEPARATOR = "|"
ROUTE_FARE_KEY_LIST_SEPARATOR = ","


def _escape_route_key_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace(
        ROUTE_FARE_KEY_SEPARATOR, f"\\{ROUTE_FARE_KEY_SEPARATOR}"
    )


def _format_route_key_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ROUTE_FARE_KEY_LIST_SEPARATOR.join(
            sorted(_format_route_key_value(item) for item in value)
        )
    return _escape_route_key_value(str(value))


def _get_route_stop_key_values(route: Mapping[str, Any]) -> list[Any]:
    stops = route.get("stops")
    if isinstance(stops, list) and stops:
        return [stops[0], stops[-1], len(stops)]
    return ["", "", ""]


def get_route_unique_key(route: Mapping[str, Any]) -> str:
    gtfs_route_id = route.get("gtfs_route_id")
    if not gtfs_route_id:
        gtfs_route_ids = route.get("gtfs")
        if isinstance(gtfs_route_ids, list) and gtfs_route_ids:
            gtfs_route_id = gtfs_route_ids[0]

    key_values = [
        route.get("co"),
        route.get("route"),
        route.get("bound"),
        route.get("service_type", "1"),
        route.get("orig_en"),
        route.get("dest_en"),
        *_get_route_stop_key_values(route),
        gtfs_route_id,
        route.get("gtfs_route_seq"),
    ]
    return ROUTE_FARE_KEY_SEPARATOR.join(
        _format_route_key_value(value) for value in key_values
    )


def build_route_fare_dict(
    routes: list[dict[str, Any]],
    exported_route_keys: MutableMapping[str, str] | None = None,
    source: str = "",
) -> dict[str, dict[str, Any]]:
    route_fare_dict: dict[str, dict[str, Any]] = {}
    for route in routes:
        route_key = get_route_unique_key(route)
        if route_key in route_fare_dict:
            raise ValueError(f"Duplicate routeFare key in {source}: {route_key}")
        if exported_route_keys is not None and route_key in exported_route_keys:
            previous_source = exported_route_keys[route_key]
            raise ValueError(
                "Duplicate routeFare key globally: "
                f"{route_key} already exported by {previous_source}, now {source}"
            )
        route["route_key"] = route_key
        route_fare_dict[route_key] = route

    if exported_route_keys is not None:
        for route_key in route_fare_dict:
            exported_route_keys[route_key] = source

    return route_fare_dict
