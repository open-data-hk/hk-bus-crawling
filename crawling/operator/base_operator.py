class BaseOperator:
    code = ""

    @classmethod
    def route_key(cls, route) -> str:
        return ""

    @classmethod
    def key(cls, *values) -> str:
        return "|".join(str("" if value is None else value) for value in values)

    @classmethod
    def route_no(cls, route):
        return route.get("route_no") or route.get("route")

    @classmethod
    def bound(cls, route):
        return route.get("bound")

    @classmethod
    def service_type(cls, route):
        return route.get("service_type", "1")

    @classmethod
    def provider_route_id(cls, route):
        return route.get("id")

    @classmethod
    def gtfs_route_id(cls, route):
        gtfs_route_id = route.get("gtfs_route_id")
        if gtfs_route_id:
            return gtfs_route_id

        gtfs_route_ids = route.get("gtfs")
        if isinstance(gtfs_route_ids, list) and gtfs_route_ids:
            return gtfs_route_ids[0]

        return None

    @classmethod
    def gtfs_route_seq(cls, route):
        return route.get("gtfs_route_seq")
