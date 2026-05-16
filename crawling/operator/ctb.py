from .base_operator import BaseOperator


class CTB(BaseOperator):
    code = "ctb"

    @classmethod
    def route_key(cls, route):
        return cls.key(
            cls.code,
            cls.route_no(route),
            cls.bound(route),
            cls.gtfs_route_id(route),
            cls.gtfs_route_seq(route),
        )
