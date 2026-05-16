from .base_operator import BaseOperator


class GMB(BaseOperator):
    code = "gmb"

    @classmethod
    def route_key(cls, route):
        return cls.key(
            cls.code,
            cls.route_no(route),
            cls.gtfs_route_id(route),
            cls.gtfs_route_seq(route),
        )
