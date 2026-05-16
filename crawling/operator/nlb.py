from .base_operator import BaseOperator


class NLB(BaseOperator):
    code = "nlb"

    @classmethod
    def route_key(cls, route):
        return cls.key(
            cls.code,
            cls.route_no(route),
            cls.provider_route_id(route),
            cls.gtfs_route_id(route),
            cls.gtfs_route_seq(route),
        )
