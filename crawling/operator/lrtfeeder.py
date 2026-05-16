from .base_operator import BaseOperator


class LRTFeeder(BaseOperator):
    code = "lrtfeeder"

    @classmethod
    def route_key(cls, route):
        return cls.key(
            cls.code,
            cls.route_no(route),
            cls.service_type(route),
            cls.gtfs_route_id(route),
            cls.gtfs_route_seq(route),
        )
