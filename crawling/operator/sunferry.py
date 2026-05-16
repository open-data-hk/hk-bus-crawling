from .base_operator import BaseOperator


class SunFerry(BaseOperator):
    code = "sunferry"

    @classmethod
    def route_key(cls, route):
        return cls.key(cls.code, cls.route_no(route))
