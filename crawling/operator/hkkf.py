from .base_operator import BaseOperator


class HKKF(BaseOperator):
    code = "hkkf"

    @classmethod
    def route_key(cls, route):
        return cls.key(cls.code, cls.route_no(route), cls.bound(route))
