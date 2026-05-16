from .base_operator import BaseOperator


class LWB(BaseOperator):
    code = "lwb"

    @classmethod
    def route_key(cls, route):
        return cls.key(
            cls.code, cls.route_no(route), cls.bound(route), cls.service_type(route)
        )
