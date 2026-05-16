from .base_operator import BaseOperator


class KMB(BaseOperator):
    code = "kmb"

    @classmethod
    def route_key(cls, route):
        return cls.key(
            cls.code, cls.route_no(route), cls.bound(route), cls.service_type(route)
        )
