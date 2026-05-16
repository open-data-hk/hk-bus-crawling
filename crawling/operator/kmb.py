from .base_operator import BaseOperator


class KMB(BaseOperator):
    @classmethod
    def route_key(cls, route):
        return super().route_key(route)
