from .base_operator import BaseOperator


class KMB(BaseOperator):
    @classmethod
    def route_key(cls, route):
        route_no = route.get("route")
        service_type = route.get("service_type")
        return f"kmb|{route_no}|{route['bound']}|{service_type}"
