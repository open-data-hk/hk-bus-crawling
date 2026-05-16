from .base_operator import BaseOperator
from .kmb import KMB

OPERATOR_CLASSES: dict[str, type[BaseOperator]] = {
    "kmb": KMB,
}


def get_operator_class(co: str | None) -> type[BaseOperator] | None:
    if co is None:
        return None
    return OPERATOR_CLASSES.get(co)
