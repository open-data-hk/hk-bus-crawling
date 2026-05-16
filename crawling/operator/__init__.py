from _operator import *

from .base_operator import BaseOperator
from .ctb import CTB
from .fortuneferry import FortuneFerry
from .gmb import GMB
from .hkkf import HKKF
from .kmb import KMB
from .light_rail import LightRail
from .lrtfeeder import LRTFeeder
from .lwb import LWB
from .mtr import MTR
from .nlb import NLB
from .sunferry import SunFerry

OPERATOR_CLASSES: dict[str, type[BaseOperator]] = {
    "ctb": CTB,
    "fortuneferry": FortuneFerry,
    "gmb": GMB,
    "hkkf": HKKF,
    "kmb": KMB,
    "lightRail": LightRail,
    "lrtfeeder": LRTFeeder,
    "lwb": LWB,
    "mtr": MTR,
    "nlb": NLB,
    "sunferry": SunFerry,
}


def get_operator_class(co: str | None) -> type[BaseOperator] | None:
    if co is None:
        return None
    return OPERATOR_CLASSES.get(co)
