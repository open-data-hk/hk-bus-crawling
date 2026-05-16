import json

from route_fare_key import build_route_fare_dict
from utils import DATA_DIR

EXPORTED_CLEANSED_ROUTE_FARE_KEYS: dict[str, str] = {}


def is_name_match(name_a: str, name_b: str) -> bool:
    """Match two string by checking its lowercase is substring of each other"""
    lower_a = name_a.lower()
    lower_b = name_b.lower()
    return lower_a in lower_b or lower_b in lower_a


def _time_to_min(t: str) -> int:
    """e.g. 0730xxxxx = 07 x 60 + 30 = 450"""
    return int(t[:2]) * 60 + int(t[2:4])


def count_services(freq: dict[str, str] | None) -> int:
    """
    Calculate total number of service with provided freq dict (value is compressed freq string)
    """
    if freq is None:
        return 0
    total = 0
    for freq_str in freq.values():
        start_freq_str = freq_str.split("|")
        for i, token in enumerate(start_freq_str):
            if "," in token:
                start, freq_mins = token.split(",")
                if i + 1 < len(start_freq_str):
                    end = start_freq_str[i + 1].split(",")[0]
                    total += (_time_to_min(end) - _time_to_min(start)) / int(freq_mins)
            elif i == 0 or "," not in start_freq_str[i - 1]:
                total += 1
    return total


def cleansing(co):
    with open(DATA_DIR / ("routeFareList.%s.json" % co), "r", encoding="UTF-8") as f:
        route_dict = json.load(f)
    routeList = list(route_dict.values())

    for i, route_i in enumerate(routeList):
        route_i["co"] = [co for co in route_i["co"] if co != "ferry"]
        if "skip" in route_i or "freq" in route_i:
            continue
        bestIdx, maxBus = -1, 0
        for j, route_j in enumerate(routeList):
            if i == j:
                continue
            if (
                route_i["route"] == route_j["route"]
                and sorted(route_i["co"]) == sorted(route_j["co"])
                and is_name_match(route_i["orig_en"], route_j["orig_en"])
                and is_name_match(route_i["dest_en"], route_j["dest_en"])
            ):
                # TODO: check operator routes has "freq" set, now it always skip as "freq" not found
                if "freq" not in route_j:
                    continue
                bus = count_services(route_j["freq"])
                if bus > maxBus:
                    bestIdx = j
                    maxBus = bus
        if bestIdx != -1:
            routeList[bestIdx]["service_type"] = (
                1
                if "service_type" not in route_i
                else routeList[bestIdx]["service_type"]
            )
            if (
                len(route_i["stops"]) <= 0
                or route_i["stops"] == routeList[bestIdx]["stops"]
            ):
                route_i["skip"] = True

    cleansed_route_list = [route for route in routeList if "skip" not in route]
    cleansed_route_dict = build_route_fare_dict(
        cleansed_route_list,
        exported_route_keys=EXPORTED_CLEANSED_ROUTE_FARE_KEYS,
        source=co,
    )
    print(co, len(route_dict), len(cleansed_route_dict))

    with open(
        DATA_DIR / ("routeFareList.%s.cleansed.json" % co), "w", encoding="UTF-8"
    ) as f:
        f.write(json.dumps(cleansed_route_dict, ensure_ascii=False))


cleansing("kmb")
cleansing("lwb")
cleansing("ctb")
cleansing("nlb")
cleansing("lrtfeeder")
cleansing("gmb")
cleansing("lightRail")
cleansing("mtr")
cleansing("sunferry")
cleansing("fortuneferry")
cleansing("hkkf")
