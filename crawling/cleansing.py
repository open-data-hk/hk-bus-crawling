import json

from utils import DATA_DIR


def isNameMatch(name_a, name_b):
    tmp_a = name_a.lower()
    tmp_b = name_b.lower()
    return tmp_a.find(tmp_b) >= 0 or tmp_b.find(tmp_a) >= 0


def _time_to_min(t):
    return int(t[:2]) * 60 + int(t[2:4])


def countBus(freq):
    if freq is None:
        return 0
    total = 0
    for s in freq.values():
        tokens = s.split("|")
        for i, token in enumerate(tokens):
            if "," in token:
                start, headway = token.split(",")
                if i + 1 < len(tokens):
                    end = tokens[i + 1].split(",")[0]
                    total += (_time_to_min(end) - _time_to_min(start)) / int(headway)
            elif i == 0 or "," not in tokens[i - 1]:
                total += 1
    return total


def cleansing(co):
    with open(DATA_DIR / ("routeFareList.%s.json" % co), "r", encoding="UTF-8") as f:
        routeList = json.load(f)

    for i in range(len(routeList)):
        route = routeList[i]
        route["co"] = [co for co in route["co"] if co != "ferry"]
        if "skip" in route or "freq" in route:
            continue
        bestIdx, maxBus = -1, 0
        for j in range(len(routeList)):
            if i == j:
                continue
            _route = routeList[j]
            if (
                route["route"] == _route["route"]
                and sorted(route["co"]) == sorted(_route["co"])
                and isNameMatch(route["orig_en"], _route["orig_en"])
                and isNameMatch(route["dest_en"], _route["dest_en"])
            ):
                if "freq" not in _route:
                    continue
                bus = countBus(_route["freq"])
                if bus > maxBus:
                    bestIdx = j
                    maxBus = bus
        if bestIdx != -1:
            routeList[bestIdx]["service_type"] = (
                1
                if "service_type" not in routeList[i]
                else routeList[bestIdx]["service_type"]
            )
            if (
                len(routeList[i]["stops"]) <= 0
                or routeList[i]["stops"] == routeList[bestIdx]["stops"]
            ):
                routeList[i]["skip"] = True

    _routeList = [route for route in routeList if "skip" not in route]
    print(co, len(routeList), len(_routeList))

    with open(
        DATA_DIR / ("routeFareList.%s.cleansed.json" % co), "w", encoding="UTF-8"
    ) as f:
        f.write(json.dumps(_routeList, ensure_ascii=False))


cleansing("kmb")
cleansing("ctb")
cleansing("nlb")
cleansing("lrtfeeder")
cleansing("gmb")
cleansing("lightRail")
cleansing("mtr")
cleansing("sunferry")
cleansing("fortuneferry")
cleansing("hkkf")
