import json

import requests
from utils import DATA_DIR

with open(DATA_DIR / "integrated_routes.json") as f:
    new_route_list = json.load(f)

r = requests.get(
    "https://open-data-hk.github.io/hk-bus-crawling/integrated_routes.json"
)
old_route_list = r.json()

for newKey in new_route_list:
    if newKey not in old_route_list:
        print("new " + newKey)

for oldKey in old_route_list:
    if oldKey not in new_route_list:
        print("old " + oldKey)
