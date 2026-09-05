# HK Bus Crawling (a.k.a. hk-bus-eta)

[![Python 3.8.8](https://img.shields.io/badge/python-3.8.8-blue.svg)](https://www.python.org/downloads/release/python-388/) ![Data fetching status](https://github.com/hkbus/hk-bus-crawling/actions/workflows/fetch-data.yml/badge.svg) 

This project fetches bus route information for KMB, CTB, NLB, minibus, MTR, and lightrail. The crawled route, stop, service-day, holiday, and nearby-stop data is published as separate JSON files in gh-pages.

## Fetching Transport ETA

The package is a python version for the npm package [hk-bus-eta](https://www.npmjs.com/package/hk-bus-eta).

### Installation
To install the package,

```sh
pip install hk-bus-eta
```

## Usage

**Fetch ETAs of a route**
```python
from hk_bus_eta import HKEta

hketa = HKEta()
etas = hketa.getEtas(route_id = "TCL+1+Hong Kong+Tung Chung", seq=0, language="en")
print (etas)

"""
[{'eta': '2023-09-12T11:43:00+08:00', 'remark': {'zh': '1號月台', 'en': 'Platform 1'}, 'co': 'mtr'}, {'eta': '2023-09-12T11:51:00+08:00', 'remark': {'zh': '1號月台', 'en': 'Platform 1'}, 'co': 'mtr'}, {'eta': '2023-09-12T11:58:00+08:00', 'remark': {'zh': '1號月台', 'en': 'Platform 1'}, 'co': 'mtr'}, {'eta': '2023-09-12T12:05:00+08:00', 'remark': {'zh': '1號月台', 'en': 'Platform 1'}, 'co': 'mtr'}]
"""
```

**List Route IDs**
```python
from hk_bus_eta import HKEta

hketa = new HKEta()
route_ids = list( hketa.route_list.keys() )
print( route_ids )

"""
['1+1+CHUK YUEN ESTATE+STAR FERRY', '1+1+Central (Hong Kong Station Public Transport Interchange)+The Peak (Public Transport Terminus)', '1+1+Felix Villas+Happy Valley (Upper)', '1+1+Happy Valley (Upper)+Felix Villas', '1+1+Kowloon Bay (Telford Gardens)+Sai Kung', '1+1+Mui Wo Ferry Pier+Tai O', '1+1+STAR FERRY+CHUK YUEN ESTATE', '1+1+Sai Kung+Kowloon Bay (Telford Gardens)', '1+1+Tai O+Mui Wo Ferry Pier', '1+1+The Peak (Public Transport Terminus)+Central (Hong Kong Station Public Transport Interchange)']
"""
```


## Data Crawling by yourself

### Usage
Daily fetched JSON is in [gh-pages](https://github.com/open-data-hk/hk-bus-crawling/tree/gh-pages), including [integrated_routes.json](https://open-data-hk.github.io/hk-bus-crawling/integrated_routes.json), [operators_stops.json](https://open-data-hk.github.io/hk-bus-crawling/operators_stops.json), [nearby_operators_stops.json](https://open-data-hk.github.io/hk-bus-crawling/nearby_operators_stops.json), [service_days.json](https://open-data-hk.github.io/hk-bus-crawling/service_days.json), and [holidays.json](https://open-data-hk.github.io/hk-bus-crawling/holidays.json).

### Installation

Use `uv` to install the dependencies to your environment:

```
uv sync
```

### Data Fetching

To fetch data, run the followings,
```
uv run python ./crawling/parseJourneyTime.py
uv run python ./crawling/parseGtfs.py
uv run python ./crawling/parseHoliday.py
uv run python ./crawling/ctb_crawl.py
uv run python ./crawling/ctb.py
uv run python ./crawling/kmb_crawl.py
uv run python ./crawling/kmb.py
uv run python ./crawling/nlb_crawl.py
uv run python ./crawling/nlb.py
uv run python ./crawling/lrtfeeder.py
uv run python ./crawling/lightRail.py
uv run python ./crawling/mtr.py
uv run python ./crawling/sunferry.py
uv run python ./crawling/fortuneferry.py
uv run python ./crawling/hkkf.py
uv run python ./crawling/gmb_crawl.py
uv run python ./crawling/gmb.py
uv run python ./crawling/matchGtfs.py
uv run python ./crawling/cleansing.py
uv run python ./crawling/mergeRoutes.py
```

## Citing 

Please kindly state you are using this app as
`
HK Bus Crawling@2021, https://github.com/hkbus/hk-bus-crawling
`

## Waypoint data

You may refer to the repository [HK Bus WayPoints Crawling](https://github.com/hkbus/route-waypoints)

## Contributors
[ChunLaw](http://github.com/chunlaw/)
