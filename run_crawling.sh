#!/bin/bash
set -e

python ./crawling/parseHoliday.py
python ./crawling/ctb_crawl.py
python ./crawling/ctb.py
python ./crawling/kmb.py
python ./crawling/nlb_crawl.py
python ./crawling/nlb.py
python ./crawling/lrtfeeder.py
python ./crawling/lightRail.py
python ./crawling/mtr.py
python ./crawling/parseJourneyTime.py
python ./crawling/parseGtfs.py
python ./crawling/sunferry.py
python ./crawling/fortuneferry.py
python ./crawling/hkkf.py
python ./crawling/gmb_crawl.py
python ./crawling/gmb.py
python ./crawling/matchGtfs.py
python ./crawling/cleansing.py
python ./crawling/mergeRoutes.py

# for consistency in GitHub Actions
python ./crawling/mergeStopList.py
python ./crawling/routeCompare.py
python ./crawling/mtrExits.py
