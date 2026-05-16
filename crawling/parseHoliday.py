import asyncio
import json
import logging
from os import path

import httpx
from utils import DATA_DIR

logger = logging.getLogger(__name__)


async def main():
    if not path.isfile(DATA_DIR / "holidays.json"):
        if path.isfile(DATA_DIR / "holiday.json"):
            with open(DATA_DIR / "holiday.json") as f:
                holidays = json.load(f)
        else:
            async with httpx.AsyncClient() as a_client:
                r = await a_client.get("https://www.1823.gov.hk/common/ical/tc.json")
                data = r.json()
            holidays = [
                holiday["dtstart"][0] for holiday in data["vcalendar"][0]["vevent"]
            ]
        with open(DATA_DIR / "holidays.json", "w") as f:
            json.dump(holidays, f, ensure_ascii=False)
        logger.info("Created holidays.json")
    else:
        logger.info("holidays.json already exists, download skipped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
