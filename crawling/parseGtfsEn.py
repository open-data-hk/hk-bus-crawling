import asyncio
import logging

from parseGtfs import parseGtfs

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(parseGtfs("en"))
