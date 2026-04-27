import io
import json
import logging
import zipfile
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"

# TODO: create once only
DATA_DIR.mkdir(exist_ok=True)


def download_igeocom_geojson():
    """Download iGeoCom.geojson if file not found"""

    file_name = "iGeoCom.geojson"
    file_path = DATA_DIR / "iGeoCom.geojson"

    if file_path.exists():
        logger.info("iGeoCom.geojson exists in data directory")
        return

    logger.info("downloading iGeoCom.geojson")

    url = "https://open.hkmapservice.gov.hk/OpenData/directDownload?productName=iGeoCom&sheetName=iGeoCom&productFormat=GEOJSON"
    response = requests.get(url)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        with zf.open(file_name) as f:
            file_path.write_bytes(f.read())

    logger.info("downloaded iGeoCom.geojson")


def query_igeocom_geojson(
    class_code: str | None = None,
    type_code: str | None = None,
    subcat: str | None = None,
) -> list[dict]:
    download_igeocom_geojson()

    geojson_path = DATA_DIR / "iGeoCom.geojson"

    geojson = json.loads(geojson_path.read_text("utf-8"))
    features = geojson["features"]

    # data dictionary: https://open.hkmapservice.gov.hk/OpenData/opendata_static/resources/resources_iGeoCom_GeoJSON.zip

    if class_code:
        features = [
            feature
            for feature in features
            if feature["properties"]["CLASS"] == class_code
        ]

    if type_code:
        features = [
            feature
            for feature in features
            if feature["properties"]["TYPE"] == type_code
        ]

    if subcat:
        features = [
            feature for feature in features if feature["properties"]["SUBCAT"] == subcat
        ]

    return features
