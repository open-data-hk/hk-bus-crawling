from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

# TODO: create once only
DATA_DIR.mkdir(exist_ok=True)
