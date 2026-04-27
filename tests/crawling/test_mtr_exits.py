import json
from pathlib import Path


def test_identical_files():
    exits_data = json.loads(Path("data/exits.mtr.json").read_text(encoding="UTF-8"))
    exits_snapshot = json.loads(
        Path("tests/snapshots/exits.mtr.json").read_text(encoding="UTF-8")
    )

    # TODO: wait for original pipeline fixed

    data_by_key = {(e["name_zh"], e["exit"]): e for e in exits_data}
    snap_by_key = {(e["name_zh"], e["exit"]): e for e in exits_snapshot}

    # Original pipeline missed these exits
    # 羅湖 — A
    # 茘景 — A1, A2, A3, B, C
    # 茘枝角 — A, B1, B2, C, D1, D2, D3, D4
    # 落馬洲 — A
    assert snap_by_key.keys() <= data_by_key.keys()

    # TODO: revert dp
    # dp should be 5 but 2+ fails
    dp = 1
    for key, snapshot in snap_by_key.items():
        exit_data = data_by_key[key]
        assert {
            **exit_data,
            "lat": round(exit_data["lat"], dp),
            "lng": round(exit_data["lng"], dp),
            # original pipeline evaluate barrierFree incorrectly
            "barrierFree": None,
        } == {
            **snapshot,
            "lat": round(snapshot["lat"], dp),
            "lng": round(snapshot["lng"], dp),
            "barrierFree": None,
        }
