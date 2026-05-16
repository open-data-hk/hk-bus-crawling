import json

import typer


def main(route_fare_list_json: str):
    """
    Simple tool to normalize JSON for easier comparison. The normalized JSON will be written to the same directory with `.norm` added.
    """
    normalized_json_name = f"{route_fare_list_json}.norm"
    with open(route_fare_list_json) as f:
        source_json = json.load(f)
    if isinstance(source_json, dict) and "holidays" in source_json:
        source_json["holidays"] = sorted(source_json["holidays"])

    with open(normalized_json_name, "w") as f:
        json.dump(source_json, f, sort_keys=True, ensure_ascii=False)


if __name__ == "__main__":
    typer.run(main)
