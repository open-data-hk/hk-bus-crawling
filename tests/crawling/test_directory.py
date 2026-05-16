from crawling.utils import DATA_DIR


def test_output_exists():
    """
    Test that the output files exist.
    """
    # only applicable in local env, with files outputted

    output_files = [
        DATA_DIR / "holidays.json",
        DATA_DIR / "integrated_routes.json",
        DATA_DIR / "service_days.json",
        DATA_DIR / "operators_stops.json",
        DATA_DIR / "nearby_operators_stops.json",
    ]
    assert all(output_file.exists() for output_file in output_files)
