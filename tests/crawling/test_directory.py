from crawling.utils import DATA_DIR


def test_output_exists():
    """
    Test that the output file exists.
    """
    # only applicable in local env, with files outputted

    output_file = DATA_DIR / "routeFareList.json"
    assert output_file.exists()
