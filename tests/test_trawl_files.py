import pytest

from echodataflow.utils.trawl_files import get_valid_hauls


def test_get_valid_hauls_returns_complete_file_mapping():
    bio_filenames = {
        "length": ["bucket/LengthFreq/001_LFdata.xlsx", "bucket/LengthFreq/002_LFdata.xlsx"],
        "specimen": ["bucket/Specimens/001_specimens.xlsx"],
        "catch": ["bucket/Catch/001_CatchPerc.xlsx"],
        "info": ["bucket/NetConfig/202506_001_NetConfig.xlsx"],
    }

    assert get_valid_hauls(bio_filenames) == {
        1: {
            "length": "bucket/LengthFreq/001_LFdata.xlsx",
            "specimen": "bucket/Specimens/001_specimens.xlsx",
            "catch": "bucket/Catch/001_CatchPerc.xlsx",
            "info": "bucket/NetConfig/202506_001_NetConfig.xlsx",
        }
    }


def test_get_valid_hauls_rejects_duplicate_file_type_for_haul():
    with pytest.raises(ValueError, match="Multiple 'length' files found for haul 001"):
        get_valid_hauls(
            {
                "length": ["first/001_LFdata.xlsx", "second/001_LFdata.xlsx"],
                "specimen": ["001_specimens.xlsx"],
            }
        )


def test_get_valid_hauls_handles_empty_inventory():
    assert get_valid_hauls({}) == {}
