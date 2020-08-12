from localemr.common import get_emr_version


def test_get_emr_version():
    assert get_emr_version('emr-5.26.0') == '5.25.0'
    assert get_emr_version('emr-0.0.0') == '5.0.0'
    assert get_emr_version('emr-7.0.0') == '6.0.0'
