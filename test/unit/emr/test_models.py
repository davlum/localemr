from localemr.emr.models import MultiProcessing


def test_get_emr_version():
    assert MultiProcessing.get_emr_version('emr-5.26.0') == '5.25.0'
    assert MultiProcessing.get_emr_version('emr-0.0.0') == '5.0.0'
    assert MultiProcessing.get_emr_version('emr-7.0.0') == '6.0.0'
