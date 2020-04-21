import pytest
from test.example_step import EXAMPLE_STEP
from src.emr_to_livy import from_dash_to_camel_case, extract_conf_until_jar


def test_from_dash_to_camel_case():
    assert from_dash_to_camel_case('--conf') == 'conf'
    assert from_dash_to_camel_case('--executor-memory') == 'executorMemory'
    with pytest.raises(ValueError):
        from_dash_to_camel_case('foobar')


def test_extract_conf_until_jar():
    cli_args = EXAMPLE_STEP['HadoopJarStep']['Args']
    assert extract_conf_until_jar({}, cli_args[1:]) == {
        'deployMode': 'cluster',
        'master': 'yarn',
        'class': 'com.company.org.Jar',
        'name': 'test',
        'numExecutors': '256',
        'driverMemory': '4G',
        'executorMemory': '30G',
        'file': 's3://example-bucket/artifacts/jar-with-dependencies.jar',
        'conf': {
            'spark.driver.cores': '1',
            'spark.yarn.maxAppAttempts': '1'
        },
        'args': [
            '--output-path=s3://ccpa/delete',
            '--partitions=512',
            '--final-output-concurrency=256',
            '--graphite-port=2003',
            '--max-age-in-days=180',
        ]
    }
