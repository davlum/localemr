import pytest
from test.example_step import EXAMPLE_STEP
from src.convert_s3_to_local import extract_s3_parts, extract_s3_files_from_step, convert_s3_to_local_path

cli_args = EXAMPLE_STEP['HadoopJarStep']['Args']


def test_extract_s3_parts():
    s3_path = 's3://adgear-data/hash/hourly/trader/exchange/2019-10-15/00'
    bucket, key = extract_s3_parts(s3_path)
    assert bucket == 'adgear-data'
    assert key == 'hash/hourly/trader/exchange/2019-10-15/00'
    with pytest.raises(ValueError) as e:
        extract_s3_parts('foobar')
        assert e == "Couldn't extract bucket and key from S3 bucket {}".format(s3_path)


def test_extract_s3_files_from_step():
    assert extract_s3_files_from_step('/tmp/example', cli_args) == [
        ('s3://example-bucket/artifacts/jar-with-dependencies.jar',
         '/tmp/example/example-bucket/artifacts/jar-with-dependencies.jar'),
        ('s3://ccpa/delete', '/tmp/example/ccpa/delete')
    ]


def test_convert_s3_to_local_path():
    assert convert_s3_to_local_path('/tmp/example', cli_args) == [
        '/usr/bin/spark-submit',
        '--deploy-mode',
        'cluster',
        '--master',
        'yarn',
        '--class',
        'com.company.org.Jar',
        '--name',
        'test',
        '--num-executors',
        '256',
        '--driver-memory',
        '4G',
        '--executor-memory',
        '30G',
        '--conf',
        'spark.driver.cores=1',
        '--conf',
        'spark.yarn.maxAppAttempts=1',
        'file:///tmp/example/example-bucket/artifacts/jar-with-dependencies.jar',
        '--output-path=file:///tmp/example/ccpa/delete',
        '--partitions=512',
        '--final-output-concurrency=256',
        '--graphite-port=2003',
        '--max-age-in-days=180',
    ]
