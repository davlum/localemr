from test.fixtures.example_steps import EXAMPLE_STEP
import pytest
from localemr.common import (
    from_dash_to_snake_case,
    extract_conf_until_jar,
    EmrRequest,
    EmrRequestConfig,
    from_snake_to_camel_case,
    get_emr_version,
    transform_emr_req_to_cli_args,
    LocalFakeStep,
)


def test_from_dash_to_snake_case():
    assert from_dash_to_snake_case('--conf') == 'conf'
    assert from_dash_to_snake_case('--executor-memory') == 'executor_memory'
    with pytest.raises(ValueError):
        from_dash_to_snake_case('foobar')


def test_from_snake_to_camel_case():
    assert from_snake_to_camel_case('executor_memory') == 'executorMemory'
    assert from_snake_to_camel_case('a_beautiful_day') == 'aBeautifulDay'


def test_extract_conf_until_jar():
    fake_step = LocalFakeStep(
        state='',
        hostname='',
        cluster_id='',
        cluster_name='',
        args=EXAMPLE_STEP['HadoopJarStep']['Args'],
        jar=EXAMPLE_STEP['HadoopJarStep']['Jar'],
    )
    assert extract_conf_until_jar(fake_step).to_dict() == EmrRequest(
        command='spark-submit',
        args=[
            '--output-path=/ccpa/delete',
            '--partitions=512',
            '--final-output-concurrency=256',
            '--graphite-port=2003',
            '--max-age-in-days=180',
        ],
        emr_request_config=EmrRequestConfig(
            name='test',
            class_name='com.company.org.Jar',

        ),
        file='/tmp/example-bucket/artifacts/jar-with-dependencies.jar',
        conf={'spark.driver.cores': '1', 'spark.yarn.maxAppAttempts': '1'},
    ).to_dict()


def test_transform_emr_req_to_cli_args():
    data = EmrRequest(
        command='spark-submit',
        args=[
            '--output-path=/ccpa/delete',
            '--partitions=512',
            '--final-output-concurrency=256',
            '--graphite-port=2003',
            '--max-age-in-days=180',
        ],
        emr_request_config=EmrRequestConfig(
            name='test',
            class_name='com.company.org.Jar',

        ),
        file='/tmp/example-bucket/artifacts/jar-with-dependencies.jar',
        conf={'spark.driver.cores': '1', 'spark.yarn.maxAppAttempts': '1'},
    )
    result = transform_emr_req_to_cli_args(data)
    assert result == [
        'spark-submit',
        '--class',
        'com.company.org.Jar',
        '--name',
        'test',
        '--conf',
        'spark.yarn.maxAppAttempts=1',
        '/tmp/example-bucket/artifacts/jar-with-dependencies.jar',
        '--output-path=/ccpa/delete',
        '--partitions=512',
        '--final-output-concurrency=256',
        '--graphite-port=2003',
        '--max-age-in-days=180'
    ]


def test_get_emr_version():
    assert get_emr_version('emr-5.26.0') == '5.25.0'
    assert get_emr_version('emr-0.0.0') == '5.0.0'
    assert get_emr_version('emr-7.0.0') == '6.0.0'
