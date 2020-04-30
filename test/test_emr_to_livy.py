import pytest
from test.example_step import EXAMPLE_STEP
import src.livy.backend as livy
from src.livy.models import LivyRequestBody


def test_from_dash_to_snake_case():
    assert livy.from_dash_to_snake_case('--conf') == 'conf'
    assert livy.from_dash_to_snake_case('--executor-memory') == 'executor_memory'
    with pytest.raises(ValueError):
        livy.from_dash_to_snake_case('foobar')


def test_from_snake_to_camel_case():
    assert LivyRequestBody.from_snake_to_camel_case('executor_memory') == 'executorMemory'
    assert LivyRequestBody.from_snake_to_camel_case('a_beautiful_day') == 'aBeautifulDay'


def test_extract_conf_until_jar():
    cli_args = EXAMPLE_STEP['HadoopJarStep']['Args']
    assert livy.extract_conf_until_jar(cli_args[1:]).to_dict() == LivyRequestBody(
        class_name='com.company.org.Jar',
        name='test',
        num_executors=256,
        driver_memory='4G',
        executor_memory='30G',
        file='s3://example-bucket/artifacts/jar-with-dependencies.jar',
        conf={
            'spark.driver.cores': '1',
            'spark.yarn.maxAppAttempts': '1'
        },
        args= [
            '--output-path=s3://ccpa/delete',
            '--partitions=512',
            '--final-output-concurrency=256',
            '--graphite-port=2003',
            '--max-age-in-days=180',
        ]
    ).to_dict()
