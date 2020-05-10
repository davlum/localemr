import os
import pytest
import boto3
from moto import mock_s3
from test.fixtures.example_steps import EXAMPLE_STEP, S3_STEP
import localemr.s3.convert_to_local as s3

cli_args = EXAMPLE_STEP['HadoopJarStep']['Args']


def test_extract_s3_parts():
    s3_path = 's3://adgear-data/hash/hourly/trader/exchange/2019-10-15/00'
    bucket, key = s3.extract_s3_parts(s3_path)
    assert bucket == 'adgear-data'
    assert key == 'hash/hourly/trader/exchange/2019-10-15/00'
    with pytest.raises(ValueError) as e:
        s3.extract_s3_parts('foobar')
        assert e == "Couldn't extract bucket and key from S3 bucket {}".format(s3_path)


def test_extract_s3_files_from_step():
    assert s3.extract_s3_paths_from_step(cli_args) == [
        ('example-bucket', 'artifacts/jar-with-dependencies.jar'), ('ccpa', 'delete')
    ]


def test_convert_s3_to_local_path():
    assert s3.convert_s3_to_local_path('/tmp/example', cli_args) == [
        '/usr/bin/spark-submit',
        '--deploy-mode', 'cluster',
        '--master', 'yarn',
        '--class', 'com.company.org.Jar',
        '--name', 'test',
        '--num-executors', '256',
        '--driver-memory', '4G',
        '--executor-memory', '30G',
        '--conf', 'spark.driver.cores=1',
        '--conf', 'spark.yarn.maxAppAttempts=1',
        'file:///tmp/example/example-bucket/artifacts/jar-with-dependencies.jar',
        '--output-path=file:///tmp/example/ccpa/delete',
        '--partitions=512',
        '--final-output-concurrency=256',
        '--graphite-port=2003',
        '--max-age-in-days=180',
    ]


@mock_s3
def test_s3_ls():
    conn = boto3.client('s3')
    bucket = 'bucket'
    conn.create_bucket(Bucket=bucket)
    conn.put_object(Bucket=bucket, Key="key/test/o1", Body="")
    conn.put_object(Bucket=bucket, Key='key/test/o2', Body="")
    res = [r.key for r in s3.ls_s3(conn, bucket, 'key/test')]
    assert set(res) == {"key/test/o1", 'key/test/o2'}

    conn.put_object(Bucket=bucket, Key='key/2020-05/03/02/part.avro', Body="")  # Will be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/05/part.avro', Body="")  # Will be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/02/02/part.avro', Body="")  # Won't be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/08/part.gz', Body="")  # Won't be returned

    res = [r.key for r in s3.ls_s3(conn, bucket, 'key/2020-05/03/*/*.avro')]
    assert set(res) == {'key/2020-05/03/02/part.avro', 'key/2020-05/03/05/part.avro'}


@mock_s3
def test_is_nonempty_s3_subdir():
    conn = boto3.client('s3')
    bucket = 'bucket'
    conn.create_bucket(Bucket=bucket)
    conn.put_object(Bucket=bucket, Key="key/test/o1", Body="")
    conn.put_object(Bucket=bucket, Key="thing", Body="")
    assert s3.is_nonempty_s3_subdir(conn, bucket, 'key')
    assert not s3.is_nonempty_s3_subdir(conn, bucket, 'thing')
    assert not s3.is_nonempty_s3_subdir(conn, bucket, 'foobar')


@mock_s3
def test_is_s3_file():
    conn = boto3.client('s3')
    bucket = 'bucket'
    conn.create_bucket(Bucket=bucket)
    conn.put_object(Bucket=bucket, Key="key/test/o1", Body="")
    assert s3.is_s3_file(conn, bucket, "key/test/o1")
    assert not s3.is_s3_file(conn, bucket, 'key/test')


@mock_s3
def test_process_s3_file():
    conn = boto3.client('s3')
    bucket = 'bucket'
    conn.create_bucket(Bucket=bucket)
    key = "key/test/o1"
    conn.put_object(Bucket=bucket, Key="key/test/o1", Body="hello")
    local_dir_prefix = 'tmp/file'
    s3.process_s3_file(conn, local_dir_prefix, bucket, key)
    with open(os.path.join(local_dir_prefix, bucket, key)) as f:
        s = f.read()
        assert s == 'hello'


@mock_s3
def test_get_files_from_s3():
    conn = boto3.client('s3')
    bucket = 'bucket'
    conn.create_bucket(Bucket=bucket)
    prefix = '/tmp/files'
    conn.upload_file('test/fixtures/word-count.jar', bucket, 'tmp/files/word-count.jar')
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/02/part.txt', Body="hello goodbye")  # Will be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/05/part.txt', Body="hello")  # Will be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/02/02/part.txt', Body="goodbye")  # Won't be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/08/part.gz', Body="foobar")  # Won't be returned
    s3.get_files_from_s3(conn, '/tmp/files', S3_STEP['HadoopJarStep']['Args'])
    assert os.path.exists(os.path.join(prefix, bucket, 'tmp/files/word-count.jar'))
    assert os.path.exists(os.path.join(prefix, bucket, 'key/2020-05/03/02/part.txt'))
    assert os.path.exists(os.path.join(prefix, bucket, 'key/2020-05/03/05/part.txt'))
    assert not os.path.exists(os.path.join(prefix, bucket, 'key/2020-05/02/02/part.txt'))
    assert not os.path.exists(os.path.join(prefix, bucket, 'key/2020-05/03/08/part.gz'))
