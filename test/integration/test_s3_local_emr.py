import time
from io import StringIO
from test.fixtures.example_steps import S3_STEP, MAX_WAIT
import pytest
import boto3
import pandas as pd
from localemr.common import EMR_STEP_TERMINAL_STATES, EmrStepState


@pytest.fixture(scope='module', autouse=True)
def populate_s3():
    s3 = boto3.client('s3', endpoint_url="http://s3:2000")
    bucket = 'bucket'
    try:
        keys = s3.list_objects_v2(Bucket=bucket)
        if 'Contents' in keys:
            s3.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': k['Key']} for k in keys['Contents']]})
    except s3.exceptions.NoSuchBucket:
        s3.create_bucket(Bucket=bucket)


def test_run_spark_step_with_s3():
    conn = boto3.client('s3', endpoint_url="http://s3:2000")
    bucket = 'bucket'
    conn.upload_file('test/fixtures/wc-spark.jar', bucket, 'tmp/localemr/wc-spark.jar')
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/02/part.txt', Body="hello goodbye")  # Will be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/05/part.txt', Body="hello")  # Will be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/02/02/part.txt', Body="goodbye")  # Won't be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/08/part.gz', Body="foobar")  # Won't be returned
    emr = boto3.client(
        service_name='emr',
        region_name='us-east-1',
        endpoint_url='http://localhost:3000',
    )
    resp = emr.run_job_flow(
        Name="log-etl-dev",
        ReleaseLabel='emr-5.29.0',
        Instances={
            'MasterInstanceType': 'm4.xlarge',
            'SlaveInstanceType': 'm4.xlarge',
            'InstanceCount': 3,
            'KeepJobFlowAliveWhenNoSteps': True,
        },

    )

    cluster_id = resp["JobFlowId"]

    add_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[S3_STEP])
    first_step_ip = add_response['StepIds'][0]
    wait_counter = 0
    while wait_counter != MAX_WAIT:
        time.sleep(5)
        resp = emr.describe_step(ClusterId=cluster_id, StepId=first_step_ip)
        state = resp['Step']['Status']['State']
        if state in EmrStepState.COMPLETED:
            obj = conn.get_object(Bucket=bucket, Key="tmp/localemr/output/part-00000")
            result = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')), header=None)
            assert set(map(tuple, result.values.tolist())) == {("goodbye", 1), ("hello", 2)}
            return
        if state in EMR_STEP_TERMINAL_STATES:
            raise ValueError("Job failed with status; {}".format(state))
        wait_counter = wait_counter + 1

    raise TimeoutError("Test timed out and failed")


# def test_run_mapreduce_step_with_s3():
#     conn = boto3.client('s3', endpoint_url="http://s3:2000")
#     bucket = 'bucket'
#     conn.put_object(Bucket=bucket, Key='user/joe/wordcount/input/file01', Body="Hello World Bye World")  # Will be returned
#     conn.put_object(Bucket=bucket, Key='user/joe/wordcount/input/file02', Body="Hello Hadoop Goodbye Hadoop")  # Will be returned
#     emr = boto3.client(
#         service_name='emr',
#         region_name='us-east-1',
#         endpoint_url='http://localhost:3000',
#     )
#     resp = emr.run_job_flow(
#         Name="log-etl-dev",
#         ReleaseLabel='emr-5.29.0',
#         Instances={
#             'MasterInstanceType': 'm4.xlarge',
#             'SlaveInstanceType': 'm4.xlarge',
#             'InstanceCount': 3,
#             'KeepJobFlowAliveWhenNoSteps': True,
#         },
#
#     )
#
#     cluster_id = resp["JobFlowId"]
#
#     add_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[MAPREDUCE_STEP])
#     first_step_ip = add_response['StepIds'][0]
#     wait_counter = 0
#     while wait_counter != MAX_WAIT:
#         time.sleep(5)
#         resp = emr.describe_step(ClusterId=cluster_id, StepId=first_step_ip)
#         state = resp['Step']['Status']['State']
#         if state in EmrStepState.COMPLETED:
#             obj = conn.get_object(Bucket=bucket, Key="user/joe/wordcount/output/part-r-00000")
#             result = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')), header=None, sep='\t')
#             assert set(map(tuple, result.values.tolist())) == {("Goodbye", 1), ("Hadoop", 2), ("Hello", 2), ("World", 2), ("Bye", 1)}
#             return
#         if state in EMR_STEP_TERMINAL_STATES:
#             raise ValueError("Job failed with status; {}".format(state))
#         wait_counter = wait_counter + 1
#
#     raise TimeoutError("Test timed out and failed")
