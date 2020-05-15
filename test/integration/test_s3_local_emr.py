import time
import boto3
from io import StringIO
import pandas as pd
from test.fixtures.util import get_client, make_cluster
from localemr.models import EMR_STEP_TERMINAL_STATES
from test.fixtures.example_steps import S3_STEP


def test_run_step_with_s3():
    conn = boto3.client('s3', endpoint_url="http://s3:2000")
    bucket = 'bucket'
    conn.create_bucket(Bucket=bucket)
    conn.upload_file('/tmp/localemr/word-count.jar', bucket, 'tmp/localemr/word-count.jar')
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/02/part.txt', Body="hello goodbye")  # Will be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/05/part.txt', Body="hello")  # Will be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/02/02/part.txt', Body="goodbye")  # Won't be returned
    conn.put_object(Bucket=bucket, Key='key/2020-05/03/08/part.gz', Body="foobar")  # Won't be returned

    emr = get_client()
    resp = make_cluster(emr)

    cluster_id = resp["JobFlowId"]

    add_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[S3_STEP])
    first_step_ip = add_response['StepIds'][0]
    max_wait = 10
    while max_wait != 0:
        time.sleep(5)
        resp = emr.describe_step(ClusterId=cluster_id, StepId=first_step_ip)
        if resp['Step']['Status']['State'] in EMR_STEP_TERMINAL_STATES:
            obj = conn.get_object(Bucket=bucket, Key="tmp/localemr/output/part-00000")
            result = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')), header=None)
            assert set(map(tuple, result.values.tolist())) == {("goodbye", 1), ("hello", 2)}
            return
        max_wait = max_wait - 1

    raise ValueError("Test timed out and failed")
