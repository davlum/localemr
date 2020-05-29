import time
import boto3
from test.fixtures.example_steps import EXAMPLE_STEP, MAX_WAIT
from localemr.common import EMR_STEP_TERMINAL_STATES


def test_run_step_no_jar():
    emr = boto3.client(
        service_name='emr',
        region_name='us-east-1',
        endpoint_url='http://localhost:3000',
    )
    resp = emr.run_job_flow(
        Name="log-etl-dev",
        ReleaseLabel='emr-5.7.0',
        Instances={
            'MasterInstanceType': 'm4.xlarge',
            'SlaveInstanceType': 'm4.xlarge',
            'InstanceCount': 3,
            'KeepJobFlowAliveWhenNoSteps': True,
        },
    )

    cluster_id = resp['JobFlowId']

    add_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[EXAMPLE_STEP])
    first_step_ip = add_response['StepIds'][0]
    wait_counter = 0
    while wait_counter != MAX_WAIT:
        time.sleep(10)
        resp = emr.describe_step(ClusterId=cluster_id, StepId=first_step_ip)
        if resp['Step']['Status']['State'] in EMR_STEP_TERMINAL_STATES:
            log = resp['Step']['Status']['FailureDetails']['LogFile']
            assert "java.lang.ClassNotFoundException: com.company.org.Jar" in log
            return
        wait_counter = wait_counter + 1

    raise TimeoutError("Test timed out and failed")
