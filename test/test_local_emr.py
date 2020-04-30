import json
import boto3
from test.example_step import EXAMPLE_STEP
from src.emr.models import EMRStepStates
import time


def test_my_model_save():
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
            'TerminationProtected': False,
            'Ec2SubnetId': 'my-subnet-id',
            'Ec2KeyName': 'my-key',
        },
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )

    cluster_id = resp['JobFlowId']

    add_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[EXAMPLE_STEP])
    first_step_ip = add_response['StepIds'][0]
    max_wait = 10
    while max_wait != 0:
        time.sleep(3)
        resp = emr.describe_step(ClusterId=cluster_id, StepId=first_step_ip)
        if resp['Step']['Status']['State'] == EMRStepStates.FAILED:
            log = json.loads(resp['Step']['Status']['FailureDetails']['LogFile'])
            assert log['log'][3] == "java.lang.ClassNotFoundException: com.company.org.Jar"
            return
        max_wait = max_wait - 1

    raise ValueError("Test timed out and failed")