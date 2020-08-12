import time
from test.fixtures.example_steps import MAX_WAIT
import boto3
from botocore.exceptions import ClientError
import pytest
from localemr.models import EmrClusterState


def test_run_job_flow_terminates_with_no_steps():
    # Transition of state should be from STARTING -> TERMINATING -> TERMINATED, but unsure
    # of how to get the timing of the responses correctly. Job flow could start terminating
    # before first request made to get status.
    client = boto3.client(
        service_name='emr',
        region_name='us-east-1',
        endpoint_url='http://localhost:3000',
    )
    resp = client.run_job_flow(
        Name="log-etl-dev",
        ReleaseLabel='emr-5.26.0',
        Instances={
            'MasterInstanceType': 'm4.xlarge',
            'SlaveInstanceType': 'm4.xlarge',
        }
    )
    cluster_id = resp['JobFlowId']
    resp = client.describe_job_flows(JobFlowIds=[cluster_id])
    state = resp['JobFlows'][0]['ExecutionStatusDetail']['State']
    wait_counter = 0
    while state != EmrClusterState.TERMINATED:
        resp = client.describe_job_flows(JobFlowIds=[cluster_id])
        state = resp['JobFlows'][0]['ExecutionStatusDetail']['State']
        assert state in (EmrClusterState.TERMINATING, EmrClusterState.TERMINATED, EmrClusterState.STARTING)
        time.sleep(10)
        wait_counter = wait_counter + 1
        if wait_counter == MAX_WAIT:
            raise TimeoutError("Test timed out before correct state achieved.")
    # Implicit but stated anyway
    assert state == EmrClusterState.TERMINATED


def test_run_job_flow_continues_with_no_steps():
    # Transition of state should be from STARTING -> WAITING -> TERMINATING -> TERMINATED
    client = boto3.client(
        service_name='emr',
        region_name='us-east-1',
        endpoint_url='http://localhost:3000',
    )
    cluster_name = "log-etl-2"
    resp = client.run_job_flow(
        Name=cluster_name,
        ReleaseLabel='emr-5.24.0',
        Instances={
            'MasterInstanceType': 'm4.xlarge',
            'SlaveInstanceType': 'm4.xlarge',
            'KeepJobFlowAliveWhenNoSteps': True,
        }
    )
    cluster_id = resp['JobFlowId']
    resp = client.describe_job_flows(JobFlowIds=[cluster_id])
    state = resp['JobFlows'][0]['ExecutionStatusDetail']['State']
    wait_counter = 0
    while state != EmrClusterState.WAITING:
        resp = client.describe_job_flows(JobFlowIds=[cluster_id])
        print(resp)
        state = resp['JobFlows'][0]['ExecutionStatusDetail']['State']
        assert state in (EmrClusterState.WAITING, EmrClusterState.STARTING)
        time.sleep(10)
        wait_counter = wait_counter + 1
        if wait_counter == MAX_WAIT:
            raise TimeoutError("Test timed out before correct state achieved.")

    assert state == EmrClusterState.WAITING
    client.terminate_job_flows(JobFlowIds=[cluster_id])
    resp = client.describe_job_flows(JobFlowIds=[cluster_id])
    state = resp['JobFlows'][0]['ExecutionStatusDetail']['State']
    assert state == EmrClusterState.TERMINATING


def test_invalid_emr_release_label():
    emr = boto3.client(
        service_name='emr',
        region_name='us-east-1',
        endpoint_url='http://localhost:3000',
    )
    with pytest.raises(ClientError) as e:
        emr.run_job_flow(
            Name="bad-cluster",
            ReleaseLabel='5.7.0',
            Instances={
                'MasterInstanceType': 'm4.xlarge',
                'SlaveInstanceType': 'm4.xlarge',
                'InstanceCount': 3,
                'KeepJobFlowAliveWhenNoSteps': True,
            },
        )
    assert '5.7.0 is not a valid emr release label' in str(e.value)
