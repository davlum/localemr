import boto3
from test.example_step import EXAMPLE_STEP


def test_my_model_save():
    emr = boto3.client(
        service_name='emr',
        region_name='us-east-1',
        endpoint_url='http://localhost:5000',
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

    add_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=EXAMPLE_STEP)
    print(add_response)
    first_step_ip = add_response['StepIds'][0]
    count = 1
    while count != 0:
        resp = emr.describe_step(ClusterId=cluster_id, StepId=first_step_ip)
        print(resp)
        count = count - 1
    raise ValueError('drrrr')
