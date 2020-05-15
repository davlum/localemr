import boto3


def get_client():
    return boto3.client(
        service_name='emr',
        region_name='us-east-1',
        endpoint_url='http://localhost:3000',
    )


def make_cluster(emr, release_label='emr-5.29.0'):
    return emr.run_job_flow(
        Name="log-etl-dev",
        ReleaseLabel=release_label,
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
