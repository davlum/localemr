![build](https://github.com/davlum/localemr/workflows/Local%20EMR%20CI/badge.svg)

# local-emr

Based on the work from [spulec/moto](https://github.com/spulec/moto).

A locally running service that resembles [AWS EMR](https://aws.amazon.com/emr/).
Should not be used in any production environment whatsoever. The intent is to
facilitate local development.

Currently requires [Docker](https://www.docker.com/) in order to bring up additional services, such as
[Apache Livy](https://livy.incubator.apache.org/) and [Apache Spark](https://spark.apache.org/).

Make sure AWS related environment variables are set in order to not alter your actual
AWS infrastructure.

```.env
AWS_ACCESS_KEY_ID=testing
AWS_SECRET_ACCESS_KEY=testing
AWS_SECURITY_TOKEN=testing
AWS_SESSION_TOKEN=testing
```

Example usage with Python and boto3.

1. `docker-compose up`

2. In another shell; 
```python
import boto3

# Connect to local EMR service
emr = boto3.client(
    service_name='emr',
    region_name='us-east-1',
    endpoint_url='http://localhost:3000',
)

# Create a fake EMR cluster
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

print(resp)

# Get the cluster ID
cluster_id = resp['JobFlowId']

# Example step
step = {
    'Name': 'EMR Job',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            '/usr/bin/spark-submit',
            '--class', 'com.oreilly.learningsparkexamples.mini.scala.WordCount',
            '--name', 'test',
            'file:///tmp/files/word-count.jar',
            '/tmp/files/input.txt',
            '/tmp/files/output',
        ]
    }
}

add_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])

print(add_response)
```
3. Navigate to http://localhost:8998 to watch the submitted Spark job.
4. `docker-compose down --volumes` to remove the shared volumes.
