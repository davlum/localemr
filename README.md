![build](https://github.com/davlum/localemr/workflows/Local%20EMR%20CI/badge.svg)

# local-emr

Based on the work from [spulec/moto](https://github.com/spulec/moto).

A locally running service that resembles [Elastic Map Reduce](https://aws.amazon.com/emr/).
This should not be used in any production environment whatsoever. The intent is to
facilitate local development.

Currently requires [Docker](https://www.docker.com/) in order to bring up additional services, such as
[Apache Livy](https://livy.incubator.apache.org/) and [Apache Spark](https://spark.apache.org/).

Make sure AWS related environment variables are set in order to not alter your actual
Amazon infrastructure.

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
import os
import boto3

os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'

# Insert some data into locally running S3
s3 = boto3.client('s3', endpoint_url="http://localhost:2000")
bucket = 'bucket'
s3.create_bucket(Bucket=bucket)
s3.upload_file('test/fixtures/word-count.jar', bucket, 'tmp/localemr/word-count.jar')
s3.put_object(Bucket=bucket, Key='key/2020-05/03/02/part.txt', Body="hello goodbye")  # Will be returned
s3.put_object(Bucket=bucket, Key='key/2020-05/03/05/part.txt', Body="hello")  # Will be returned
s3.put_object(Bucket=bucket, Key='key/2020-05/02/02/part.txt', Body="goodbye")  # Won't be returned
s3.put_object(Bucket=bucket, Key='key/2020-05/03/08/part.gz', Body="foobar")  # Won't be returned

# The step to submit
step = {
    'Name': 'EMR Job',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            '/usr/bin/spark-submit',
            '--master', 'yarn',
            '--class', 'com.oreilly.learningsparkexamples.mini.scala.WordCount',
            '--name', 'test',
            '--conf', 'spark.driver.cores=1',
            '--conf', 'spark.yarn.maxAppAttempts=1',
            's3a://bucket/tmp/localemr/word-count.jar',
            's3a://bucket/key/2020-05/03/*/*.txt',
            's3a://bucket/tmp/localemr/output',
        ]
    }
}

# Connect to local EMR service
emr = boto3.client(
    service_name='emr',
    region_name='us-east-1',
    endpoint_url='http://localhost:3000',
)

# Create a fake EMR cluster
# run_job_flow could take a while to fetch the docker container
resp = emr.run_job_flow(
    Name='example-localemr',
    # The ReleaseLabel determines the verison of Spark
    ReleaseLabel='emr-5.29.0',
    Instances={
        'MasterInstanceType': 'm4.xlarge',
        'SlaveInstanceType': 'm4.xlarge',
        'InstanceCount': 3,
        'KeepJobFlowAliveWhenNoSteps': True,
    },
)

print(resp)

# Get the cluster ID
cluster_id = resp['JobFlowId']

# Add the step to the cluster
add_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])

print(add_response)
```
3. `docker ps` in order to see which port the container has binded to.
```bash
$ docker ps
CONTAINER ID        IMAGE                                        COMMAND                  CREATED              STATUS              PORTS                              NAMES
1bd78a17e0fa        davlum/localemr-container:0.5.0-spark2.4.4   "./entrypoint"           About a minute ago   Up About a minute   0.0.0.0:32795->8998/tcp            example-localemr
c5f6b1223aa9        motoserver/moto:latest                       "/usr/bin/moto_serve…"   2 minutes ago        Up 2 minutes        0.0.0.0:2000->2000/tcp, 5000/tcp   s3
c1f9e17da9b1        emr-local-monkey-patch_localemr              "python main.py"         26 minutes ago       Up 2 minutes        0.0.0.0:3000->3000/tcp             localemr
```
4. In this case the container can be accessed at http://localhost:32795