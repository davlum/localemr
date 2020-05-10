from test.fixtures.util import get_client, make_cluster
from localemr.emr.models import EMRStepStates
from test.fixtures.example_steps import S3_STEP
import time


def test_run_step_no_jar():
    emr = get_client()
    resp = make_cluster(emr)

    cluster_id = resp['JobFlowId']

    add_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[S3_STEP])
    first_step_ip = add_response['StepIds'][0]
    max_wait = 10
    while max_wait != 0:
        time.sleep(3)
        resp = emr.describe_step(ClusterId=cluster_id, StepId=first_step_ip)
        if resp['Step']['Status']['State'] == EMRStepStates.FAILED:
            log = resp['Step']['Status']['FailureDetails']['LogFile']
            assert "java.lang.ClassNotFoundException: com.company.org.Jar" in log
            return
        max_wait = max_wait - 1

    raise ValueError("Test timed out and failed")
