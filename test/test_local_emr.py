import json
from test.fixtures.example_step import EXAMPLE_STEP, WORKING_STEP
from test.fixtures.util import get_client, make_cluster
from localemr.emr.models import EMRStepStates
import time


def test_run_step_no_jar():
    emr = get_client()
    resp = make_cluster(emr)

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


def test_run_step_with_jar():
    # https://stackoverflow.com/questions/51038328/apache-livy-doesnt-work-with-local-jar-file
    emr = get_client()
    resp = make_cluster(emr)

    cluster_id = resp['JobFlowId']

    add_response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[WORKING_STEP])
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
