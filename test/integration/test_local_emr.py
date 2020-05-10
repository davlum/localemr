from test.fixtures.example_steps import EXAMPLE_STEP, WORKING_STEP
from test.fixtures.util import get_client, make_cluster
from localemr.emr.models import EMRStepStates
import pandas as pd
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
            log = resp['Step']['Status']['FailureDetails']['LogFile']
            assert "java.lang.ClassNotFoundException: com.company.org.Jar" in log
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
    max_wait = 20
    while max_wait != 0:
        time.sleep(3)
        resp = emr.describe_step(ClusterId=cluster_id, StepId=first_step_ip)
        if resp['Step']['Status']['State'] == EMRStepStates.COMPLETED:
            result = pd.read_csv("/tmp/files/output/part-00000")
            expected = pd.read_csv("test/fixtures/expected.csv")
            assert set(map(tuple, result.values.tolist())) == set(map(tuple, expected.values.tolist()))
            return
        max_wait = max_wait - 1

    raise ValueError("Test timed out and failed")
