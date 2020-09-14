import time
from typing import List
from xml.sax.saxutils import escape
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from localemr.common import SparkResult, FailureDetails, EmrStepState, LocalFakeStep
from localemr.config import Configuration
from localemr.exec.interface import ExecInterface


def wait_for_cluster(hostname: str):
    session = requests.Session()
    retry = Retry(connect=8, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.get(hostname + '/health')


def send_step_to_livylike(hostname: str, emr_step_id, cli_args: List[str]) -> SparkResult:
    """

    Parameters
    ----------
    config : The application Configuration object
    hostname : hostname of the cluster, ie the cluster name
    emr_step_id: The Id of the step
    cli_args : a list of arguments used to run a command line spark submit
        could support other commands in the future

    Returns
    -------
    SparkResult object. For now the reason property of the FailureDetails
    object will always contain `Unknown Error` until it is figured out what
    other values are possible when. Reference;
    https://docs.aws.amazon.com/emr/latest/APIReference/API_FailureDetails.html

    """
    url = 'http://{}:8998'.format(hostname)
    wait_for_cluster(url)
    step_endpoint = '{}/batch/{}'.format(url, emr_step_id)
    r = requests.post(step_endpoint, json={'args': cli_args})
    r.raise_for_status()
    batch_state = r.json()['status']
    while batch_state not in ('FAILED', 'SUCCEEDED'):
        time.sleep(5)
        r = requests.get(step_endpoint)
        r.raise_for_status()
        batch_state = r.json()['status']
    if batch_state == 'SUCCEEDED':
        return SparkResult(
            EmrStepState.COMPLETED,
            FailureDetails(),
        )
    if batch_state == 'FAILED':
        return SparkResult(
            EmrStepState.FAILED,
            FailureDetails(
                reason='Unknown Error',
                log_file=escape(r.json()['log']),
            ),
        )

    raise ValueError("Quit polling Livy in non-terminal state %s" % batch_state)


class LivyLike(ExecInterface):

    def __init__(self, config: Configuration):
        self.config = config

    def exec_process(self, emr_step: LocalFakeStep):
        return send_step_to_livylike(emr_step.hostname, emr_step.id, emr_step.to_cli_args())
