import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from localemr.common import (
    SparkResult,
    FailureDetails,
    EmrStepState,
    LocalFakeStep,
    SPARK_CONF_MAP,
    UNWANTED_SPARK_CONFIGS,
    UNWANTED_CONF_CONFIGS,
)
from localemr.config import Configuration
from localemr.exec.interface import ExecInterface
from localemr.exec.livy.models import *


def extract_spark_conf_from_args(fake_step: LocalFakeStep, cli_args: iter):
    spark_conf = {}
    livy_args = {}
    file = fake_step.jar
    for elem in cli_args:
        if '.jar' in elem:
            # Gathered all the Spark related configuration, which is placed before the jar
            file = elem
            break

        if elem in UNWANTED_SPARK_CONFIGS:
            next(cli_args)
            continue

        if elem == '--class':
            livy_args['class_name'] = next(cli_args)
        elif elem == '--proxy-user':
            livy_args['proxy_user'] = next(cli_args)
        elif elem == '--conf':
            key_val = next(cli_args).split('=', 1)
            if key_val[0] in UNWANTED_CONF_CONFIGS:
                continue
            spark_conf[key_val[0]] = key_val[1]
        else:
            try:
                spark_conf[SPARK_CONF_MAP[elem]] = next(cli_args)
            except KeyError as e:
                raise ValueError("Unconfigured Spark CLI config `{}` in arguments: {}".format(elem, fake_step.to_cli_args())) from e

    return LivyRequestBody(
        file=file,
        conf=spark_conf,
        args=list(cli_args),
        **livy_args,
    )


def transform_emr_step_to_livy_req(fake_step: LocalFakeStep) -> LivyRequestBody:
    cli_args = iter(fake_step.to_cli_args())
    command = [next(cli_args)]
    if 'spark-submit' not in command:
        # Apache livy only supports spark jobs.
        raise ValueError("Job is not a spark job. Arguments: {}".format(fake_step.to_cli_args()))

    return extract_spark_conf_from_args(fake_step, cli_args)


def post_livy_batch(hostname: str, data: LivyRequestBody) -> LivyBatchObject:
    resp = requests.post(hostname + '/batches', json=data.to_dict())
    logging.info(resp.json())
    resp.raise_for_status()
    return LivyBatchObject.from_dict(resp.json())


def get_livy_batch(hostname: str, batch_id) -> LivyBatchObject:
    resp = requests.get(hostname + '/batches/{}'.format(batch_id))
    logging.info(resp.json())
    resp.raise_for_status()
    return LivyBatchObject.from_dict(resp.json())


def get_batch_logs(hostname: str, batch_id) -> dict:
    resp = requests.get(hostname + '/batches/{}/log'.format(batch_id))
    logging.info(resp.json())
    resp.raise_for_status()
    return resp.json()


def wait_for_cluster(hostname: str):
    session = requests.Session()
    retry = Retry(connect=8, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.get(hostname)


def send_step_to_livy(emr_step: LocalFakeStep) -> SparkResult:
    """

    Parameters
    ----------
    emr_step : The step to be transformed and submitted to Livy

    Returns
    -------
    SparkResult object. For now the reason property of the FailureDetails
    object will always contain `Unknown Error` until it is figured out what
    other values are possible when. Reference;
    https://docs.aws.amazon.com/emr/latest/APIReference/API_FailureDetails.html

    """
    hostname = 'http://{}:8998'.format(emr_step.hostname)
    wait_for_cluster(hostname)
    livy_step = transform_emr_step_to_livy_req(emr_step)
    livy_batch = post_livy_batch(hostname, livy_step)
    while livy_batch.state not in LIVY_TERMINAL_STATES:
        time.sleep(5)
        livy_batch = get_livy_batch(hostname, livy_batch.id)
    if livy_batch.state == LivyState.SUCCESS:
        return SparkResult(
            EmrStepState.COMPLETED,
            FailureDetails(),
        )
    if livy_batch.state in (LivyState.ERROR, LivyState.DEAD):
        return SparkResult(
            EmrStepState.FAILED,
            FailureDetails(
                reason='Unknown Error',
                log_file='\n'.join(get_batch_logs(hostname, livy_batch.id)['log']),
            ),
        )

    raise LivyError("Quit polling Livy in non-terminal state %s" % livy_batch.state)


class Livy(ExecInterface):

    def __init__(self, config: Configuration):
        self.config = config

    def exec_process(self, emr_step: LocalFakeStep):
        return send_step_to_livy(emr_step)
