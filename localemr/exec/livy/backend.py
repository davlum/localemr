import re
import json
import time
import logging
from typing import List
from xml.sax.saxutils import escape
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from localemr.common import SparkResult, FailureDetails, EmrStepState, LocalFakeStep
from localemr.config import Configuration
from localemr.exec.interface import ExecInterface
from localemr.exec.livy.exceptions import LivyError
from localemr.exec.livy.models import *


def from_dash_to_snake_case(conf_key: str):
    if conf_key[:2] != '--':
        raise ValueError("`%s` is not a --conf param")
    return conf_key[2:].replace('-', '_')


# pylint: disable=inconsistent-return-statements
def extract_conf_until_jar(args: List[str]) -> LivyRequestBody:
    spark_conf = {}
    livy_step = {}
    it = iter(args)
    for key in it:
        if key.startswith('--'):
            val = next(it)
            camel_key = from_dash_to_snake_case(key)
            if camel_key == 'conf':
                key_val_ls = val.split("=", 1)
                if len(key_val_ls) != 2:
                    raise ValueError("spark --conf `{}` is badly formatted".format(val))
                spark_conf[key_val_ls[0]] = key_val_ls[1]
            else:
                livy_step[camel_key] = val
        elif '.jar' in key:
            return LivyRequestBody(
                class_name=livy_step.get('class'),
                conf=spark_conf,
                args=list(it),
                file=key,
                **livy_step
            )
        else:
            raise ValueError("Emr step is not of expected format {}".format(args))


def transform_emr_to_livy(cli_args) -> LivyRequestBody:
    if 'spark-submit' in cli_args[0]:
        return extract_conf_until_jar(cli_args[1:])
    raise ValueError("Unsupported command `%s`" % cli_args[0])


def post_livy_batch(hostname: str, data: LivyRequestBody) -> LivyBatchObject:
    headers = {'Content-Type': 'application/json'}
    resp = requests.post(hostname + '/batches', data=json.dumps(data.to_dict()), headers=headers)
    logging.info(resp.json())
    resp.raise_for_status()
    return LivyBatchObject.from_dict(resp.json())


def get_livy_batch(hostname: str, batch_id) -> LivyBatchObject:
    headers = {'Content-Type': 'application/json'}
    resp = requests.get(hostname + '/batches/{}'.format(batch_id), headers=headers)
    logging.info(resp.json())
    resp.raise_for_status()
    return LivyBatchObject.from_dict(resp.json())


def get_batch_logs(hostname: str, batch_id) -> dict:
    headers = {'Content-Type': 'application/json'}
    resp = requests.get(hostname + '/batches/{}/log'.format(batch_id), headers=headers)
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


def convert_s3_to_s3a_path(emr_step: List[str]) -> List[str]:
    return [re.sub(r's3://|s3n://', 's3a://', v) for v in emr_step]


def add_mock_s3_conf(config: Configuration, livy_request_body: LivyRequestBody) -> LivyRequestBody:
    """
    Parameters
    ----------
    config : The Configuration object
    livy_request_body : The Request being made to Livy

    Returns
    -------
    The Livy Request enhanced with the configuration necessary to use a mock S3 instance.
    From https://github.com/sumitsu/s3_mocktest_demo
    """
    mock_s3_config = {
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.s3.client.factory.impl': 'dev.sumitsu.s3mocktest.NonChunkedDefaultS3ClientFactory',
        'spark.hadoop.fs.s3a.endpoint': config.s3_endpoint,
        'spark.hadoop.fs.s3a.access.key': config.localemr_aws_access_key_id,
        'spark.hadoop.fs.s3a.secret.key': config.localemr_aws_secret_access_key,
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.multiobjectdelete.enable': 'false',
        'spark.hadoop.fs.s3a.change.detection.version.required': 'false',
    }
    if livy_request_body.conf:
        livy_request_body.conf = {**livy_request_body.conf, **mock_s3_config}
    else:
        livy_request_body.conf = mock_s3_config
    return livy_request_body


def send_step_to_livy(config: Configuration, hostname: str, cli_args: List[str]) -> SparkResult:
    """

    Parameters
    ----------
    config : The application Configuration object
    hostname : The host and port to send the step to
    cli_args : a list of arguments used to run a command line spark submit
        could support other commands in the future

    Returns
    -------
    SparkResult object. For now the reason property of the FailureDetails
    object will always contain `Unknown Error` until it is figured out what
    other values are possible when. Reference;
    https://docs.aws.amazon.com/emr/latest/APIReference/API_FailureDetails.html

    """
    hostname = 'http://{}:8998'.format(hostname)
    if config.convert_to_mock_s3:
        cli_args = convert_s3_to_s3a_path(cli_args)
    livy_step = transform_emr_to_livy(cli_args)
    if config.convert_to_mock_s3:
        livy_step = add_mock_s3_conf(config, livy_step)
    wait_for_cluster(hostname)
    livy_batch = post_livy_batch(hostname, livy_step)
    while livy_batch.state not in LIVY_TERMINAL_STATES:
        time.sleep(5)
        livy_batch = get_livy_batch(hostname, livy_batch.id)
    if livy_batch.state == LivyState.SUCCESS:
        return SparkResult(
            EmrStepState.COMPLETED,
            FailureDetails()
        )
    if livy_batch.state in (LivyState.ERROR, LivyState.DEAD):
        return SparkResult(
            EmrStepState.FAILED,
            FailureDetails(
                reason='Unknown Error',
                log_file=escape('\n'.join(get_batch_logs(hostname, livy_batch.id)['log'])),
            )
        )

    raise LivyError("Quit polling Livy in non-terminal state %s" % livy_batch.state)


class Livy(ExecInterface):

    @staticmethod
    def exec_process(config: Configuration, emr_step: LocalFakeStep):
        return send_step_to_livy(config, emr_step.args, emr_step.hostname)
