import json
import time
import requests
import logging
from typing import List
from localemr.emr.models import FailureDetails, EMRStepStates
from localemr.models import CONF, SparkResult
from localemr.livy.exceptions import LivyError
from localemr.livy.models import *


def from_dash_to_snake_case(conf_key: str):
    if conf_key[:2] != '--':
        raise ValueError("`%s` is not a --conf param")
    return conf_key[2:].replace('-', '_')


def extract_conf_until_jar(args: List[str]) -> LivyRequestBody:
    spark_conf = {}
    livy_step = {}
    it = iter(args)
    for key in it:
        if key.startswith('--'):
            val = next(it)
            camel_key = from_dash_to_snake_case(key)
            if camel_key == 'conf':
                key_val_ls = val.split("=")
                if len(key_val_ls) != 2:
                    raise ValueError("spark --conf a `%s` is badly formatted", val)
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
            raise ValueError("Emr step is not of expected format %s", args)


def transform_emr_to_livy(cli_args) -> LivyRequestBody:
    if 'spark-submit' in cli_args[0]:
        return extract_conf_until_jar(cli_args[1:])
    else:
        raise ValueError("Unsupported command `%s`", cli_args[0])


def post_livy_batch(data: LivyRequestBody) -> LivyBatchObject:
    headers = {'Content-Type': 'application/json'}
    try:
        resp = requests.post(CONF.livy_host + '/batches', data=json.dumps(data.to_dict()), headers=headers)
        logging.info(resp.json())
        resp.raise_for_status()
        return LivyBatchObject.from_dict(resp.json())
    except requests.exceptions.HTTPError as err:
        raise LivyError(err)


def get_livy_batch(batch_id) -> LivyBatchObject:
    headers = {'Content-Type': 'application/json'}
    try:
        resp = requests.get(CONF.livy_host + '/batches/{}'.format(batch_id), headers=headers)
        logging.info(resp.json())
        resp.raise_for_status()
        return LivyBatchObject.from_dict(resp.json())
    except requests.exceptions.HTTPError as err:
        raise LivyError(err)


def get_batch_logs(batch_id) -> LivyBatchObject:
    headers = {'Content-Type': 'application/json'}
    params = {'size': 100, 'from': 0}
    try:
        resp = requests.get(CONF.livy_host + '/batches/{}/log'.format(batch_id), params=params, headers=headers)
        logging.info(resp.json())
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as err:
        raise LivyError(err)


def send_step_to_livy(cli_args: List[str]) -> SparkResult:
    """

    Parameters
    ----------
    cli_args : a list of arguments used to run a command line spark submit
        could support other commands in the future

    Returns
    -------
    SparkResult object. For now the reason property of the FailureDetails
    object will always contain `Unknown Error` until it is figured out what
    other values are possible when. Reference;
    https://docs.aws.amazon.com/emr/latest/APIReference/API_FailureDetails.html

    """
    livy_step = transform_emr_to_livy(cli_args)
    livy_batch = post_livy_batch(livy_step)
    while livy_batch.state not in LIVY_TERMINAL_STATES:
        time.sleep(5)
        livy_batch = get_livy_batch(livy_batch.id)

    if livy_batch.state == LivyState.SUCCESS:
        return SparkResult(
            EMRStepStates.COMPLETED,
            FailureDetails()
        )

    elif livy_batch.state in (LivyState.ERROR, LivyState.DEAD):

        return SparkResult(
            EMRStepStates.FAILED,
            FailureDetails(
                reason='Unknown Error',
                log_file=json.dumps(get_batch_logs(livy_batch.id)),
            )
        )
    else:
        raise LivyError("Quit polling Livy in non-terminal state %s", livy_batch.state)
