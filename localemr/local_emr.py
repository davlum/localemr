import time
import pathlib
import os
import logging
import traceback
from datetime import datetime
import pytz
from typing import List
import boto3
import tempfile
from copy import deepcopy
from multiprocessing import Queue
from botocore.errorfactory import ClientError
from localemr.convert_s3_to_local import extract_s3_parts, extract_s3_files_from_step, convert_s3_to_local_path
from localemr.livy.backend import send_step_to_livy
from localemr.emr.models import EMRStepStates, FakeStep, FailureDetails
from localemr.models import SparkResult, CONF


def s3_key_exists(s3, bucket, key) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404' and e.response['Error']['Message'] == 'Not Found':
            return False
        raise e


def get_files_from_s3(local_dir_name: str, args: List[str]):
    s3 = boto3.client('s3')
    for s3_path, local_path in extract_s3_files_from_step(local_dir_name, args):
        bucket, key = extract_s3_parts(s3_path)
        pathlib.Path(os.path.dirname(local_path)).mkdir(parents=True, exist_ok=True)
        if s3_key_exists(s3, bucket, key):
            logging.info("s3://%s/%s exists, downloading", bucket, key)
            s3.download_file(bucket, key, local_path)


def process_spark_command(cli_args: List[str]) -> SparkResult:
    return send_step_to_livy(cli_args)


def make_step_terminal(step: FakeStep, failure_details: FailureDetails, state: EMRStepStates) -> FakeStep:
    step = deepcopy(step)
    step.failure_details = failure_details
    step.state = state
    step.end_datetime = datetime.now(pytz.utc)
    return step


def process_step(process_queue: Queue, status_queue: Queue):
    step: FakeStep = process_queue.get()
    step.state = EMRStepStates.RUNNING
    step.start()
    try:
        status_queue.put(step)
        cli_args = step.args
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            dir_name = CONF.local_dir or tmp_dir_name

            if CONF.fetch_from_s3:
                get_files_from_s3(dir_name, cli_args)

            if CONF.convert_s3_to_local:
                cli_args = convert_s3_to_local_path(dir_name, cli_args)

            spark_result = process_spark_command(cli_args)
            step = make_step_terminal(step, spark_result.failure_details, spark_result.state)
            status_queue.put(step)

    except Exception as e:
        failure_details = FailureDetails(
            reason='Unknown Reason',
            message=traceback.format_exc()
        )
        step = make_step_terminal(step, failure_details, EMRStepStates.FAILED)
        logging.exception(e)
        status_queue.put(step)


def read_task_queue(process_queue: Queue, status_queue: Queue):
    while True:
        if process_queue.empty():
            time.sleep(2)
        else:
            process_step(process_queue, status_queue)
