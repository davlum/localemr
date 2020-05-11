import time
import logging
import traceback
from datetime import datetime
import pytz
from typing import List
import tempfile
from copy import deepcopy
from multiprocessing import Queue
import boto3
from localemr.s3.convert_to_local import get_files_from_s3, convert_s3_to_local_path
from localemr.livy.backend import send_step_to_livy
from localemr.emr.models import EMRStepStates, FakeStep, FailureDetails
from localemr.models import SparkResult, CONF


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
                s3 = boto3.client('s3', endpoint_url=CONF.s3_host)
                get_files_from_s3(s3, dir_name, cli_args)

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
