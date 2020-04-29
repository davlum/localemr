import json
import requests
from typing import List
import boto3
import tempfile
from src.convert_s3_to_local import extract_s3_parts, extract_s3_files_from_step, convert_s3_to_local_path
from src.emr_to_livy import transform_emr_to_livy
from src.emr.models import EMRStepStates
from multiprocessing import Queue
import time
import pathlib
import os
from botocore.errorfactory import ClientError


LIVY_TERMINAL_STATES = ['success', 'error']

LIVY_RUNNING_STATES = [
   'not_started',
   'starting',
   'busy',
   'idle',
   'shutting_down',
]

LIVY_STATES = LIVY_TERMINAL_STATES + LIVY_RUNNING_STATES


class Configuration:
    def __init__(self, convert_s3_to_local=True, local_dir=None, livy_host='livy:8998'):
        self.convert_s3_to_local = convert_s3_to_local
        # If not specified temporary directories will be used
        self.local_dir = local_dir
        self.livy_host = livy_host


CONF = Configuration()


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
        print(s3_path)
        if s3_key_exists(s3, bucket, key):
            s3.download_file(bucket, key, local_path)


def send_step_to_livy(cli_args: List[str]):
    livy_step = transform_emr_to_livy(cli_args)
    headers = {'Content-Type': 'application/json'}
    r = requests.post(CONF.livy_host + '/batches', data=json.dumps(livy_step), headers=headers)
    print(r.json())
    batch_id = r.json()['id']
    state = 'not_started'
    while state not in LIVY_TERMINAL_STATES:
        time.sleep(10)
        r = requests.get(CONF.livy_host + '/batches/{}/state'.format(batch_id), headers=headers)
        state = r.json()['state']


def process_spark_command(cli_args: List[str]):
    if True:
        send_step_to_livy(cli_args)


def process_step(process_queue: Queue, status_queue: Queue):
    step = process_queue.get()
    step.state = EMRStepStates.RUNNING
    try:
        status_queue.put(step)
        cli_args = step.args
        if CONF.convert_s3_to_local:
            if CONF.local_dir is None:
                with tempfile.TemporaryDirectory() as tmp_dir_name:
                    get_files_from_s3(tmp_dir_name, cli_args)
                    cli_args = convert_s3_to_local_path(tmp_dir_name, cli_args)
                    process_spark_command(cli_args)

            else:
                get_files_from_s3(CONF.local_dir, cli_args)
                cli_args = convert_s3_to_local_path(CONF.local_dir, cli_args)
                process_spark_command(cli_args)
    except Exception as e:
        step.state = EMRStepStates.FAILED
        step.failure_details.message = str(e.with_traceback(None))
        status_queue.put(step)


def read_task_queue(process_queue: Queue, status_queue: Queue):
    while True:
        if process_queue.empty():
            time.sleep(2)
        else:
            process_step(process_queue, status_queue)
