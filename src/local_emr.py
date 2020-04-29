import json
import requests
from moto.emr.models import FakeCluster, FakeStep
from typing import List
import boto3
import tempfile
from src.convert_s3_to_local import extract_s3_parts, extract_s3_files_from_step, convert_s3_to_local_path
from src.emr_to_livy import transform_emr_to_livy
from multiprocessing import Queue
import time
import pathlib
import os
from botocore.errorfactory import ClientError


class FailureDetails:
    def __init__(self):
        self.reason = None
        self.message = None
        self.log_file = None

    def to_dict(self):
        return {
            'Reason': self.reason,
            'Message': self.message,
            'LogFile': self.log_file,
        }


class EMRStepStates:
    PENDING = 'PENDING          '
    CANCEL_PENDING = 'CANCEL_PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    CANCELLED = 'CANCELLED'
    FAILED = 'FAILED'
    INTERRUPTED = 'INTERRUPTED'


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


def read_task_queue(process_queue: Queue, status_queue: Queue):
    while True:
        if process_queue.empty():
            time.sleep(2)
        else:
            step = process_queue.get()
            step.state = EMRStepStates.RUNNING
            try:
                status_queue.put(step)
                cli_args = step.args
                raise ValueError("SHOULD SHOW UP IN MESSAGE")
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
                print(step.failure_details.to_dict())


def add_steps(self: FakeCluster, steps):
    added_steps = []
    for step in steps:
        if self.steps:
            # If we already have other steps, this one is pending
            fake = FakeStep(state=EMRStepStates.PENDING, **step)
        else:
            fake = FakeStep(state=EMRStepStates.RUNNING, **step)
        if self.process_queue.empty():
            self.process_queue.put(fake)
        self.steps.append(fake)
        added_steps.append(fake)
    self.state = "RUNNING"
    return added_steps


def list_steps(self: FakeCluster, cluster_id, marker=None, step_ids=None, step_states=None):
    max_items = 50
    steps: List[FakeStep] = self.clusters[cluster_id].steps
    status_queue: Queue = self.clusters[cluster_id].status_queue
    if not status_queue.empty():
        updated_step: FakeStep = status_queue.get()
        steps = [updated_step if step.id == updated_step.id else step for step in steps]
        self.clusters[cluster_id].steps = steps
    if step_ids:
        steps = [s for s in steps if s.id in step_ids]
    if step_states:
        steps = [s for s in steps if s.state in step_states]
    start_idx = 0 if marker is None else int(marker)
    marker = (
        None if len(steps) <= start_idx + max_items else str(start_idx + max_items)
    )
    return steps[start_idx: start_idx + max_items], marker


def describe_step(self: FakeCluster, cluster_id, step_id):
    steps: List[FakeStep] = self.clusters[cluster_id].steps
    status_queue: Queue = self.clusters[cluster_id].status_queue
    if not status_queue.empty():
        updated_step: FakeStep = status_queue.get()
        steps = [updated_step if step.id == updated_step.id else step for step in steps]
        self.clusters[cluster_id].steps = steps
    for step in steps:
        if step.id == step_id:
            return step
