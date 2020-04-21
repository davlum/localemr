from moto.emr.models import FakeCluster, FakeStep
from typing import List
import moto.server as server
import boto3
import tempfile
from src.convert_s3_to_local import extract_s3_parts, extract_s3_files_from_step, convert_s3_to_local_path
from src.emr_to_livy import transform_emr_to_livy
from multiprocessing import Queue, Process
import time
import pathlib
import os
from botocore.errorfactory import ClientError

process_queue = Queue()

FakeCluster.process_queue = process_queue


class Configuration:
    def __init__(self, convert_s3_to_local = True, local_dir=None):
        self.convert_s3_to_local = convert_s3_to_local
        # If not specified temporary directories will be used
        self.local_dir = local_dir


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
        if s3_key_exists(s3, bucket, key):
            s3.download_file(bucket, key, local_path)


def reader_f(queue: Queue):
    while True:
        if queue.empty():
            time.sleep(2)
        else:
            step = queue.get()
            cli_args = step['args']
            if CONF.convert_s3_to_local:
                if CONF.local_dir is None:
                    with tempfile.TemporaryDirectory() as tmp_dir_name:
                        get_files_from_s3(tmp_dir_name, cli_args)
                        cli_args = convert_s3_to_local_path(tmp_dir_name, cli_args)
                        livy_step = transform_emr_to_livy(cli_args)
                        print(livy_step)
                else:
                    get_files_from_s3(CONF.local_dir, cli_args)
                    cli_args = convert_s3_to_local_path(CONF.local_dir, cli_args)
                    livy_step = transform_emr_to_livy(cli_args)
                    print(livy_step)


def add_steps(self: FakeCluster, steps):
    added_steps = []
    for step in steps:
        if self.steps:
            # If we already have other steps, this one is pending
            fake = FakeStep(state="PENDING", **step)
        else:
            fake = FakeStep(state="STARTING", **step)
        if self.process_queue.empty():
            self.process_queue.put(step)
        self.steps.append(fake)
        added_steps.append(fake)
    self.state = "RUNNING"
    return added_steps


# Overwrite these function
FakeCluster.add_steps = add_steps

reader_process = Process(target=reader_f, args=(process_queue,))
reader_process.daemon = True
reader_process.start()  # Launch reader_process() as a separate python process

server.main(['emr'])

