import os
import io
import sys
import tarfile
import subprocess
import requests
from localemr.config import Configuration
from localemr.common import LocalFakeStep, SparkResult, EmrStepState, FailureDetails
from localemr.exec.interface import ExecInterface


def download_url_to_path(url: str, path: str):
    r = requests.get(url)
    r.raise_for_status()
    f = io.BytesIO(r.content)
    with tarfile.open(fileobj=f) as tar:
        tar.extractall(path)


class Subprocess(ExecInterface):

    def __init__(self, config: Configuration):
        self.config = config

    def exec_process(self, emr_step: LocalFakeStep) -> SparkResult:
        base_log_dir = f'/var/log/{emr_step.cluster_name}/{emr_step.cluster_id}/steps/{emr_step.id}'
        os.makedirs(base_log_dir, exist_ok=True)
        stderr_path = f'{base_log_dir}/stderr.log'
        stdout_path = f'{base_log_dir}/stdout.log'
        with open(stderr_path, 'w') as stderr, open(stdout_path, 'w') as stdout, \
                subprocess.Popen(emr_step.to_cli_args(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 universal_newlines=True, env=os.environ) as proc:

            for line in proc.stdout:
                sys.stdout.write(line)
                stdout.write(line)
            for line in proc.stderr:
                sys.stderr.write(line)
                stderr.write(line)

        return_code = proc.wait()
        if return_code != 0:
            with open(stderr_path) as f:
                log = f.read()
                return SparkResult(EmrStepState.FAILED, FailureDetails(
                    reason='Unknown Error',
                    log_file=log,
                ))

        return SparkResult(EmrStepState.COMPLETED, FailureDetails())
