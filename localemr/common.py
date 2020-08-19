"""
TODO: This is a shittily named module to solve circular dependencies. Make it better.
"""
import os
import re
import inspect
from typing import Optional, List, Dict
from datetime import datetime
from distutils.version import StrictVersion
from xml.sax.saxutils import escape
import pytz
from moto.emr.models import FakeStep
from moto.emr.exceptions import EmrError


def from_dash_to_snake_case(conf_key: str):
    if conf_key[:2] != '--':
        raise ValueError("`%s` is not a --conf param")
    return conf_key[2:].replace('-', '_')


def from_snake_to_camel_case(s: str) -> str:
    words = s.split('_')
    word_ls = [words[0]] + list(map(lambda w: w.capitalize(), words[1:]))
    return ''.join(word_ls)


def from_snake_to_dash(s: str) -> str:
    if s == 'class_name':
        return 'class'
    return s.replace('_', '-')


class EmrRequestConfig:
    def __init__(self,
                 proxy_user: Optional[str] = None,
                 class_name: Optional[str] = None,
                 jars: Optional[List[str]] = None,
                 py_files: Optional[List[str]] = None,
                 files: Optional[List[str]] = None,
                 archives: Optional[List[str]] = None,
                 queue: Optional[str] = None,
                 name: Optional[str] = None,
                 **kwargs):  # pylint: disable=unused-argument
        self.proxy_user = proxy_user
        self.class_name = class_name
        self.jars = jars.split(',') if isinstance(jars, str) else jars
        self.py_files = py_files.split(',') if isinstance(py_files, str) else py_files
        self.files = files.split(',') if isinstance(files, str) else files
        self.archives = archives.split(',') if isinstance(archives, str) else archives
        self.queue = queue
        self.name = name

    def to_dict(self):
        constructor_args = set(inspect.signature(EmrRequestConfig).parameters.keys())
        intersection = constructor_args.intersection(set(dir(self)))
        return {k: getattr(self, k) for k in intersection if getattr(self, k) is not None}


class EmrRequest:
    def __init__(self,
                 file: str,
                 command: str,
                 emr_request_config: Optional[EmrRequestConfig],
                 conf: Optional[Dict[str, str]] = None,
                 args: Optional[List[str]] = None):
        self.args = args.split(',') if isinstance(args, str) else args
        self.file = file
        self.command = command
        self.emr_request_config = emr_request_config
        self.conf = conf

    def to_dict(self):
        return {
            'file': self.file,
            'command': self.command,
            'emr_request_config': self.emr_request_config.to_dict(),
            'conf': self.conf,
            'args': self.args,
        }


def flatten(d: dict) -> List[str]:
    return [x for tup in d.items() for x in tup]


def transform_emr_req_to_cli_args(r: EmrRequest) -> List[str]:
    constructor_args = set(inspect.signature(EmrRequestConfig).parameters.keys())
    intersection = sorted(constructor_args.intersection(set(dir(r.emr_request_config))))
    spark_conf = {f'--{from_snake_to_dash(k)}': getattr(r.emr_request_config, k) for k in intersection
                  if getattr(r.emr_request_config, k) is not None}
    other_conf = {'--conf': f'{k}={v}' for k, v in r.conf.items()}
    return [r.command] + flatten(spark_conf) + flatten(other_conf) + [r.file] + r.args


class ClusterSubset:
    def __init__(self, state, name=None, release_label=None, start_datetime=None, ready_datetime=None,
                 end_datetime=None):
        self.name = name
        self.release_label = release_label
        self.state = state
        self.start_datetime = start_datetime
        self.ready_datetime = ready_datetime
        self.end_datetime = end_datetime

    def run_bootstrap_actions(self):
        self.ready_datetime = datetime.now(pytz.utc)
        self.state = EmrClusterState.WAITING

    def run_termination_actions(self):
        self.end_datetime = datetime.now(pytz.utc)
        self.state = EmrClusterState.TERMINATED


class LocalFakeStep(FakeStep):
    def __init__(self, hostname, cluster_id, cluster_name, **kwargs):
        super().__init__(**kwargs)
        self.failure_details = FailureDetails()
        self.hostname = hostname
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name

    def start(self):
        self.start_datetime = datetime.now(pytz.utc)

    def to_cli_args(self) -> List[str]:
        return transform_emr_req_to_cli_args(extract_conf_until_jar(self))


AWS_SCRIPT_RUNNERS = {'command-runner.jar', 'script-runner.jar'}


# pylint: disable=inconsistent-return-statements
def extract_conf_until_jar(fake_step: LocalFakeStep) -> EmrRequest:
    spark_conf = {}
    livy_step = {}
    it = iter(fake_step.args)
    if fake_step.jar in AWS_SCRIPT_RUNNERS:
        command = os.path.basename(next(it))
    else:
        command = 'hadoop'
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
            return EmrRequest(
                command=command,
                emr_request_config=EmrRequestConfig(
                    class_name=livy_step.get('class'),
                    **livy_step,
                ),
                args=list(it),
                conf=spark_conf,
                file=key,
            )
        else:
            raise ValueError("Emr step is not of expected format {}".format(fake_step.args))


class FailureDetails:
    def __init__(self, reason=None, message=None, log_file=None):
        self.reason = escape(reason) if reason else None
        self.message = escape(message) if message else None
        self.log_file = escape(log_file) if log_file else None

    def to_dict(self):
        return {
            'Reason': self.reason,
            'Message': self.message,
            'LogFile': self.log_file,
        }


class EmrStepState:
    PENDING = 'PENDING'
    CANCEL_PENDING = 'CANCEL_PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    CANCELLED = 'CANCELLED'
    FAILED = 'FAILED'
    INTERRUPTED = 'INTERRUPTED'


EMR_STEP_TERMINAL_STATES = [
    EmrStepState.FAILED,
    EmrStepState.CANCELLED,
    EmrStepState.INTERRUPTED
]


class SparkResult:
    def __init__(self, state: EmrStepState, failure_details: FailureDetails):
        self.state = state
        self.failure_details = failure_details


class ActionOnFailure:
    TERMINATE_JOB_FLOW = 'TERMINATE_JOB_FLOW'
    TERMINATE_CLUSTER = 'TERMINATE_CLUSTER'
    CANCEL_AND_WAIT = 'CANCEL_AND_WAIT'
    CONTINUE = 'CONTINUE'


class EmrClusterState:
    STARTING = 'STARTING'
    WAITING = 'WAITING'
    BOOTSTRAPPING = 'BOOTSTRAPPING'
    RUNNING = 'RUNNING'
    TERMINATING = 'TERMINATING'
    TERMINATED = 'TERMINATED'
    TERMINATED_WITH_ERRORS = 'TERMINATED_WITH_ERRORS'


EMR_CLUSTER_TERMINAL_STATES = [
    EmrClusterState.TERMINATED,
    EmrClusterState.TERMINATED_WITH_ERRORS
]

# There must be a docker image on davlum/localemr-container
# with a matching Spark version for this to work.
EMR_TO_APPLICATION_VERSION = {
    '5.0.0': {'Spark': '2.0.0'},
    '5.0.3': {'Spark': '2.0.1'},
    '5.2.0': {'Spark': '2.0.2'},
    '5.3.0': {'Spark': '2.1.0'},
    '5.6.0': {'Spark': '2.1.1'},
    '5.8.0': {'Spark': '2.2.0'},
    '5.11.0': {'Spark': '2.2.1'},
    '5.13.0': {'Spark': '2.3.0'},
    '5.16.0': {'Spark': '2.3.1'},
    '5.18.0': {'Spark': '2.3.2'},
    '5.20.0': {'Spark': '2.4.0'},
    '5.24.0': {'Spark': '2.4.2'},
    '5.25.0': {'Spark': '2.4.3'},
    '5.27.0': {'Spark': '2.4.4'},
    '6.0.0': {'Spark': '2.4.5'},
}


def parse_release_label(cluster_release_label):
    try:
        return re.findall(r'emr-(\d+\.\d+\.\d+)', cluster_release_label)[0]
    except IndexError:
        aws_docs = 'https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html'
        message = "{} is not a valid emr release label. See {} for more info".format(
            cluster_release_label, aws_docs
        )
        raise EmrError(
            error_type="ValidationException",
            message=message,
            template="error_json",

        )


def get_emr_version(cluster_release_label):
    """
    Parameters
    ----------
    cluster_release_label : a string of form 'emr-{semver}'

    Returns
    -------
    The corresponding EMR version

    Assumes the EMR versions from EMR_VERSION_TO_APPLICATION_VERSION are sorted smallest to largest
    """
    emr_version = parse_release_label(cluster_release_label)
    parsed_emr_version = StrictVersion(emr_version)
    versions = list(EMR_TO_APPLICATION_VERSION.keys())
    last_version = versions[0]
    if parsed_emr_version <= StrictVersion(last_version):
        return last_version
    for current_version in versions[1:]:
        parsed_current_version = StrictVersion(current_version)
        if parsed_emr_version == parsed_current_version:
            return emr_version
        if StrictVersion(last_version) < parsed_emr_version < parsed_current_version:
            return last_version
        last_version = current_version

    return versions[-1]


def cluster_to_spark_version(cluster: ClusterSubset) -> dict:
    return EMR_TO_APPLICATION_VERSION[get_emr_version(cluster.release_label)]['Spark']
