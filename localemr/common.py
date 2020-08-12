"""
TODO: This is a shittily named module to solve circular dependencies. Make it better.
"""
import re
from datetime import datetime
from distutils.version import StrictVersion
import pytz
from moto.emr.models import FakeStep
from moto.emr.exceptions import EmrError


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


class LocalFakeStep(FakeStep):
    def __init__(self, hostname, **kwargs):
        super().__init__(**kwargs)
        self.failure_details = FailureDetails()
        self.hostname = hostname

    def start(self):
        self.start_datetime = datetime.now(pytz.utc)


class FailureDetails:
    def __init__(self, reason=None, message=None, log_file=None):
        self.reason = reason
        self.message = message
        self.log_file = log_file

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
    EmrStepState.COMPLETED,
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
    return EMR_TO_APPLICATION_VERSION[get_emr_version(cluster.release_label)]
