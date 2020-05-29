from __future__ import unicode_literals
from datetime import datetime

import pytz
import functools
from typing import List
from boto3 import Session

import re
import time
import logging
import traceback
from copy import deepcopy
import docker
from docker.errors import NotFound, APIError
from distutils.version import StrictVersion
from multiprocessing import Queue, Process
from xml.sax.saxutils import escape
from moto.emr.exceptions import EmrError
from moto.emr.models import (
    FakeStep,
    FakeCluster,
    ElasticMapReduceBackend
)
from localemr.livy.backend import send_step_to_livy
from localemr.config import configuration, Configuration
from localemr.common import (
    FailureDetails,
    EmrClusterState,
    EmrStepState,
    EMR_CLUSTER_TERMINAL_STATES,
    SparkResult,
    EMR_TO_APPLICATION_VERSION,
)


class LocalFakeStep(FakeStep):
    def __init__(self, hostname, **kwargs):
        super().__init__(**kwargs)
        self.failure_details = FailureDetails()
        self.hostname = hostname

    def start(self):
        self.start_datetime = datetime.now(pytz.utc)


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


class LocalFakeCluster(FakeCluster):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.step_process_queue = Queue()
        self.step_status_queue = Queue()

        self.cluster_process_queue = Queue()
        self.cluster_status_queue = Queue()
        self.release_label = self.release_label or 'emr-' + list(EMR_TO_APPLICATION_VERSION.keys())[-1]

    def run_bootstrap_actions(self):
        self.ready_datetime = datetime.now(pytz.utc)

    def terminate_on_no_steps(self):
        if not self.steps and self.state not in EMR_CLUSTER_TERMINAL_STATES:
            if not self.keep_job_flow_alive_when_no_steps:
                self.terminate()

    def terminate(self):
        self.state = EmrClusterState.TERMINATING
        self.cluster_process_queue.put(self.create_cluster_subset())

    def add_steps(self, steps):
        added_steps = []
        for step in steps:
            fake = LocalFakeStep(state=EmrStepState.PENDING, hostname=self.name, **step)
            self.step_process_queue.put(fake)
            self.steps.append(fake)
            added_steps.append(fake)
        return added_steps

    def update_with_cluster_subset(self, cluster_subset: ClusterSubset):
        self.state = cluster_subset.state or self.state
        self.start_datetime = cluster_subset.start_datetime or self.start_datetime
        self.ready_datetime = cluster_subset.ready_datetime or self.ready_datetime
        self.end_datetime = cluster_subset.end_datetime or self.end_datetime

    def create_cluster_subset(self) -> ClusterSubset:
        return ClusterSubset(
            name=self.name,
            release_label=self.release_label,
            state=self.state,
            start_datetime=self.start_datetime,
            ready_datetime=self.ready_datetime,
            end_datetime=self.end_datetime
        )


class MultiProcessing:

    @staticmethod
    def process_spark_command(config: Configuration, hostname: str, cli_args: List[str]) -> SparkResult:
        return send_step_to_livy(config, hostname, cli_args)

    @staticmethod
    def make_step_terminal(step: FakeStep, failure_details: FailureDetails, state: EmrStepState) -> FakeStep:
        step = deepcopy(step)
        step.failure_details = failure_details
        step.state = state
        step.end_datetime = datetime.now(pytz.utc)
        return step

    @staticmethod
    def process_step(config: Configuration, step: FakeStep, status_queue):
        step.state = EmrStepState.RUNNING
        step.start()
        try:
            status_queue.put(step)
            hostname = 'http://{}:8998'.format(step.hostname)
            spark_result = MultiProcessing.process_spark_command(config, hostname, step.args)
            step = MultiProcessing.make_step_terminal(step, spark_result.failure_details, spark_result.state)
            status_queue.put(step)

        except Exception as e:
            failure_details = FailureDetails(
                reason='Unknown Reason',
                message=escape(traceback.format_exc())
            )
            step = MultiProcessing.make_step_terminal(step, failure_details, EmrStepState.FAILED)
            logging.exception(e)
            status_queue.put(step)

    @staticmethod
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

    @staticmethod
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
        emr_version = MultiProcessing.parse_release_label(cluster_release_label)
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

    @staticmethod
    def get_localemr_container(config: Configuration):
        try:
            return config.client.containers.get(config.localemr_container_name)
        except NotFound as e:
            message = "Container %s not found, most likely the container name was " \
                      "changed without changing the env; LOCALEMR_CONTAINER_NAME"
            raise NotFound(message, config.localemr_container_name) from e

    @staticmethod
    def run_livy_container(client: docker.client, container_image, container_args):
        try:
            return client.containers.run(container_image, **container_args)
        except APIError as e:
            if e.status_code == 409:
                msg = e.response.json()['message']
                maybe_container_id = re.findall(r'container "(\w+)"', msg)
                if len(maybe_container_id) != 1:
                    raise e
                client.containers.get(maybe_container_id[0]).remove(v=True, force=True)
                return client.containers.run(container_image, **container_args)
            else:
                raise e

    @staticmethod
    def terminate_cluster(config: Configuration, cluster: ClusterSubset, status_queue: Queue):
        try:
            config.client.containers.get(cluster.name).remove(v=True, force=True)
        except NotFound as e:
            if e.status_code == 404:
                logging.exception("Container %s not found, could not remove", cluster.name)
            else:
                raise e
        cluster.end_datetime = datetime.now(pytz.utc)
        cluster.state = EmrClusterState.TERMINATED
        status_queue.put(cluster)

    @staticmethod
    def create_cluster(config: Configuration, cluster: ClusterSubset, status_queue: Queue):
        localemr_container = MultiProcessing.get_localemr_container(config)

        application_versions = EMR_TO_APPLICATION_VERSION[MultiProcessing.get_emr_version(cluster.release_label)]
        container_image = 'davlum/localemr-container:0.5.0-spark{}'.format(application_versions['Spark'])
        env = {
            'SPARK_MASTER': 'local[*]',
            'DEPLOY_MODE': 'client',
            'LOCAL_DIR_WHITELIST': config.local_dir_whitelist,
        }
        container_args = {
            'name': cluster.name,
            # This binds the container's port 8998 to a random port in the host
            'ports': {'8998/tcp': None},
            'detach': True,
            'environment': env,
        }
        livy_container = MultiProcessing.run_livy_container(config.client, container_image, container_args)
        # TODO: Fix this dirty hack. We're just taking the first network that is attached to the container
        localemr_networks = localemr_container.attrs['NetworkSettings']['Networks']
        localemr_network_name = list(localemr_networks.keys())[0]
        localemr_network = config.client.networks.get(localemr_network_name)
        localemr_network.connect(livy_container)
        cluster.run_bootstrap_actions()
        status_queue.put(cluster)

    @staticmethod
    def read_task_queue(
            config: Configuration,
            step_process_queue: Queue,
            step_status_queue: Queue,
            cluster_process_queue: Queue,
            cluster_status_queue: Queue):
        while True:
            if cluster_process_queue.empty():
                if step_process_queue.empty():
                    cluster_status_queue.put(ClusterSubset(state=EmrClusterState.WAITING))
                    time.sleep(4)
                else:
                    cluster_status_queue.put(ClusterSubset(state=EmrClusterState.RUNNING))
                    step_or_cluster = step_process_queue.get()
                    MultiProcessing.process_step(config, step_or_cluster, step_status_queue)
            else:
                cluster_subset = cluster_process_queue.get()
                if cluster_subset.state == EmrClusterState.STARTING:
                    MultiProcessing.create_cluster(config, cluster_subset, cluster_status_queue)
                elif cluster_subset.state == EmrClusterState.TERMINATING:
                    MultiProcessing.terminate_cluster(config, cluster_subset, cluster_status_queue)
                    return
                else:
                    raise ValueError(
                        "Should only be processing cluster actions on States; "
                        "STARTING and TERMINATING. State is; %s", cluster_subset.state
                    )


def update_wrapper(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        for cluster_id in self.clusters.keys():
            self.update_steps_and_cluster(cluster_id)
        return func(self, *args, **kwargs)
    return wrapper


class LocalElasticMapReduceBackend(ElasticMapReduceBackend):

    def __init__(self, region_name):
        super().__init__(region_name)

    def update_steps_and_cluster(self, cluster_id):
        steps: List[FakeStep] = self.clusters[cluster_id].steps
        cluster_status_queue: Queue = self.clusters[cluster_id].cluster_status_queue
        step_status_queue: Queue = self.clusters[cluster_id].step_status_queue
        while not step_status_queue.empty():
            updated_step = step_status_queue.get()
            steps = [updated_step if step.id == updated_step.id else step for step in steps]
        while not cluster_status_queue.empty():
            updated_cluster: ClusterSubset = cluster_status_queue.get()
            self.clusters[cluster_id].update_with_cluster_subset(updated_cluster)
            self.clusters[cluster_id].terminate_on_no_steps()
        self.clusters[cluster_id].steps = steps

    def run_job_flow(self, **kwargs):
        fake_cluster = LocalFakeCluster(emr_backend=self, **kwargs)
        fake_cluster.cluster_process_queue.put(fake_cluster.create_cluster_subset())
        step_reader_process = Process(
            target=MultiProcessing.read_task_queue,
            args=(
                configuration,
                fake_cluster.step_process_queue,
                fake_cluster.step_status_queue,
                fake_cluster.cluster_process_queue,
                fake_cluster.cluster_status_queue,
            )
        )
        step_reader_process.daemon = True
        step_reader_process.start()
        return fake_cluster

    add_applcations = update_wrapper(ElasticMapReduceBackend.add_applications)
    add_job_flow_steps = update_wrapper(ElasticMapReduceBackend.add_job_flow_steps)
    add_tags = update_wrapper(ElasticMapReduceBackend.add_tags)
    describe_job_flows = update_wrapper(ElasticMapReduceBackend.describe_job_flows)
    describe_step = update_wrapper(ElasticMapReduceBackend.describe_step)
    get_cluster = update_wrapper(ElasticMapReduceBackend.get_cluster)
    list_bootstrap_actions = update_wrapper(ElasticMapReduceBackend.list_bootstrap_actions)
    list_clusters = update_wrapper(ElasticMapReduceBackend.list_clusters)
    list_steps = update_wrapper(ElasticMapReduceBackend.list_steps)
    modify_instance_groups = update_wrapper(ElasticMapReduceBackend.modify_instance_groups)
    remove_tags = update_wrapper(ElasticMapReduceBackend.remove_tags)
    set_visible_to_all_users = update_wrapper(ElasticMapReduceBackend.set_visible_to_all_users)
    set_termination_protection = update_wrapper(ElasticMapReduceBackend.set_termination_protection)
    terminate_job_flows = update_wrapper(ElasticMapReduceBackend.terminate_job_flows)


emr_backends = {}

for region in Session().get_available_regions("emr"):
    emr_backends[region] = LocalElasticMapReduceBackend(region)
for region in Session().get_available_regions("emr", partition_name="aws-us-gov"):
    emr_backends[region] = LocalElasticMapReduceBackend(region)
for region in Session().get_available_regions("emr", partition_name="aws-cn"):
    emr_backends[region] = LocalElasticMapReduceBackend(region)
