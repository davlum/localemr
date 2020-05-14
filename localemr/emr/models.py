from __future__ import unicode_literals
from datetime import datetime, timedelta


import pytz
from typing import List
from boto3 import Session
from dateutil.parser import parse as dtparse
from moto.core import BaseBackend, BaseModel
from moto.emr.exceptions import EmrError
from .utils import random_instance_group_id, random_cluster_id, random_step_id

import re
import time
import logging
import traceback
import tempfile
from copy import deepcopy
import docker
from docker.errors import NotFound, APIError
from distutils.version import StrictVersion
from multiprocessing import Queue, Process
from xml.sax.saxutils import escape
from localemr.s3.convert_to_local import get_files_from_s3, convert_s3_to_local_path
from localemr.livy.backend import send_step_to_livy
from localemr.models import SparkResult
from localemr.config import Configuration
from localemr.models import EmrStepState, FailureDetails, EmrClusterState, EMR_VERSION_TO_APPLICATION_VERSION
from localemr.config import configuration


class FakeApplication(BaseModel):
    def __init__(self, name, version, args=None, additional_info=None):
        self.additional_info = additional_info or {}
        self.args = args or []
        self.name = name
        self.version = version


class FakeBootstrapAction(BaseModel):
    def __init__(self, args, name, script_path):
        self.args = args or []
        self.name = name
        self.script_path = script_path


class FakeInstanceGroup(BaseModel):
    def __init__(
        self,
        instance_count,
        instance_role,
        instance_type,
        market="ON_DEMAND",
        name=None,
        id=None,
        bid_price=None,
        ebs_configuration=None,
    ):
        self.id = id or random_instance_group_id()

        self.bid_price = bid_price
        self.market = market
        if name is None:
            if instance_role == "MASTER":
                name = "master"
            elif instance_role == "CORE":
                name = "slave"
            else:
                name = "Task instance group"
        self.name = name
        self.num_instances = instance_count
        self.role = instance_role
        self.type = instance_type
        self.ebs_configuration = ebs_configuration

        self.creation_datetime = datetime.now(pytz.utc)
        self.start_datetime = datetime.now(pytz.utc)
        self.ready_datetime = datetime.now(pytz.utc)
        self.end_datetime = None
        self.state = "RUNNING"

    def set_instance_count(self, instance_count):
        self.num_instances = instance_count


class FakeStep(BaseModel):
    def __init__(
        self,
        state,
        hostname,
        name="",
        jar="",
        args=None,
        properties=None,
        action_on_failure="TERMINATE_CLUSTER",
    ):
        self.id = random_step_id()

        self.action_on_failure = action_on_failure
        self.args = args or []
        self.name = name
        self.jar = jar
        self.properties = properties or {}

        self.creation_datetime = datetime.now(pytz.utc)
        self.end_datetime = None
        self.ready_datetime = None
        self.start_datetime = None
        self.state = state
        self.failure_details = FailureDetails()

        self.hostname = hostname

    def start(self):
        self.start_datetime = datetime.now(pytz.utc)


class ClusterSubset:
    def __init__(self, name, release_label, state, start_datetime, ready_datetime, end_datetime):
        self.name = name
        self.release_label = release_label
        self.state = state
        self.start_datetime = start_datetime
        self.ready_datetime = ready_datetime
        self.end_datetime = end_datetime

    def run_bootstrap_actions(self):
        self.ready_datetime = datetime.now(pytz.utc)
        self.state = EmrClusterState.WAITING


class FakeCluster(BaseModel):

    def __init__(
        self,
        emr_backend,
        name,
        log_uri,
        job_flow_role,
        service_role,
        steps,
        instance_attrs,
        bootstrap_actions=None,
        configurations=None,
        cluster_id=None,
        visible_to_all_users="false",
        release_label=None,
        requested_ami_version=None,
        running_ami_version=None,
        custom_ami_id=None,
    ):
        self.id = cluster_id or random_cluster_id()
        emr_backend.clusters[self.id] = self
        self.emr_backend = emr_backend

        self.applications = []

        self.bootstrap_actions = []
        for bootstrap_action in bootstrap_actions or []:
            self.add_bootstrap_action(bootstrap_action)

        self.configurations = configurations or []

        self.tags = {}

        self.log_uri = log_uri
        self.name = name
        self.normalized_instance_hours = 0

        self.steps = []
        self.add_steps(steps)

        self.visible_to_all_users = visible_to_all_users

        self.instance_group_ids = []
        self.master_instance_group_id = None
        self.core_instance_group_id = None
        if "master_instance_type" in instance_attrs and instance_attrs["master_instance_type"]:
            self.emr_backend.add_instance_groups(
                self.id,
                [
                    {
                        "instance_count": 1,
                        "instance_role": "MASTER",
                        "instance_type": instance_attrs["master_instance_type"],
                        "market": "ON_DEMAND",
                        "name": "master",
                    }
                ],
            )
        if "slave_instance_type" in instance_attrs and instance_attrs["slave_instance_type"]:
            self.emr_backend.add_instance_groups(
                self.id,
                [
                    {
                        "instance_count": instance_attrs["instance_count"] - 1,
                        "instance_role": "CORE",
                        "instance_type": instance_attrs["slave_instance_type"],
                        "market": "ON_DEMAND",
                        "name": "slave",
                    }
                ],
            )
        self.additional_master_security_groups = instance_attrs.get(
            "additional_master_security_groups"
        )
        self.additional_slave_security_groups = instance_attrs.get(
            "additional_slave_security_groups"
        )
        self.availability_zone = instance_attrs.get("availability_zone")
        self.ec2_key_name = instance_attrs.get("ec2_key_name")
        self.ec2_subnet_id = instance_attrs.get("ec2_subnet_id")
        self.hadoop_version = instance_attrs.get("hadoop_version")
        self.keep_job_flow_alive_when_no_steps = instance_attrs.get(
            "keep_job_flow_alive_when_no_steps"
        )
        self.master_security_group = instance_attrs.get(
            "emr_managed_master_security_group"
        )
        self.service_access_security_group = instance_attrs.get(
            "service_access_security_group"
        )
        self.slave_security_group = instance_attrs.get(
            "emr_managed_slave_security_group"
        )
        self.termination_protected = instance_attrs.get("termination_protected")

        self.release_label = release_label
        self.requested_ami_version = requested_ami_version
        self.running_ami_version = running_ami_version
        self.custom_ami_id = custom_ami_id

        self.role = job_flow_role or "EMRJobflowDefault"
        self.service_role = service_role

        self.creation_datetime = datetime.now(pytz.utc)
        self.start_datetime = None
        self.ready_datetime = None
        self.end_datetime = None
        self.state = None

        self.start_cluster()
        self.run_bootstrap_actions()
        if self.steps:
            self.steps[0].start()

        self.process_queue = Queue()
        self.status_queue = Queue()

    @property
    def instance_groups(self):
        return self.emr_backend.get_instance_groups(self.instance_group_ids)

    @property
    def master_instance_type(self):
        return self.emr_backend.instance_groups[self.master_instance_group_id].type

    @property
    def slave_instance_type(self):
        return self.emr_backend.instance_groups[self.core_instance_group_id].type

    @property
    def instance_count(self):
        return sum(group.num_instances for group in self.instance_groups)

    def start_cluster(self):
        self.state = EmrClusterState.STARTING
        self.start_datetime = datetime.now(pytz.utc)

    def run_bootstrap_actions(self):
        self.state = EmrClusterState.BOOTSTRAPPING
        self.ready_datetime = datetime.now(pytz.utc)
        self.state = EmrClusterState.WAITING
        if not self.steps:
            if not self.keep_job_flow_alive_when_no_steps:
                self.terminate()

    def terminate(self):
        # TODO THIS
        self.state = EmrClusterState.TERMINATING
        self.end_datetime = datetime.now(pytz.utc)
        self.state = EmrClusterState.TERMINATED

    def add_applications(self, applications):
        self.applications.extend(
            [
                FakeApplication(
                    name=app.get("name", ""),
                    version=app.get("version", ""),
                    args=app.get("args", []),
                    additional_info=app.get("additiona_info", {}),
                )
                for app in applications
            ]
        )

    def add_bootstrap_action(self, bootstrap_action):
        self.bootstrap_actions.append(FakeBootstrapAction(**bootstrap_action))

    def add_instance_group(self, instance_group):
        if instance_group.role == "MASTER":
            if self.master_instance_group_id:
                raise Exception("Cannot add another master instance group")
            self.master_instance_group_id = instance_group.id
        if instance_group.role == "CORE":
            if self.core_instance_group_id:
                raise Exception("Cannot add another core instance group")
            self.core_instance_group_id = instance_group.id
        self.instance_group_ids.append(instance_group.id)

    def add_steps(self, steps):
        added_steps = []
        for step in steps:
            hostname = configuration.emr_host if configuration.emr_host else self.name
            fake = FakeStep(state=EmrStepState.PENDING, hostname=hostname, **step)
            self.process_queue.put(fake)
            self.steps.append(fake)
            added_steps.append(fake)
        return added_steps

    def add_tags(self, tags):
        self.tags.update(tags)

    def remove_tags(self, tag_keys):
        for key in tag_keys:
            self.tags.pop(key, None)

    def set_termination_protection(self, value):
        self.termination_protected = value

    def set_visibility(self, visibility):
        self.visible_to_all_users = visibility

    def update_with_cluster_subset(self, cluster_subset: ClusterSubset):
        self.state = cluster_subset.state
        self.start_datetime = cluster_subset.start_datetime
        self.ready_datetime = cluster_subset.ready_datetime
        self.end_datetime = cluster_subset.end_datetime

    def create_cluster_subset(self) -> ClusterSubset:
        return ClusterSubset(
            name=self.name,
            release_label=self.release_label,
            state=self.state,
            start_datetime=self.start_datetime,
            ready_datetime=self.ready_datetime,
            end_datetime=self.end_datetime
        )


class DockerNetwork:
    __network = None

    def __init__(self, client: docker.client):
        if self.__network is None:
            self.__network = client.networks.create('localemr')

    @property
    def network(self):
        return self.__network


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
            cli_args = step.args
            with tempfile.TemporaryDirectory(prefix=config.local_dir) as tmp_dir_name:
                dir_name = tmp_dir_name if config.use_tmp_dir else config.local_dir

                if config.fetch_from_s3:
                    get_files_from_s3(config, dir_name, cli_args)

                if config.convert_s3_to_local:
                    cli_args = convert_s3_to_local_path(dir_name, cli_args)

                hostname = 'http://{}:8998'.format(step.hostname)
                spark_result = MultiProcessing.process_spark_command(config, hostname, cli_args)
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
        try:
            emr_version = re.findall(r'emr-(\d+\.\d+\.\d+)', cluster_release_label)[0]
        except IndexError as e:
            aws_docs = 'https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html'
            raise ValueError(
                "%s is not a valid emr release label. See %s for more info",
                cluster_release_label, aws_docs
            ) from e
        parsed_emr_version = StrictVersion(emr_version)
        versions = list(EMR_VERSION_TO_APPLICATION_VERSION.keys())
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
    def process_cluster(config: Configuration, cluster: ClusterSubset, status_queue: Queue):
        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        env = {
            'SPARK_MASTER': 'local[*]',
            'DEPLOY_MODE': 'client',
        }
        # This binds the containers port 8998 to a random port in the host
        ports = {'8998/tcp': None}
        try:
            localemr_container = client.containers.get(config.localemr_container_name)
        except NotFound as e:
            message = "Container %s not found, most likely the container name was " \
                      "changed without changing the env; LOCALEMR_CONTAINER_NAME"
            raise NotFound(message, config.localemr_container_name) from e

        try:
            localemr_volume = list(filter(lambda v: config.localemr_volume_name in v.name, client.volumes.list()))[0]
        except IndexError as e:
            message = "Volume %s not found, most likely the volume name was " \
                      "changed without changing the env; LOCALEMR_VOLUME_NAME"
            raise IndexError(message, config.localemr_volume_name) from e

        volume_bind = {localemr_volume.name: {'bind': '/tmp/localemr', 'mode': 'rw'}}

        application_versions = EMR_VERSION_TO_APPLICATION_VERSION[
            MultiProcessing.get_emr_version(cluster.release_label)]
        container_image = 'davlum/livy:0.5.0-spark{}'.format(application_versions['Spark'])

        try:
            livy_container = client.containers.run(
                container_image,
                name=cluster.name,
                ports=ports,
                environment=env,
                detach=True,
                volumes=volume_bind
            )
        except APIError as e:
            if e.status_code == 409:
                msg = e.response.json()['message']
                maybe_container_id = re.findall(r'container "(\w+)"', msg)
                if len(maybe_container_id) != 1:
                    raise e
                client.containers.get(maybe_container_id[0]).remove(v=True, force=True)
                livy_container = client.containers.run(
                    container_image,
                    name=cluster.name,
                    ports=ports,
                    environment=env,
                    detach=True,
                    volumes=volume_bind
                )
            else:
                raise e

        docker_network = DockerNetwork(client).network
        docker_network.connect(livy_container)
        docker_network.connect(localemr_container)
        cluster.run_bootstrap_actions()
        status_queue.put(cluster)

    @staticmethod
    def read_task_queue(config: Configuration, process_queue: Queue, status_queue: Queue):
        while True:
            if process_queue.empty():
                # TODO: Change the state of the cluster to WAITING here
                time.sleep(4)
            else:
                # TODO: Change the state of the cluster to RUNNING here
                step_or_cluster = process_queue.get()
                if isinstance(step_or_cluster, FakeStep):
                    MultiProcessing.process_step(config, step_or_cluster, status_queue)
                elif isinstance(step_or_cluster, ClusterSubset):
                    MultiProcessing.process_cluster(config, step_or_cluster, status_queue)
                else:
                    raise ValueError("DA FUQ IS IT")


class ElasticMapReduceBackend(BaseBackend):

    def __init__(self, region_name):
        super(ElasticMapReduceBackend, self).__init__()
        self.region_name = region_name
        self.clusters = {}
        self.instance_groups = {}

    def reset(self):
        region_name = self.region_name
        self.__dict__ = {}
        self.__init__(region_name)

    def add_applications(self, cluster_id, applications):
        cluster = self.get_cluster(cluster_id)
        cluster.add_applications(applications)

    def add_instance_groups(self, cluster_id, instance_groups):
        cluster = self.clusters[cluster_id]
        result_groups = []
        for instance_group in instance_groups:
            group = FakeInstanceGroup(**instance_group)
            self.instance_groups[group.id] = group
            cluster.add_instance_group(group)
            result_groups.append(group)
        return result_groups

    def add_job_flow_steps(self, job_flow_id, steps):
        cluster = self.clusters[job_flow_id]
        steps = cluster.add_steps(steps)
        return steps

    def add_tags(self, cluster_id, tags):
        cluster = self.get_cluster(cluster_id)
        cluster.add_tags(tags)

    def describe_job_flows(
        self,
        job_flow_ids=None,
        job_flow_states=None,
        created_after=None,
        created_before=None,
    ):
        clusters = self.clusters.values()

        within_two_month = datetime.now(pytz.utc) - timedelta(days=60)
        clusters = [c for c in clusters if c.creation_datetime >= within_two_month]

        if job_flow_ids:
            clusters = [c for c in clusters if c.id in job_flow_ids]
        if job_flow_states:
            clusters = [c for c in clusters if c.state in job_flow_states]
        if created_after:
            created_after = dtparse(created_after)
            clusters = [c for c in clusters if c.creation_datetime > created_after]
        if created_before:
            created_before = dtparse(created_before)
            clusters = [c for c in clusters if c.creation_datetime < created_before]

        # Amazon EMR can return a maximum of 512 job flow descriptions
        return sorted(clusters, key=lambda x: x.id)[:512]

    def describe_step(self, cluster_id, step_id):
        steps = self.update_steps_and_clusters(cluster_id)
        for step in steps:
            if step.id == step_id:
                return step

    def get_cluster(self, cluster_id):
        if cluster_id in self.clusters:
            self.update_steps_and_clusters(cluster_id)
            return self.clusters[cluster_id]
        raise EmrError("ResourceNotFoundException", "", "error_json")

    def get_instance_groups(self, instance_group_ids):
        return [
            group
            for group_id, group in self.instance_groups.items()
            if group_id in instance_group_ids
        ]

    def list_bootstrap_actions(self, cluster_id, marker=None):
        max_items = 50
        actions = self.clusters[cluster_id].bootstrap_actions
        start_idx = 0 if marker is None else int(marker)
        marker = (
            None
            if len(actions) <= start_idx + max_items
            else str(start_idx + max_items)
        )
        return actions[start_idx: start_idx + max_items], marker

    def list_clusters(
        self, cluster_states=None, created_after=None, created_before=None, marker=None
    ):
        max_items = 50
        clusters = self.clusters.values()
        if cluster_states:
            clusters = [c for c in clusters if c.state in cluster_states]
        if created_after:
            created_after = dtparse(created_after)
            clusters = [c for c in clusters if c.creation_datetime > created_after]
        if created_before:
            created_before = dtparse(created_before)
            clusters = [c for c in clusters if c.creation_datetime < created_before]
        clusters = sorted(clusters, key=lambda x: x.id)
        start_idx = 0 if marker is None else int(marker)
        marker = (
            None
            if len(clusters) <= start_idx + max_items
            else str(start_idx + max_items)
        )
        return clusters[start_idx: start_idx + max_items], marker

    def list_instance_groups(self, cluster_id, marker=None):
        max_items = 50
        groups = sorted(self.clusters[cluster_id].instance_groups, key=lambda x: x.id)
        start_idx = 0 if marker is None else int(marker)
        marker = (
            None if len(groups) <= start_idx + max_items else str(start_idx + max_items)
        )
        return groups[start_idx: start_idx + max_items], marker

    def update_steps_and_clusters(self, cluster_id) -> List[FakeStep]:
        steps: List[FakeStep] = self.clusters[cluster_id].steps
        status_queue: Queue = self.clusters[cluster_id].status_queue
        while not status_queue.empty():
            updated_step_or_cluster = status_queue.get()
            if isinstance(updated_step_or_cluster, FakeStep):
                steps = [updated_step_or_cluster if step.id == updated_step_or_cluster.id else step for step in steps]
            if isinstance(updated_step_or_cluster, FakeCluster):
                self.clusters[cluster_id].state = updated_step_or_cluster.state
                self.clusters[cluster_id].ready_datetime = updated_step_or_cluster.ready_datetime
        self.clusters[cluster_id].steps = steps
        return steps

    def list_steps(self, cluster_id, marker=None, step_ids=None, step_states=None):
        max_items = 50
        steps = self.update_steps_and_clusters(cluster_id)
        if step_ids:
            steps = [s for s in steps if s.id in step_ids]
        if step_states:
            steps = [s for s in steps if s.state in step_states]
        start_idx = 0 if marker is None else int(marker)
        marker = (
            None if len(steps) <= start_idx + max_items else str(start_idx + max_items)
        )
        return steps[start_idx: start_idx + max_items], marker

    def modify_instance_groups(self, instance_groups):
        result_groups = []
        for instance_group in instance_groups:
            group = self.instance_groups[instance_group["instance_group_id"]]
            group.set_instance_count(int(instance_group["instance_count"]))
        return result_groups

    def remove_tags(self, cluster_id, tag_keys):
        cluster = self.get_cluster(cluster_id)
        cluster.remove_tags(tag_keys)

    def run_job_flow(self, **kwargs):
        fake_cluster = FakeCluster(self, **kwargs)
        if configuration.dynamic_clusters:
            fake_cluster.process_queue.put(fake_cluster.create_cluster_subset())
        else:
            fake_cluster.run_bootstrap_actions()

        step_reader_process = Process(
            target=MultiProcessing.read_task_queue,
            args=(
                configuration,
                fake_cluster.process_queue,
                fake_cluster.status_queue,
            )
        )
        step_reader_process.daemon = True
        step_reader_process.start()

        return fake_cluster

    def set_visible_to_all_users(self, job_flow_ids, visible_to_all_users):
        for job_flow_id in job_flow_ids:
            cluster = self.clusters[job_flow_id]
            cluster.set_visibility(visible_to_all_users)

    def set_termination_protection(self, job_flow_ids, value):
        for job_flow_id in job_flow_ids:
            cluster = self.clusters[job_flow_id]
            cluster.set_termination_protection(value)

    def terminate_job_flows(self, job_flow_ids):
        # TODO terminate the clusters when they are dynamic
        clusters = []
        for job_flow_id in job_flow_ids:
            cluster = self.clusters[job_flow_id]
            cluster.terminate()
            clusters.append(cluster)
        return clusters


emr_backends = {}
for region in Session().get_available_regions("emr"):
    emr_backends[region] = ElasticMapReduceBackend(region)
for region in Session().get_available_regions("emr", partition_name="aws-us-gov"):
    emr_backends[region] = ElasticMapReduceBackend(region)
for region in Session().get_available_regions("emr", partition_name="aws-cn"):
    emr_backends[region] = ElasticMapReduceBackend(region)
