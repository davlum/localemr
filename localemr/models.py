from __future__ import unicode_literals
from datetime import datetime

import functools
from typing import List
from multiprocessing import Queue, Process

import pytz
from boto3 import Session
from moto.emr.models import (
    FakeStep,
    FakeCluster,
    ElasticMapReduceBackend
)
from localemr.config import configuration
from localemr.fork_exec import ForkExec, run_fork_exec
from localemr.common import (
    LocalFakeStep,
    EmrClusterState,
    EmrStepState,
    EMR_CLUSTER_TERMINAL_STATES,
    EMR_TO_APPLICATION_VERSION,
    ClusterSubset
)


class LocalFakeCluster(FakeCluster):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.step_process_queue = Queue()
        self.step_status_queue = Queue()

        self.cluster_process_queue = Queue()
        self.cluster_status_queue = Queue()
        # Use latest release if none is specified
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
            fake = LocalFakeStep(
                state=EmrStepState.PENDING,
                hostname=self.name,
                cluster_id=self.id,
                cluster_name=self.name,
                main_class=step.pop('hadoop_jar_step._main_class', None),
                **step,
            )
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


def update_wrapper(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        for cluster_id in self.clusters.keys():
            self.update_steps_and_cluster(cluster_id)
        return func(self, *args, **kwargs)
    return wrapper


class LocalElasticMapReduceBackend(ElasticMapReduceBackend):

    def __init__(self, region_name):
        super(LocalElasticMapReduceBackend, self).__init__(region_name)
        self.fork_exec = ForkExec(configuration)

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
            target=run_fork_exec,
            args=(
                self.fork_exec,
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
