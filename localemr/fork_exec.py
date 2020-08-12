"""
This class abstracts forking a process. This allows for multiple backends.
The first backend supported was Docker. This had some advantages, as the Docker container could have all
the necessary dependencies, but it has the disadvantage of requiring Docker.
"""

import time
import logging
import traceback
from copy import deepcopy
from datetime import datetime
from multiprocessing import Queue
import pytz
from xml.sax.saxutils import escape
from localemr.config import Configuration
from localemr.common import (
    FailureDetails,
    LocalFakeStep,
    EmrStepState,
    EmrClusterState,
    ClusterSubset,
)


def make_step_terminal(step: LocalFakeStep, failure_details: FailureDetails, state: EmrStepState) -> LocalFakeStep:
    step = deepcopy(step)
    step.failure_details = failure_details
    step.state = state
    step.end_datetime = datetime.now(pytz.utc)
    return step


def process_step(config: Configuration, step: LocalFakeStep, status_queue):
    step.state = EmrStepState.RUNNING
    step.start()
    try:
        status_queue.put(step)
        spark_result = config.exec_impl.exec_process(config, step)
        step = make_step_terminal(step, spark_result.failure_details, spark_result.state)
        status_queue.put(step)

    # pylint: disable=broad-except
    except Exception as e:
        failure_details = FailureDetails(
            reason='Unknown Reason',
            message=escape(traceback.format_exc())
        )
        step = make_step_terminal(step, failure_details, EmrStepState.FAILED)
        logging.exception(e)
        status_queue.put(step)


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
                step = step_process_queue.get()
                process_step(config, step, step_status_queue)
        else:
            cluster_subset = cluster_process_queue.get()
            if cluster_subset.state == EmrClusterState.STARTING:
                config.fork_impl.create_process(config, cluster_subset, cluster_status_queue)
            elif cluster_subset.state == EmrClusterState.TERMINATING:
                config.fork_impl.terminate_process(config, cluster_subset, cluster_status_queue)
                return
            else:
                raise ValueError(
                    "Should only be processing cluster actions on States; "
                    "STARTING and TERMINATING. State is; {}".format(cluster_subset.state)
                )
