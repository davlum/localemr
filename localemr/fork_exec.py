"""
This module combines the ForkInterface and ExecInterface which have their implementations determined in Config.
"""

import time
import logging
import traceback
from copy import deepcopy
from datetime import datetime
from multiprocessing import Queue
from xml.sax.saxutils import escape
import pytz
from localemr.config import Configuration
from localemr.fork.interface import ForkInterface
from localemr.exec.interface import ExecInterface
from localemr.common import (
    FailureDetails,
    LocalFakeStep,
    EmrStepState,
    EmrClusterState,
    ClusterSubset,
)


class ForkExec:

    def __init__(self, config: Configuration):
        self.fork = ForkInterface().get_impl(config)
        self.exec = ExecInterface().get_impl(config)


def make_step_terminal(step: LocalFakeStep, failure_details: FailureDetails, state: EmrStepState) -> LocalFakeStep:
    step = deepcopy(step)
    step.failure_details = failure_details
    step.state = state
    step.end_datetime = datetime.now(pytz.utc)
    return step


def process_step(exec_impl: ExecInterface, step: LocalFakeStep, status_queue):
    step.state = EmrStepState.RUNNING
    step.start()
    try:
        status_queue.put(step)
        spark_result = exec_impl.exec_process(step)
        step = make_step_terminal(step, spark_result.failure_details, spark_result.state)
        status_queue.put(step)

    # pylint: disable=broad-except
    except Exception as e:
        failure_details = FailureDetails(
            reason='Unknown Reason',
            message=escape(traceback.format_exc()),
        )
        step = make_step_terminal(step, failure_details, EmrStepState.FAILED)
        logging.exception(e)
        status_queue.put(step)


def run_fork_exec(
        fork_exec: ForkExec,
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
                process_step(fork_exec.exec, step, step_status_queue)
        else:
            cluster_subset = cluster_process_queue.get()
            if cluster_subset.state == EmrClusterState.STARTING:
                fork_exec.fork.create_process(cluster_subset, cluster_status_queue)
            elif cluster_subset.state == EmrClusterState.TERMINATING:
                fork_exec.fork.terminate_process(cluster_subset, cluster_status_queue)
                return
            else:
                raise ValueError(
                    "Should only be processing cluster actions on States; "
                    "STARTING and TERMINATING. State is; {}".format(cluster_subset.state),
                )
