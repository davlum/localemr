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
    PENDING = 'PENDING          '
    CANCEL_PENDING = 'CANCEL_PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    CANCELLED = 'CANCELLED'
    FAILED = 'FAILED'
    INTERRUPTED = 'INTERRUPTED'


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


EMR_VERSION_TO_APPLICATION_VERSION = {
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
