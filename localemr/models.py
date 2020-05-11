from localemr.emr.models import EMRStepStates, FailureDetails


class SparkResult:
    def __init__(self, state: EMRStepStates, failure_details: FailureDetails):
        self.state = state
        self.failure_details = failure_details
