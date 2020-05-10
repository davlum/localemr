from localemr.emr.models import EMRStepStates, FailureDetails


class SparkResult:
    def __init__(self, state: EMRStepStates, failure_details: FailureDetails):
        self.state = state
        self.failure_details = failure_details


class Configuration:
    def __init__(self,
                 fetch_from_s3=True,
                 convert_s3_to_local=True,
                 local_dir='/tmp/files',
                 livy_host='http://livy:8998',
                 max_fetch_from_s3='1GB'):
        self.fetch_from_s3 = fetch_from_s3
        self.convert_s3_to_local = convert_s3_to_local
        # If local_dir not specified temporary directories will be used
        self.local_dir = local_dir
        self.livy_host = livy_host
        self.max_fetch_from_s3 = max_fetch_from_s3


CONF = Configuration()
