import boto3
import os


def is_true(bool_thing) -> bool:
    return bool_thing if isinstance(bool_thing, bool) else bool_thing == 'True'


class Configuration:

    def __init__(self):
        # If true, will fetch files with an S3 prefix. Requires authentication.
        self.fetch_from_s3 = is_true(os.environ.get('FETCH_FROM_S3', False))
        #  'If true, converts any paths on a submitted step from S3 to local directories.'
        self.convert_s3_to_local = is_true(os.environ.get('CONVERT_S3_TO_LOCAL', False))
        # Where the files should be stored
        self.local_dir = os.environ.get('LOCAL_DIR', '/tmp/localemr/')
        # Whether to use temporary directories
        self.use_tmp_dir = os.environ.get('USE_TMP_DIR', False)
        # If true dynamically bring up the local emr clusters
        # TODO: There is no difference between dynamic and static clusters.
        # Static clusters are just containers that are brought up at the same time as the localemr container
        # Maybe specify these clusters in a parseable env var or conf file
        self.dynamic_clusters = is_true(os.environ.get('DYNAMIC_CLUSTERS', False))
        # The host where the Apache Livy server is running, only relevant when DYNAMIC_CLUSTERS=False
        self.emr_host = os.environ.get('EMR_HOST', 'livy')
        if self.dynamic_clusters:
            self.emr_host = ''
        # Max number of files to fetch from S3
        self.max_fetch_from_s3 = int(os.environ.get('MAX_FETCH_FROM_S3', 5))
        # Number of log file lines to return in the response
        self.livy_log_file_lines = int(os.environ.get('MAX_FETCH_FROM_S3', 100))
        # TODO: This should be a separate service ro-s3
        if self.fetch_from_s3:
            # The host where the S3 endpoint is
            self.s3 = boto3.client('s3', endpoint_url=os.environ.get('S3_HOST', None))

        self.localemr_container_name = os.environ.get('LOCALEMR_CONTAINER_NAME', 'localemr')
        # TODO: Volumes shouldn't matter to the end user so figure out a way to get rid of this
        self.localemr_volume_name = os.environ.get('LOCALEMR_VOLUME_NAME', 'localemr')


configuration = Configuration()
