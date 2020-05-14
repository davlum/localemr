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
        # If local_dir not specified temporary directories will be used
        self.local_dir = os.environ.get('LOCAL_DIR', None)
        # The host where the Apache Livy server is running
        self.livy_host = os.environ.get('LIVY_HOST', 'http://livy:8998')
        # Max number of files to fetch from S3
        self.max_fetch_from_s3 = int(os.environ.get('MAX_FETCH_FROM_S3', 5))
        # Number of log file lines to return in the response
        self.livy_log_file_lines = int(os.environ.get('MAX_FETCH_FROM_S3', 100))
        if self.fetch_from_s3:
            # The host where the S3 endpoint is
            self.s3 = boto3.client('s3', endpoint_url=os.environ.get('S3_HOST', None))
        # If true dynamically bring up the local emr clusters
        self.dynamic_clusters = is_true(os.environ.get('DYNAMIC_CLUSTERS', False))


config = Configuration()
