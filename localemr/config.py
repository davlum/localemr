import docker
import os


def is_true(bool_thing) -> bool:
    return bool_thing if isinstance(bool_thing, bool) else bool_thing == 'True'


class Configuration:

    def __init__(self):
        # If true, adds the necessary configuration to use a mocked instance of S3
        # based on https://github.com/sumitsu/s3_mocktest_demo
        self.convert_to_mock_s3 = is_true(os.environ.get('CONVERT_TO_MOCK_S3', True))
        # Number of log file lines to return in the response
        self.livy_log_file_lines = int(os.environ.get('LIVY_LOG_FILE_LINES', 100))
        # Which directories livy can read files from
        # Default is extremely permissive as this is intended for development
        self.local_dir_whitelist = os.environ.get('LOCAL_DIR_WHITELIST', '/')
        # The host where the S3 endpoint is
        self.s3_endpoint = os.environ.get('S3_ENDPOINT', None)
        # docker base url
        docker_base_url = os.environ.get('DOCKER_BASE_URL', 'unix://var/run/docker.sock')
        self.client = docker.DockerClient(base_url=docker_base_url)
        # The container name of the localemr container
        self.localemr_container_name = os.environ.get('LOCALEMR_CONTAINER_NAME', 'localemr')

        self.localemr_aws_access_key_id = os.environ.get('localemr_aws_access_key_id ', 'TESTING')
        self.localemr_aws_secret_access_key = os.environ.get('localemr_aws_secret_access_key ', 'TESTING')
        self.localemr_aws_default_region = os.environ.get('localemr_aws_default_region ', 'us-east-1')
        # TODO: This feature
        # # a comma separated string of the form <cluster_name>:<emr_release_label>,...
        # # example;
        # #       cluster-1:emr-5.29.0,cluster-2:emr-5.24.0
        # unparsed_boot_clusters = os.environ.get('BOOT_CLUSTER_LIST', '')
        # self.boot_clusters = list(map(lambda x: tuple(x.split(':')), unparsed_boot_clusters.split(',')))


configuration = Configuration()
