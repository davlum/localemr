import os
import docker
from localemr.fork.interface import ForkInterface
from localemr.exec.interface import ExecInterface


def is_true(bool_thing) -> bool:
    return bool_thing if isinstance(bool_thing, bool) else bool_thing == 'True'


class Configuration:

    def __init__(self):
        self.fork_impl: ForkInterface = self.get_fork_impl()
        self.exec_impl: ExecInterface = self.get_exec_impl()

        # If true, adds the necessary configuration to use a mocked instance of S3
        # based on https://github.com/sumitsu/s3_mocktest_demo
        self.convert_to_mock_s3 = is_true(os.environ.get('CONVERT_TO_MOCK_S3', True))

        # The host where the S3 endpoint is
        self.s3_endpoint = os.environ.get('S3_ENDPOINT', None)
        # docker base url
        # The container name of the localemr container
        self.localemr_container_name = os.environ.get('LOCALEMR_CONTAINER_NAME', 'localemr')
        self.localemr_aws_access_key_id = os.environ.get('LOCALEMR_AWS_ACCESS_KEY_ID', 'TESTING')
        self.localemr_aws_secret_access_key = os.environ.get('LOCALEMR_AWS_SECRET_ACCESS_KEY', 'TESTING')
        self.localemr_aws_default_region = os.environ.get('LOCALEMR_AWS_DEFAULT_REGION', 'us-east-1')
        self.localemr_container_repo = os.environ.get('LOCALEMR_CONTAINER_REPO', 'davlum/localemr-container:0.5.0-spark')

    def get_fork_impl(self) -> ForkInterface:
        fork_impl = os.environ.get('LOCALEMR_FORK_IMPL', 'Docker')
        if fork_impl == 'Docker':
            from localemr.fork.docker.models import Docker
            docker_base_url = os.environ.get('DOCKER_BASE_URL', 'unix://var/run/docker.sock')
            self.client = docker.DockerClient(base_url=docker_base_url)
            return Docker()
        raise ValueError("`LOCALEMR_FORK_IMPL` val not in allowed values: {}".format(fork_impl))

    def get_exec_impl(self) -> ExecInterface:
        exec_impl = os.environ.get('LOCALEMR_EXEC_IMPL', 'Livy')
        if exec_impl == '':
            from localemr.exec.livy.backend import Livy
            # Which directories livy can read files from
            # Default is extremely permissive as this is intended for development
            self.local_dir_whitelist = os.environ.get('LOCAL_DIR_WHITELIST', '/')
            return Livy()
        raise ValueError("`LOCALEMR_FORK_IMPL` val not in allowed values: {}".format(exec_impl))


configuration = Configuration()
