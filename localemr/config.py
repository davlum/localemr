import os


def is_true(bool_thing) -> bool:
    return bool_thing if isinstance(bool_thing, bool) else bool_thing == 'True'


class Configuration:

    def __init__(self):
        self.exec_impl = os.environ.get('LOCALEMR_EXEC_IMPL', 'livy')
        self.fork_impl = os.environ.get('LOCALEMR_FORK_IMPL', 'docker')

        self.docker_base_url = os.environ.get('DOCKER_BASE_URL', 'unix://var/run/docker.sock')
        # The host where the S3 endpoint is
        self.s3_endpoint = os.environ.get('S3_ENDPOINT', 'http://s3:2000')
        # The container name of the localemr container

        self.localemr_container_name = os.environ.get('LOCALEMR_CONTAINER_NAME', 'localemr')
        self.localemr_aws_access_key_id = os.environ.get('LOCALEMR_AWS_ACCESS_KEY_ID', 'TESTING')
        self.localemr_aws_secret_access_key = os.environ.get('LOCALEMR_AWS_SECRET_ACCESS_KEY', 'TESTING')
        self.localemr_aws_default_region = os.environ.get('LOCALEMR_AWS_DEFAULT_REGION', 'us-east-1')
        #self.localemr_container_repo = os.environ.get('LOCALEMR_CONTAINER_REPO', 'davlum/localemr-container:0.5.0-spark')
        # use below docker hub image to support emr 6.9.0 ( spark 3.3.0, hadoop 3.3.3 and livy 0.7.1)
        # else uncomment the above one
        self.localemr_container_repo = os.environ.get('LOCALEMR_CONTAINER_REPO', 'sumitzet/localemr-container:0.7.1-spark3.3.0')


configuration = Configuration()
