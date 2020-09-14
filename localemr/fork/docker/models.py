import re
import logging
from multiprocessing import Queue
import docker
from docker.errors import NotFound, APIError
from localemr.fork.interface import ForkInterface
from localemr.config import Configuration
from localemr.common import ClusterSubset, cluster_to_spark_version


class Docker(ForkInterface):

    def __init__(self, config: Configuration, client: docker.DockerClient):
        self.config = config
        self.client = client

    def get_localemr_container(self):
        try:
            return self.client.containers.get(self.config.localemr_container_name)
        except NotFound as e:
            message = "Container %s not found, most likely the container name was " \
                      "changed without changing the env; LOCALEMR_CONTAINER_NAME"
            raise NotFound(message, self.config.localemr_container_name) from e

    def run_fork_container(self, container_image, container_args):
        try:
            return self.client.containers.run(container_image, **container_args)
        except APIError as e:
            if e.status_code == 409:
                msg = e.response.json()['message']
                maybe_container_id = re.findall(r'container "(\w+)"', msg)
                if len(maybe_container_id) != 1:
                    raise e
                self.client.containers.get(maybe_container_id[0]).remove(v=True, force=True)
                return self.client.containers.run(container_image, **container_args)
            raise e

    def terminate_process(self, cluster: ClusterSubset, status_queue: Queue):
        try:
            self.client.containers.get(cluster.name).remove(v=True, force=True)
        except NotFound as e:
            if e.status_code == 404:
                logging.exception("Container %s not found, could not remove", cluster.name)
            else:
                raise e
        cluster.run_termination_actions()
        status_queue.put(cluster)

    def create_process(self, cluster: ClusterSubset, status_queue: Queue):
        localemr_container = self.get_localemr_container()

        spark_version = cluster_to_spark_version(cluster)
        container_image = '{repo}{spark_version}'.format(
            repo=self.config.localemr_container_repo,
            spark_version=spark_version,
        )
        env = {
            'AWS_DEFAULT_REGION': self.config.localemr_aws_default_region,
            'AWS_ACCESS_KEY_ID': self.config.localemr_aws_access_key_id,
            'AWS_SECRET_ACCESS_KEY': self.config.localemr_aws_secret_access_key,
            'AWS_REGION': self.config.localemr_aws_default_region,
            'S3_ENDPOINT': self.config.s3_endpoint,
        }
        container_args = {
            'name': cluster.name,
            # This binds the container's port 8998 to a random port in the host
            'ports': {'8998/tcp': None},
            'detach': True,
            'environment': env,
        }
        fork_container = self.run_fork_container(container_image, container_args)
        # TODO: Fix this dirty hack. We're just taking the first network that is attached to the container
        localemr_networks = localemr_container.attrs['NetworkSettings']['Networks']
        localemr_network_name = list(localemr_networks.keys())[0]
        localemr_network = self.client.networks.get(localemr_network_name)
        localemr_network.connect(fork_container)
        cluster.run_bootstrap_actions()
        status_queue.put(cluster)
