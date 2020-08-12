from datetime import datetime
import re
import logging
from multiprocessing import Queue
import pytz
import docker
from docker.errors import NotFound, APIError
from localemr.fork.interface import ForkInterface
from localemr.config import Configuration
from localemr.common import (
    EmrClusterState,
    ClusterSubset,
    cluster_to_spark_version,
)


class Docker(ForkInterface):

    @staticmethod
    def get_localemr_container(config: Configuration):
        try:
            return config.client.containers.get(config.localemr_container_name)
        except NotFound as e:
            message = "Container %s not found, most likely the container name was " \
                      "changed without changing the env; LOCALEMR_CONTAINER_NAME"
            raise NotFound(message, config.localemr_container_name) from e

    @staticmethod
    def run_fork_container(client: docker.client, container_image, container_args):
        try:
            return client.containers.run(container_image, **container_args)
        except APIError as e:
            if e.status_code == 409:
                msg = e.response.json()['message']
                maybe_container_id = re.findall(r'container "(\w+)"', msg)
                if len(maybe_container_id) != 1:
                    raise e
                client.containers.get(maybe_container_id[0]).remove(v=True, force=True)
                return client.containers.run(container_image, **container_args)
            raise e

    @staticmethod
    def terminate_process(config: Configuration, cluster: ClusterSubset, status_queue: Queue):
        try:
            config.client.containers.get(cluster.name).remove(v=True, force=True)
        except NotFound as e:
            if e.status_code == 404:
                logging.exception("Container %s not found, could not remove", cluster.name)
            else:
                raise e
        cluster.end_datetime = datetime.now(pytz.utc)
        cluster.state = EmrClusterState.TERMINATED
        status_queue.put(cluster)

    @staticmethod
    def create_process(config: Configuration, cluster: ClusterSubset, status_queue: Queue):
        localemr_container = Docker.get_localemr_container(config)

        application_versions = cluster_to_spark_version(cluster)
        container_image = '{repo}{spark_version}'.format(
            repo=config.localemr_container_repo,
            spark_version=application_versions['Spark']
        )
        env = {
            'LOCAL_DIR_WHITELIST': config.local_dir_whitelist,
            'AWS_DEFAULT_REGION': config.localemr_aws_default_region,
            'AWS_ACCESS_KEY_ID': config.localemr_aws_access_key_id,
            'AWS_SECRET_ACCESS_KEY': config.localemr_aws_secret_access_key,
            'AWS_REGION': config.localemr_aws_default_region,
            'S3_ENDPOINT': config.s3_endpoint,
        }
        container_args = {
            'name': cluster.name,
            # This binds the container's port 8998 to a random port in the host
            'ports': {'8998/tcp': None},
            'detach': True,
            'environment': env,
        }
        fork_container = Docker.run_fork_container(config.client, container_image, container_args)
        # TODO: Fix this dirty hack. We're just taking the first network that is attached to the container
        localemr_networks = localemr_container.attrs['NetworkSettings']['Networks']
        localemr_network_name = list(localemr_networks.keys())[0]
        localemr_network = config.client.networks.get(localemr_network_name)
        localemr_network.connect(fork_container)
        cluster.run_bootstrap_actions()
        status_queue.put(cluster)
