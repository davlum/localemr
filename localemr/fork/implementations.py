# pylint: disable=import-outside-toplevel
import os
from localemr.config import Configuration


def get_docker_impl(config: Configuration):
    import docker
    from localemr.fork.docker.models import Docker
    docker_base_url = os.environ.get('DOCKER_BASE_URL', 'unix://var/run/docker.sock')
    docker_client = docker.DockerClient(base_url=docker_base_url)
    return Docker(config, docker_client)
