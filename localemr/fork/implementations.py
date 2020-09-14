# pylint: disable=import-outside-toplevel
from localemr.config import Configuration


def get_docker_impl(config: Configuration):
    import docker
    from localemr.fork.docker.models import Docker
    docker_client = docker.DockerClient(base_url=config.docker_base_url)
    return Docker(config, docker_client)
