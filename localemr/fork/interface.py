"""
This class abstracts forking a process. This allows for multiple backends.
The first backend supported was Docker. This had some advantages, as the Docker container could have all
the necessary dependencies, but it has the disadvantage of requiring Docker.
"""
import abc
from multiprocessing import Queue
from localemr.config import Configuration
from localemr.common import ClusterSubset


class ForkInterface(abc.ABC):

    @staticmethod
    @abc.abstractmethod
    def terminate_process(config: Configuration, cluster: ClusterSubset, status_queue: Queue):
        pass

    @staticmethod
    @abc.abstractmethod
    def create_process(config: Configuration, cluster: ClusterSubset, status_queue: Queue):
        pass
