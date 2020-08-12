"""
This class abstracts forking a process. This allows for multiple backends.
The first backend supported was Docker. This had some advantages, as the Docker container could have all
the necessary dependencies, but it has the disadvantage of requiring Docker.

Here the `fork` is an abstract way of describing an EMR cluster. In our case it's just some process
(or processes) that have access to the resources (Spark being one of the resources for example),
required to run EMR steps.
"""
import importlib
from abc import abstractmethod
from multiprocessing import Queue
from localemr.config import Configuration
from localemr.common import ClusterSubset


class ForkInterface:

    _impl = None

    @abstractmethod
    def terminate_process(self, cluster: ClusterSubset, status_queue: Queue):
        """

        Parameters
        ----------
        self : App configuration
        cluster : The configuration of the cluster
        status_queue : The multiprocessing queue to put the updated cluster attributes after
            the action has been completed, in this case, terminating the process.

        Returns
        -------
        None
        """

    @abstractmethod
    def create_process(self, cluster: ClusterSubset, status_queue: Queue):
        """
        Parameters
        ----------
        self : App configuration
        cluster : The configuration of the cluster
        status_queue : The multiprocessing queue to put the updated cluster attributes after
            the action has been completed, in this case, creating the process.

        Returns
        -------
        None
        """

    def get_impl(self, config: Configuration):
        if self._impl is not None:
            return self._impl
        localemr_fork = importlib.import_module('localemr.fork.implementations')
        self._impl = getattr(localemr_fork, 'get_{}_impl'.format(config.fork_impl))(config)
        return self._impl
