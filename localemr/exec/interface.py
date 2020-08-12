"""
This class abstracts the running of a command. This allows for multiple backends.
The first backend supported was [Apache Livy][1]. The Livy backend enables batching Spark jobs
through an HTTP API, but is limited in that batching other commands (such as hdfs or mapreduce) is not really supported.
Supporting different backends is in the interest of the client.

Here `exec` is an abstract way of thinking about

[1]: <https://livy.apache.org/>
"""
from abc import abstractmethod
import importlib
from localemr.common import SparkResult, LocalFakeStep
from localemr.config import Configuration


class ExecInterface:

    _impl = None

    @abstractmethod
    def exec_process(self, emr_step: LocalFakeStep) -> SparkResult:
        """
        Parameters
        ----------
        self : The application configuration
        emr_step : The EMR step to run

        Returns
        -------
        Run emr_step synchronously and return SparkResult.
        """

    def get_impl(self, config: Configuration):
        if self._impl is not None:
            return self._impl
        localemr_exec = importlib.import_module('localemr.exec.implementations')
        self._impl = getattr(localemr_exec, 'get_{}_impl'.format(config.exec_impl))(config)
        return self._impl
