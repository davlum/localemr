"""
This class abstracts the running of a command. This allows for multiple backends.
The first backend supported was [Apache Livy][1]. The Livy backend enables batching Spark jobs
through an HTTP API, but is limited in that batching other commands (such as hdfs or mapreduce) is not really supported. Supporting
different backends is in the interest of the client.

[1]: <https://livy.apache.org/>
"""

import abc
from localemr.common import SparkResult, LocalFakeStep
from localemr.config import Configuration


class ExecInterface(abc.ABC):

    @staticmethod
    @abc.abstractmethod
    def exec_process(config: Configuration, emr_step: LocalFakeStep) -> SparkResult:
        """
        Parameters
        ----------
        config : The application configuration
        emr_step : The EMR step to run

        Returns
        -------
        Run emr_step synchronously and return SparkResult.
        """
        pass
