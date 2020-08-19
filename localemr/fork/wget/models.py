import os
import io
import re
import tarfile
from multiprocessing import Queue
import requests
from localemr.config import Configuration
from localemr.fork.interface import ForkInterface
from localemr.common import ClusterSubset, cluster_to_spark_version


def download_url_to_path(url: str, path: str):
    r = requests.get(url)
    r.raise_for_status()
    f = io.BytesIO(r.content)
    with tarfile.open(fileobj=f) as tar:
        tar.extractall(path)


def change_spark_in_path(spark_bin: str, path: str) -> str:
    spark_bin_re = r'/opt/spark-\d\.\d\.\d-bin-without-hadoop/bin'
    if not re.findall(spark_bin_re, path):
        return f'{path}:{spark_bin}'
    return re.sub(spark_bin_re, spark_bin, path)


class Wget(ForkInterface):

    def __init__(self, config: Configuration):
        self.config = config

    def create_process(self, cluster: ClusterSubset, status_queue: Queue):
        spark_version = cluster_to_spark_version(cluster)
        spark_dir = f'spark-{spark_version}-bin-without-hadoop'
        spark_path = f'/opt/{spark_dir}'
        spark_bin = f'{spark_path}/bin'
        if not os.path.isdir(spark_path):
            url = f"https://archive.apache.org/dist/spark/spark-{spark_version}/{spark_dir}.tgz"
            download_url_to_path(url, '/opt')

        os.environ['SPARK_HOME'] = spark_path
        os.environ['PATH'] = change_spark_in_path(spark_bin, os.environ['PATH'])

        cluster.run_bootstrap_actions()
        status_queue.put(cluster)

    def terminate_process(self, cluster: ClusterSubset, status_queue: Queue):
        cluster.run_termination_actions()
        status_queue.put(cluster)
