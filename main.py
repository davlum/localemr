import moto.server as server
from localemr.emr.models import FakeCluster
from localemr.local_emr import read_task_queue
from localemr.config import Configuration
from multiprocessing import Process
import argparse
import sys


# Replace moto emr with the custom emr implementation
del sys.modules['moto.emr']
sys.modules['moto.emr'] = __import__('localemr.emr')
sys.modules['moto.emr.urls'] = __import__('localemr.emr.urls')
del sys.modules['moto.emr.exceptions']
sys.modules['moto.emr.exceptions'] = __import__('localemr.emr.exceptions')
del sys.modules['moto.emr.models']
sys.modules['moto.emr.models'] = __import__('localemr.emr.models')
del sys.modules['moto.emr.utils']
sys.modules['moto.emr.utils'] = __import__('localemr.emr.utils')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local AWS EMR - A local service that imitates AWS EMR ")
    parser.add_argument('-p', '--port', help='Port to run the service on', type=int, default=3000)
    parser.add_argument("-H", "--host", type=str, help="Which host to bind", default="0.0.0.0")
    fetch_help = 'Will fetch files with an S3 prefix. Requires authentication.'
    parser.add_argument('-f', '--fetch-from-s3', help=fetch_help, action='store_true')
    convert_help = 'Converts the any paths on a submitted step from S3 to local directories.'
    parser.add_argument('-c', '--convert-s3-to-local', help=convert_help, action='store_true')
    livy_host_help = 'The host where the Apache Livy server is running'
    parser.add_argument('-l', '--livy-host', help=livy_host_help, default='http://livy:8998')
    parser.add_argument('-s', '--s3-host', help='The host where the S3 endpoint is', default=None)
    max_fetch_help = 'Max number of bytes to fetch from S3'
    parser.add_argument('-m', '--max-fetch-from-s3', help=max_fetch_help, default=100000, type=int)
    local_dir_help = 'Where to download the S3 files to. If empty then a temp dir is used.'
    parser.add_argument('-d', '--local-dir', help=local_dir_help, default=None)

    args = parser.parse_args()
    conf = Configuration(args)
    reader_process = Process(target=read_task_queue, args=(conf, FakeCluster.process_queue, FakeCluster.status_queue,))
    reader_process.daemon = True
    reader_process.start()
    server.main(['emr', '-H', args.host, '--port', str(args.port)])
