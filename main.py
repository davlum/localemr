import moto.server as server
from src.emr.models import FakeCluster
from src.local_emr import read_task_queue
from multiprocessing import Process
import sys


# Replace moto emr with the custom emr implementation
del sys.modules['moto.emr']
sys.modules['moto.emr'] = __import__('src.emr')
sys.modules['moto.emr.urls'] = __import__('src.emr.urls')
del sys.modules['moto.emr.exceptions']
sys.modules['moto.emr.exceptions'] = __import__('src.emr.exceptions')
del sys.modules['moto.emr.models']
sys.modules['moto.emr.models'] = __import__('src.emr.models')
del sys.modules['moto.emr.utils']
sys.modules['moto.emr.utils'] = __import__('src.emr.utils')


if __name__ == "__main__":
    reader_process = Process(target=read_task_queue, args=(FakeCluster.process_queue, FakeCluster.status_queue,))
    reader_process.daemon = True
    reader_process.start()
    server.main(['emr', '-H', '0.0.0.0', '-p3000'])
