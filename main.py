import moto.server as server
import moto.emr.models as models
from src.local_emr import add_steps, read_task_queue, list_steps, describe_step, FailureDetails
from multiprocessing import Queue, Process
import moto.emr.responses as resp
from moto.core import BaseModel
from datetime import datetime
import pytz
from src.response_templates import DESCRIBE_STEP_TEMPLATE, LIST_STEPS_TEMPLATE
from moto.emr.utils import random_step_id

resp.DESCRIBE_STEP_TEMPLATE = DESCRIBE_STEP_TEMPLATE

resp.LIST_STEPS_TEMPLATE = LIST_STEPS_TEMPLATE

process_queue = Queue()

status_queue = Queue()

models.FakeCluster.process_queue = process_queue

models.FakeCluster.status_queue = status_queue


# Overwrite these function
models.FakeCluster.add_steps = add_steps
models.ElasticMapReduceBackend.list_steps = list_steps
models.ElasticMapReduceBackend.describe_step = describe_step

reader_process = Process(target=read_task_queue, args=(process_queue, status_queue,))
reader_process.daemon = True
reader_process.start()

server.main(['emr', '-H', '0.0.0.0', '-p3000'])
