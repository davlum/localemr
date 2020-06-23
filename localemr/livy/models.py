import inspect
from typing import List, Dict, Optional


class LivyState:

    NOT_STARTED = 'not_started'
    STARTING = 'starting'
    BUSY = 'busy'
    IDLE = 'idle'
    SHUTTING_DOWN = 'shutting_down'
    SUCCESS = 'success'
    ERROR = 'error'
    DEAD = 'dead'


LIVY_TERMINAL_STATES = [LivyState.SUCCESS, LivyState.ERROR, LivyState.DEAD]

LIVY_RUNNING_STATES = [
    LivyState.NOT_STARTED,
    LivyState.STARTING,
    LivyState.BUSY,
    LivyState.IDLE,
    LivyState.SHUTTING_DOWN,
]

LIVY_STATES = LIVY_TERMINAL_STATES + LIVY_RUNNING_STATES


class LivyRequestBody:
    """
    Reference:
    https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.0/running-spark-applications/content/livy_api_reference_for_batch_jobs.html

    All the parameters that have to do with optimization (example num_executors) are stripped away as the job will run
    with whatever resources are available.
    """
    def __init__(self,
                 file: str,
                 proxy_user: Optional[str] = None,
                 class_name: Optional[str] = None,
                 args: Optional[List[str]] = None,
                 jars: Optional[List[str]] = None,
                 py_files: Optional[List[str]] = None,
                 files: Optional[List[str]] = None,
                 archives: Optional[List[str]] = None,
                 queue: Optional[str] = None,
                 name: Optional[str] = None,
                 conf: Optional[Dict[str, str]] = None,
                 **kwargs):  # pylint: disable=unused-argument
        self.file = file
        self.proxy_user = proxy_user
        self.class_name = class_name
        self.args = args.split(',') if isinstance(args, str) else args
        self.jars = jars.split(',') if isinstance(jars, str) else jars
        self.py_files = py_files.split(',') if isinstance(py_files, str) else py_files
        self.files = files.split(',') if isinstance(files, str) else files
        self.archives = archives.split(',') if isinstance(archives, str) else archives
        self.queue = queue
        self.name = name
        self.conf = conf

    def to_dict(self):
        constructor_args = set(inspect.signature(LivyRequestBody).parameters.keys())
        intersection = constructor_args.intersection(set(dir(self)))
        return {
            self.from_snake_to_camel_case(k): getattr(self, k) for k in intersection if getattr(self, k) is not None
        }

    @staticmethod
    def from_snake_to_camel_case(s: str):
        words = s.split('_')
        word_ls = [words[0]] + list(map(lambda w: w.capitalize(), words[1:]))
        return ''.join(word_ls)


class LivyBatchObject:
    # pylint: disable=redefined-builtin
    def __init__(self, id: int, app_id: str, app_info: Dict[str, str], log: List[str], state: LivyState):
        self.id = id
        self.app_id = app_id
        self.app_info = app_info
        self.log = log
        self.state = state

    @staticmethod
    def from_dict(d):
        return LivyBatchObject(
            id=d['id'],
            app_id=d['appId'],
            app_info=d['appInfo'],
            log=d['log'],
            state=d['state']
        )


__all__ = [
    'LivyRequestBody',
    'LivyBatchObject',
    'LivyState',
    'LIVY_TERMINAL_STATES',
    'LIVY_STATES',
    'LIVY_RUNNING_STATES',
]
