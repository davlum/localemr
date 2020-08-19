# pylint: disable=import-outside-toplevel
import os
from localemr.config import Configuration


def get_subprocess_impl(config: Configuration):
    from localemr.exec.subprocess.models import Subprocess
    return Subprocess(config)


def get_livy_impl(config: Configuration):
    from localemr.exec.livy.backend import Livy
    # Which directories livy can read files from
    # Default is extremely permissive as this is intended for development
    config.local_dir_whitelist = os.environ.get('LOCAL_DIR_WHITELIST', '/')
    return Livy(config)
