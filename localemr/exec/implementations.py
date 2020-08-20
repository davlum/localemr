# pylint: disable=import-outside-toplevel
from localemr.config import Configuration


def get_subprocess_impl(config: Configuration):
    from localemr.exec.subprocess.models import Subprocess
    return Subprocess(config)
