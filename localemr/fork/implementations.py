# pylint: disable=import-outside-toplevel
from localemr.config import Configuration


def get_wget_impl(config: Configuration):
    from localemr.fork.wget.models import Wget
    return Wget(config)
