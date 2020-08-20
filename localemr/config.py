import os


def is_true(bool_thing) -> bool:
    return bool_thing if isinstance(bool_thing, bool) else bool_thing == 'True'


class Configuration:

    def __init__(self):
        self.exec_impl = os.environ.get('LOCALEMR_EXEC_IMPL', 'subprocess')
        self.fork_impl = os.environ.get('LOCALEMR_FORK_IMPL', 'wget')


configuration = Configuration()
