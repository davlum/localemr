from requests.exceptions import HTTPError


class LivyError(HTTPError):
    pass
