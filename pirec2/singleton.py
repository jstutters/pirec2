class Singleton(object):
    """Singleton base class."""

    _shared_state = {}

    def __init__(self):
        self.__dict__ = self._shared_state
