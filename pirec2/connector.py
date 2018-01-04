import hashlib
import os
from .notset import NOTSET
from .pipeline import Pipeline


class Connector():
    def __init__(self, parent, value=NOTSET, filename=None, name=None, key=None, checksum=None):
        self._parent = parent
        self._filename = filename
        self._value = value
        self._is_file = filename is not None
        self._key = key
        self._checksum = checksum
        self._value_changed = False
        self._name = name

    @property
    def parent(self):
        return self._parent

    @property
    def ready(self):
        return self._parent.ready

    @property
    def name(self):
        if self._name is None and self.filename is not None:
            n = self.filename.split('.', maxsplit=1)[0]
        else:
            n = self._name
        return n

    def run(self):
        self._parent.run()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, v):
        self._value_changed = True
        self._value = v

    @property
    def filename(self):
        return self._filename

    @property
    def complete(self):
        if self.is_file:
            no_change = self.changed is False
            exists = os.path.exists(self.full_filename)
            return exists and no_change
        else:
            return self.value != NOTSET

    @property
    def changed(self):
        if self.is_file:
            if Pipeline().skip_checksums:
                return False
            else:
                return self.checksum != sha1sum(self.full_filename)
        else:
            return self._value_changed

    @filename.setter
    def filename(self, v):
        self._filename = v

    @property
    def full_filename(self):
        return os.path.join(self.parent.working_dir, self._filename)

    @property
    def is_file(self):
        return self._is_file

    @property
    def key(self):
        return self._key

    def as_dict(self):
        state = {
            'type': type(self).__name__,
            'parent': self.parent.key,
            'value': self.value,
            'filename': self.filename,
            'key': self.key,
            'checksum': self.checksum
        }
        return state

    @property
    def checksum(self):
        return self._checksum

    def read_checksum(self):
        if self.is_file:
            self._checksum = sha1sum(self.full_filename)


def sha1sum(filename):
    sha1 = hashlib.sha1()
    with open(filename, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()
