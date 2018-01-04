import hashlib
import importlib
import json
import logging
import os
import shutil
import tempfile
from .singleton import Singleton


class NotSet():
    def __repr__(self):
        return 'NOTSET'


NOTSET = NotSet()


class Connector():
    def __init__(self, parent, value=NOTSET, filename=None, key=None, checksum=None):
        self._parent = parent
        self._filename = filename
        self._value = value
        self._is_file = filename is not None
        self._key = key
        self._checksum = checksum
        self._value_changed = False

    @property
    def parent(self):
        return self._parent

    @property
    def ready(self):
        return self._parent.ready

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
            return not isinstance(self.value, NotSet)

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


class Task():
    def __init__(self):
        pipeline = Pipeline()
        self._ready = False
        self._inputs = []
        self._outputs = []
        self._ip_map = {}
        self._working_dir = None
        self._id = pipeline.get_next_id()
        pipeline.register(self)
        self._working_dir = os.path.join(pipeline.working_dir, self.key)

    def run(self):
        previous_dir = os.getcwd()
        if not os.path.exists(self._working_dir):
            os.makedirs(self._working_dir)
        os.chdir(self._working_dir)
        self._ready_inputs()
        if (self._complete() is False) or (self._inputs_changed() is True):
            Pipeline().logger.info('Running: %s', self.key)
            try:
                self.process()
            except Exception as e:
                raise e
            else:
                self.ready = True
            finally:
                os.chdir(previous_dir)
        else:
            Pipeline().logger.info('Up-to-date: %s', self.key)
            self.ready = True
            os.chdir(previous_dir)

    def process(self):
        pass

    @property
    def key(self):
        return '{1:03d}-{0}'.format(type(self).__name__, self._id)

    def add_input(self, ip, filename=None):
        self._inputs.append(ip)
        if filename is not None:
            self._ip_map[id(ip)] = filename
        return ip

    def add_output(self, value=NOTSET, filename=None):
        op = Connector(self, value=value, filename=filename, key=len(self._outputs))
        self._outputs.append(op)
        return op

    def get_output(self, key):
        return self._outputs[key]

    def _ready_inputs(self):
        for ip in self._inputs:
            if not ip.ready:
                ip.run()
            if ip.is_file:
                self._get_file(ip)

    def _complete(self):
        completion = [op.complete for op in self._outputs]
        return all(completion)

    def _inputs_changed(self):
        change = [ip.changed for ip in self._inputs]
        return any(change)

    def _checksum_outputs(self):
        for op in self._outputs:
            op.read_checksum()

    def _ip_name(self, ip):
        return self._ip_map[id(ip)]

    def _get_file(self, ip):
        pipeline = Pipeline()
        dest = os.path.join(self.working_dir, self._ip_name(ip))
        pipeline.logger.debug(
            'Copying %s to %s',
            ip.full_filename,
            dest
        )
        shutil.copy(ip.full_filename, dest)

    @property
    def ready(self):
        return self._ready

    @ready.setter
    def ready(self, v):
        self._ready = v

    @property
    def working_dir(self):
        return self._working_dir

    @property
    def inputs(self):
        return self._inputs

    def as_dict(self):
        self._checksum_outputs()
        state = {
            'module': self.__module__,
            'class': type(self).__name__,
            'inputs': [ip.as_dict() for ip in self.inputs]
        }
        return state

    def set_checksums(self, checksums):
        pairs = zip(self._inputs, checksums)
        for ip, cs in pairs:
            ip._checksum = cs


class InputTask(Task):
    def __init__(self):
        super().__init__()
        self._working_dir = os.getcwd()

    def run(self):
        if self._inputs_changed():
            Pipeline().logger.info('Running: %s', self.key)
        else:
            Pipeline().logger.info('Up-to-date: %s', self.key)
        self.ready = True

    def _read_output_checksums(self):
        for op in self._outputs:
            op.read_checksum()

    def as_dict(self):
        self._read_output_checksums()
        state = {
            'module': self.__module__,
            'class': type(self).__name__,
            'inputs': [{'type': 'Source', 'filename': ip.filename, 'value': ip.value, 'checksum': ip.checksum}
                       for ip in self._outputs]
        }
        return state

    def _inputs_changed(self):
        change = [op.changed for op in self._outputs]
        return any(change)

    def set_checksums(self, checksums):
        pairs = zip(self._outputs, checksums)
        for op, cs in pairs:
            op._checksum = cs


class Pipeline(Singleton):
    _working_dir = None
    _logger = None
    _root_node = None
    _unit_id = None
    _units = None
    _skip_checksums = None

    def __init__(self, log_level=logging.INFO, working_dir=None, skip_checksums=False):
        Singleton.__init__(self)
        if self._working_dir is None:
            if working_dir is not None:
                self._working_dir = working_dir
            else:
                self._working_dir = tempfile.mkdtemp()
        if self._skip_checksums is None:
            self._skip_checksums = skip_checksums
        if self._logger is None:
            self._configure_logging(log_level)
        if self._unit_id is None:
            self._unit_id = 0
        if self._units is None:
            self._units = {}

    def _configure_logging(self, log_level):
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(log_level)
        stderr_handler = logging.StreamHandler()
        self._logger.addHandler(stderr_handler)

    def get_next_id(self):
        self._unit_id += 1
        return self._unit_id

    def register(self, unit):
        self._units[unit.key] = unit
        self._root_node = unit

    def get_unit(self, key):
        return self._units[key]

    @property
    def logger(self):
        return self._logger

    @property
    def working_dir(self):
        return self._working_dir

    @property
    def skip_checksums(self):
        return self._skip_checksums

    @property
    def root_node(self):
        return self._root_node

    def run(self, node=None):
        if node is not None:
            self._root_node = node
        try:
            self._root_node.run()
        except Exception as e:
            raise e

    def save(self, state_file):
        state = {
            'log_level': self.logger.level,
            'working_dir': self.working_dir,
            'unit_id': self._unit_id,
            'units': [u.as_dict() for u in self._units.values()],
            'root_node': self._root_node.key
        }
        state_file.write(json.dumps(state, default=json_encode))

    @classmethod
    def load(cls, state_file):
        state = json.loads(state_file.read(), object_hook=json_decode)
        pipeline = cls(
            log_level=state['log_level'],
            working_dir=state['working_dir']
        )
        for unit in state['units']:
            mod = importlib.import_module(unit['module'])
            UnitClass = getattr(mod, unit['class'])
            inputs = []
            checksums = []
            for saved_input in unit['inputs']:
                ip = None
                if saved_input['type'] == 'Source':
                    if not isinstance(saved_input['value'], NotSet):
                        ip = saved_input['value']
                    elif saved_input['filename']:
                        ip = saved_input['filename']
                elif saved_input['type'] == 'Connector':
                    ip = pipeline.get_unit(saved_input['parent']).get_output(saved_input['key'])
                inputs.append(ip)
                checksums.append(saved_input['checksum'])
            unit = UnitClass(*inputs)
            unit.set_checksums(checksums)
        pipeline._root_node = pipeline.get_unit(state['root_node'])
        return pipeline


def json_encode(o):
    if isinstance(o, NotSet):
        return 'NOTSET'
    else:
        pass
    return json.JSONEncoder.default(o)


def json_decode(o):
    for k in o:
        if o[k] == 'NOTSET':
            o[k] = NOTSET
    return o
