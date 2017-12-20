import importlib
import json
import logging
import os
import shutil
import tempfile
from .singleton import Singleton


class Connector():
    def __init__(self, parent, value=None, filename=None, key=None):
        self._parent = parent
        self._filename = filename
        self._value = value
        self._is_file = filename is not None
        self._key = key

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
        self._value = v

    @property
    def filename(self):
        return self._filename

    @property
    def complete(self):
        if self.is_file:
            # todo: need to verify checksum hasn't changed
            return os.path.exists(self.filename)
        else:
            return False

    @property
    def changed(self):
        if self.is_file:
            return False
        else:
            return True

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
            'key': self.key
        }
        return state


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

    def run(self):
        pipeline = Pipeline()
        previous_dir = os.getcwd()
        self._working_dir = os.path.join(pipeline.working_dir, self.key)
        if not os.path.exists(self._working_dir):
            os.makedirs(self._working_dir)
        os.chdir(self._working_dir)
        self._ready_inputs()
        if (self._complete() is False) or (self._inputs_changed() is True):
            print(self.key, 'not complete')
            try:
                self.process()
            except Exception as e:
                raise e
            else:
                self.ready = True
            finally:
                os.chdir(previous_dir)
        else:
            print(self.key, 'already complete')
            self.ready = True
            os.chdir(previous_dir)

    @property
    def key(self):
        return '{0}-{1:03d}'.format(type(self).__name__, self._id)

    def add_input(self, ip, filename=None):
        self._inputs.append(ip)
        if filename is not None:
            self._ip_map[id(ip)] = filename
        return ip

    def add_output(self, value=None, filename=None):
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

    def _ip_name(self, ip):
        return self._ip_map[id(ip)]

    def _get_file(self, ip):
        pipeline = Pipeline()
        dest = os.path.join(self.working_dir, self._ip_name(ip))
        pipeline.logger.info(
            'copying %s to %s',
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
        state = {
            'module': self.__module__,
            'class': type(self).__name__,
            'inputs': [ip.as_dict() for ip in self.inputs]
        }
        return state


class InputTask(Task):
    def __init__(self):
        super().__init__()
        self._working_dir = os.getcwd()

    def run(self):
        pass

    def as_dict(self):
        state = {
            'module': self.__module__,
            'class': type(self).__name__,
            'inputs': [{'type': 'Source', 'filename': ip.filename, 'value': ip.value}
                       for ip in self._outputs]
        }
        return state


class Pipeline(Singleton):
    _working_dir = None
    _logger = None
    _root_node = None
    _unit_id = None
    _units = None

    def __init__(self, log_level=logging.INFO, working_dir=None):
        Singleton.__init__(self)
        if self._working_dir is None:
            if working_dir is not None:
                self._working_dir = working_dir
            else:
                self._working_dir = tempfile.mkdtemp()
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

    def get_unit(self, key):
        return self._units[key]

    @property
    def logger(self):
        return self._logger

    @property
    def working_dir(self):
        return self._working_dir

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
        state_file.write(json.dumps(state))

    @classmethod
    def load(cls, state_file):
        state = json.loads(state_file.read())
        pipeline = cls(
            log_level=state['log_level'],
            working_dir=state['working_dir']
        )
        for unit in state['units']:
            mod = importlib.import_module(unit['module'])
            UnitClass = getattr(mod, unit['class'])
            inputs = []
            for saved_input in unit['inputs']:
                ip = None
                print(saved_input['type'])
                if saved_input['type'] == 'Source':
                    if saved_input['value']:
                        ip = saved_input['value']
                    elif saved_input['filename']:
                        ip = saved_input['filename']
                elif saved_input['type'] == 'Connector':
                    ip = pipeline.get_unit(saved_input['parent']).get_output(saved_input['key'])
                inputs.append(ip)
            print('creating', unit['class'])
            print('inputs:', inputs)
            unit = UnitClass(*inputs)
        pipeline._root_node = pipeline.get_unit(state['root_node'])
        return pipeline
