import json
import logging
import os
import pickle
import shutil
import tempfile
from .singleton import Singleton

class Source():
    def __init__(self, value=None, filename=None):
        self._value = value
        self._filename = filename
        self._is_file = filename is not None

    @property
    def ready(self):
        return True

    @property
    def value(self):
        return self._value

    @property
    def filename(self):
        return self._filename

    @property
    def is_file(self):
        return self._is_file

    @property
    def parent(self):
        return None


class Connector():
    def __init__(self, parent, value=None, filename=None):
        self._parent = parent
        self._filename = filename
        self._value = value
        self._is_file = filename is not None

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

    @filename.setter
    def filename(self, v):
        self._filename = v

    @property
    def full_filename(self):
        return os.path.join(self.parent.working_dir, self._filename)

    @property
    def is_file(self):
        return self._is_file

    def connect(self):
        pass


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
        os.makedirs(self._working_dir)
        os.chdir(self._working_dir)
        self._ready_inputs()
        try:
            self.process()
        except Exception as e:
            raise e
        else:
            self.ready = True
        finally:
            pickle.dump(self, open('task.pkl', 'wb'))
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
        if value is None:
            op = Connector(self, filename=filename)
        else:
            op = Source(value=value)
        self._outputs.append(op)
        return op
    
    def _ready_inputs(self):
        for ip in self._inputs:
            if not ip.ready:
                ip.run()
            if ip.is_file:
                self._get_file(ip)

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

    def save(self, state_file):
        state = {
            'module': self.__module__,
            'class': type(self).__name__,
            'inputs': [ip.key for ip in self.inputs]
        }
        return state


class InputTask(Task):
    def __init__(self):
        super().__init__()
        self._working_dir = os.getcwd()

    def run(self):
        pass


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

    @property
    def logger(self):
        return self._logger

    @property
    def working_dir(self):
        return self._working_dir

    def run(self, node):
        self._root_node = node
        try:
            node.run()
        except Exception as e:
            raise e
        finally:
            self._logger = None
            save_pipeline(self)

    @classmethod
    def load(cls, state_file):
        json_state = json.loads(state_file.read())
        instance = cls(
            log_level=json_state['log_level'],
            working_dir=json_state['working_dir']
        )
        instance._unit_id = json_state['unit_id']

    def save(self, state_file):
        state = {
            'log_level': self._log_level,
            'working_dir': self.working_dir,
            'unit_id': self._unit_id
        }
        state_file.write(json.dumps(state))


def save_pipeline(pipeline):
    save_path = os.path.join(pipeline.working_dir, 'pipeline.pkl')
    pickle.dump(pipeline, open(save_path, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)