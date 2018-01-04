import importlib
import json
import logging
import tempfile
from .notset import NOTSET
from .singleton import Singleton


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
                    if saved_input['value'] != NOTSET:
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
    if o == NOTSET:
        return 'NOTSET'
    else:
        pass
    return json.JSONEncoder.default(o)


def json_decode(o):
    for k in o:
        if o[k] == 'NOTSET':
            o[k] = NOTSET
    return o
