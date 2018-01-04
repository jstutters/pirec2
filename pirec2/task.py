import os
import shutil
from .pipeline import Pipeline
from .connector import Connector, NOTSET


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

    def add_output(self, value=NOTSET, filename=None, name=None):
        op = Connector(self, value=value, filename=filename, name=name, key=len(self._outputs))
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
        input_state = [
            {
                'type': 'Source',
                'filename': ip.filename,
                'value': ip.value,
                'checksum': ip.checksum
            }
            for ip in self._outputs
        ]
        state = {
            'module': self.__module__,
            'class': type(self).__name__,
            'inputs': input_state
        }
        return state

    def _inputs_changed(self):
        change = [op.changed for op in self._outputs]
        return any(change)

    def set_checksums(self, checksums):
        pairs = zip(self._outputs, checksums)
        for op, cs in pairs:
            op._checksum = cs
