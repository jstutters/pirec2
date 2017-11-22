import subprocess
from tempfile import TemporaryDirectory
import attr


class op():
    _value = None

    def __init__(self, value=None):
        if value is not None:
            self._value = value

    def __set__(self, obj, value):
        self._value = value

    def __call__(self):
        return self._value


def ip(file=False, default=attr.Factory(op), validator=None, repr=True, cmp=True, hash=None, init=True,
       convert=None, metadata={}):
    metadata = dict() if not metadata else metadata
    metadata['__connector_type'] = 'INPUT'
    if file:
        metadata['__input_type'] = 'FILE'
    else:
        metadata['__input_type'] = 'OTHER'
    return attr.ib(default, validator, repr, cmp, hash, init, convert, metadata)


class Stage():
    def _gather_inputs(self):
        inputs = attr.asdict(self)
        for k, v in inputs.items():
            metadata = getattr(attr.fields(self.__class__), k).metadata
            is_input = metadata['__connector_type'] == 'INPUT'
            is_file = metadata['__input_type'] == 'FILE'
            if is_input and is_file:
                msg = 'would gather {0} to {1}/{2:03d}-{3}'.format(v(), self.working_dir, self.step, type(self).__name__.lower())
                print(msg)

    def run(self):
        self._gather_inputs()
        self.process()

    def call(self, cmd):
        subprocess.check_output(cmd, cwd=self.working_dir)


class Gather(Stage):
    def __init__(self, image):
        self.image = op(value=image)


class Pipeline():
    stages = []

    def append(self, stage):
        self.stages.append(stage)
        return stage

    def _setup_stages(self):
        for step, stage in enumerate(self.stages):
            stage.step = step
            stage.working_dir = self.working_dir

    def run(self):
        with TemporaryDirectory() as tempdir:
            self.working_dir = tempdir
            self._setup_stages()
            for s in self.stages:
                s.run()

    def __add__(self, stage):
        self.append(stage)
        return stage

