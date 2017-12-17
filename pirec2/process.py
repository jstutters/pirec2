import subprocess
import tempfile


def tempenv(f):
    def wrapper(*args):
        process_instance = args[0]
        with tempfile.TemporaryDirectory() as tempdir:
            process_instance.working_dir = tempdir
            return f(*args)
    return wrapper


class Process(object):
    working_dir = None
    
    def exec(self, cmd):
        output = subprocess.check_output(cmd, cwd=self.working_dir)
        return output


class TestProcess(Process):
    @tempenv
    def process(self):
        print(self.exec(['pwd']))