import os
from pirec2.task import InputTask, Task, Pipeline


class SimpleStart(InputTask):
    def __init__(self, the_file, extra):
        super().__init__()
        self.the_file = self.add_output(filename=the_file)
        self.extra = self.add_output(value=extra)


class Start(InputTask):
    def __init__(self, the_file, the_other_file):
        super().__init__()
        self.the_file = self.add_output(filename=the_file)
        self.the_other_file = self.add_output(filename=the_other_file)


class Shouter(Task):
    def __init__(self, quiet):
        super().__init__()
        self.quiet = self.add_input(quiet, filename='quiet.txt')
        self.loud = self.add_output(filename='loud.txt')

    def process(self):
        p = Pipeline()
        with open('quiet.txt', 'r') as quiet_file:
            with open(self.loud.filename, 'w') as loud_file:
                loud_file.write(str.upper(quiet_file.read()))


class Appender(Task):
    def __init__(self, before, extra_text):
        super().__init__()
        self.before = self.add_input(before, filename='before.txt')
        self.extra_text = self.add_input(extra_text)
        self.appended = self.add_output(filename='appended.txt')

    def process(self):
        p = Pipeline()
        with open('before.txt', 'r') as before_file:
            with open(self.appended.filename, 'w') as appended_file:
                appended_file.write(before_file.read())
                appended_file.write(self.extra_text.value)


class Reverser(Task):
    def __init__(self, forward):
        super().__init__()
        self.forward = self.add_input(forward, filename='forward.txt')
        self.backward = self.add_output(filename='backward.txt')

    def process(self):
        p = Pipeline()
        with open('forward.txt', 'r') as forward_file:
            with open('backward.txt', 'w') as backward_file:
                backward_str = forward_file.read()[::-1]
                backward_file.write(backward_str)


class End(Task):
    def __init__(self, word):
        super().__init__()
        self.input = self.add_input(word, filename='input.txt')
        self.result = self.add_output()

    def process(self):
        p = Pipeline()
        with open('input.txt', 'r') as input_file:
            self.result.value = input_file.read()


class Adder(Task):
    def __init__(self, file_a, file_b):
        super().__init__()
        self.file_a = self.add_input(file_a, filename='a.txt')
        self.file_b = self.add_input(file_b, filename='b.txt')
        self.joined = self.add_output(filename='joined.txt')

    def process(self):
        p = Pipeline()
        p.logger.info('running Adder')
        with open('a.txt', 'r') as a:
            with open('b.txt', 'r') as b:
                with open('joined.txt', 'w') as j:
                    j.write(a.read().strip() + b.read().strip())
