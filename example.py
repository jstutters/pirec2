from pirec2 import Gather, Stage, Pipeline, ip, op, attr


@attr.s
class Dilate(Stage):
    image = ip(file=True)
    amount = ip()
    output = op()

    def process(self):
        print("would dilate", self.image(), "by", self.amount)
        self.output = str.upper(self.image())


@attr.s
class Binarize(Stage):
    image = ip(file=True)
    output = op()

    def process(self):
        print("would binarize", self.image())


if __name__ == '__main__':
    p = Pipeline()
    inputs = Gather("foo.nii.gz")
    dilate = p + Dilate(inputs.image, 5)
    binarize = p + Binarize(dilate.output)
    p.run()
