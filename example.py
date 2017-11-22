import logging
from pirec2 import Gather, Stage, Pipeline, ip, op, attr


@attr.s
class Dilate(Stage):
    image = ip(file=True, name="ip.nii.gz")
    amount = ip()
    output = op()

    def process(self):
        logging.info("would dilate %s by %d" % (self.image(), self.amount))
        self.output = str.upper(self.image())


@attr.s
class Binarize(Stage):
    image = ip(file=True, name="ip.nii.gz")
    output = op()

    def process(self):
        logging.info("would binarize " + self.image())


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-8s [%(process)d] %(message)s',
    )

    p = Pipeline()
    inputs = Gather("foo.nii.gz")
    dilate = p + Dilate(inputs.image, 5)
    binarize = p + Binarize(dilate.output)
    p.run()
