import argparse
import importlib
import os
import sys
from .graph import make_graph
from .task import Pipeline


def run(pipeline_name, save_filename=None, skip_checksums=False):
    sys.path.append(os.getcwd())
    mod = importlib.import_module(pipeline_name)
    if save_filename is not None:
        with open(save_filename, 'r') as f:
            pipeline = Pipeline.load(f)
        pipeline.run()
        with open(save_filename, 'w') as f:
            pipeline = pipeline.save(f)
    else:
        tmp_path = os.path.join(os.getcwd(), 'tmp')
        if not os.path.exists(tmp_path):
            os.makedirs(tmp_path)
        pipeline = Pipeline(working_dir=tmp_path, skip_checksums=skip_checksums)
        end = mod.definition()
        make_graph(end, 'imagetest.png')
        pipeline.run(end)
        with open('pipeline.json', 'w') as f:
            pipeline.save(f)


def make_parser():
    parser = argparse.ArgumentParser(description='Neuroimaging build system.')
    parser.add_argument('pipeline', help='pipeline definition')
    parser.add_argument('-s', dest='record_filename', metavar='record_filename', help='JSON pipeline record', default=None)
    parser.add_argument('-c', dest='skip_checksums', action='store_true')
    return parser


def main():
    parser = make_parser()
    args = parser.parse_args()
    run(args.pipeline, args.record_filename, args.skip_checksums)


if __name__ == '__main__':
    main()
