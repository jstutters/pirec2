import importlib
import os
import sys
from pirec2.graph import make_graph
from pirec2.task import Pipeline


def run(pipeline_name, save_filename=None):
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
        pipeline = Pipeline(working_dir=tmp_path, skip_checksums=False)
        end = mod.definition()
        make_graph(end, 'imagetest.png')
        pipeline.run(end)
        with open('pipeline.json', 'w') as f:
            pipeline.save(f)


def main():
    if len(sys.argv) == 3:
        run(sys.argv[1], sys.argv[2])
    else:
        run(sys.argv[1])


if __name__ == '__main__':
    main()
