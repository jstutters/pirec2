import os
import pickle
from pirec2.graph import make_graph
from pirec2.task import Pipeline
import stages


def load(filename):
    pipeline = pickle.load(open(filename, 'rb'))
    return pipeline


if __name__ == '__main__':
    tmp_path = os.path.join(os.getcwd(), 'tmp')
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)
    pipeline = Pipeline(working_dir=tmp_path)
    start = stages.Start('test.txt')
    shouter = stages.Shouter(start.the_file)
    reverser = stages.Reverser(shouter.loud)
    added = stages.Adder(shouter.loud, reverser.backward)
    other_added = stages.Adder(start.the_file, added.joined)
    end = stages.End(other_added.joined)
    pipeline.run(end)
    print(end.result.value)
    make_graph(end, 'test.png')
    with open('pipeline.json', 'w') as f:
        pipeline.save(f)
