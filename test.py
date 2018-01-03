import os
import pickle
from pirec2.graph import make_graph
from pirec2.task import Pipeline
import stages


if __name__ == '__main__':
    tmp_path = os.path.join(os.getcwd(), 'tmp')
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)
    pipeline = Pipeline(working_dir=tmp_path)
    start = stages.Start('test_a.txt', 'test_b.txt')
    shouter_a = stages.Shouter(start.the_file)
    shouter_b = stages.Shouter(start.the_other_file)
    adder = stages.Adder(shouter_a.loud, shouter_b.loud)
    end = stages.End(adder.joined)
    pipeline.run(end)
    print(end.result.value)
    #make_graph(end, 'test.png')
    with open('pipeline.json', 'w') as f:
        pipeline.save(f)
