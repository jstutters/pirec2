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
    start = stages.SimpleStart('test.txt', 'foo')
    appender = stages.Appender(start.the_file, start.extra)
    end = stages.End(appender.appended)
    pipeline.run(end)
    print('==================')
    print('Results:')
    print(end.result.value)
    # make_graph(end, 'test.png')
    with open('pipeline.json', 'w') as f:
        pipeline.save(f)
