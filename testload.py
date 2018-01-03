import pirec2.task

with open('pipeline.json', 'r') as f:
    pipeline = pirec2.task.Pipeline.load(f)
pipeline.run()
print(pipeline.root_node.result.value)
# with open('pipeline.json', 'w') as f:
    # pipeline.save(f)
