import pygraphviz as pgv
from hashids import Hashids


def make_graph(node, filename):
    g = walk_graph(node)
    g.layout(prog='dot')
    g.draw(filename)


def walk_graph(node, graph=None):
    if graph is None:
        graph = pgv.AGraph()    
    for ip in node.inputs:
        if ip.parent is not None:
            graph.add_edge(_node_name(node), _node_name(ip.parent)) 
            graph = walk_graph(ip.parent, graph)
    return graph


def _node_name(n):
    return '{0}-{1:03d}'.format(type(n).__name__, n._id)
