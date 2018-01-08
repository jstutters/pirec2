import pygraphviz as pgv


def make_graph(node, filename):
    nodes = walk_graph(node)
    g = pgv.AGraph(directed=True)
    for n in nodes:
        g.add_edge(n[0], n[2], sametail=n[1], label=n[1], fontsize='8')
    g = g.reverse()
    g.layout(prog='dot')
    g.draw(filename)


def walk_graph(node, graph=None):
    if graph is None:
        graph = []
    for ip in node.inputs:
        if ip.parent is not None:
            graph.append((_node_name(node), ip.name, _node_name(ip.parent)))
            graph = walk_graph(ip.parent, graph)
    return graph


def _node_name(n):
    return '{1:03d}-{0}'.format(type(n).__name__, n._id)
