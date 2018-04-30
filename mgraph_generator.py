import random

from arango_graph.metagraph import key_to_id, MetaGraph


_10k = 10000
_50k = 50000
_100k = 100000
_1M = 1000000
_10M = 10000000


total_nodes = 2
total_edges = 1

total_metanodes = 1
meta_width = 2
meta_depth = 3


nodes_file = 'nodes.csv'
edgenodes_file = 'edgenodes.csv'
graph_file = 'graph_edges.csv'


def write_headers():
    with open(nodes_file, 'w') as f:
        f.write('"{}";"{}";"{}"\n'.format('_key', 'int_attr', 'str_attr'))

    with open(edgenodes_file, 'w') as f:
        f.write('"{}";"{}";"{}";"{}";"{}"\n'.format('_key', 'int_attr', 'str_attr', 'from', 'to'))

    with open(graph_file, 'w') as f:
        f.write('"{}";"{}";"{}"\n'.format('_from', '_to', 'submeta'))


def write_nodes():
    """
        arangoimp --file nodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
    """

    with open(nodes_file, 'w') as f:
        for x in range(1, total_nodes + 1):
            nid = 'v{}'.format(x)
            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))
            f.write(
                '"{nid}";{int_attr};"{str_attr}"\n'.format(
                    nid=nid, int_attr=int_attr, str_attr=str_attr
                )
            )


def write_edges():
    """
        arangoimp --file edgenodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
        arangoimp --file graph_edges.csv --type csv --separator ';' --collection NodesConnections --server.database mgraph --server.password 1234
    """

    edges = []

    with open(edgenodes_file, 'w') as f:
        for x in range(1, total_nodes + 1):
            eid = 'e{}'.format(x)
            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))
            from_nid = 'v{}'.format(random.randint(1, total_nodes))
            to_nid = 'v{}'.format(random.randint(1, total_nodes))
            edges.append((eid, from_nid, to_nid))
            f.write(
                '"{eid}";{int_attr};"{str_attr}";"{from_nid}";"{to_nid}"\n'.format(
                    eid=eid, int_attr=int_attr, str_attr=str_attr,
                    from_nid=key_to_id(MetaGraph.NODES_COLL, from_nid),
                    to_nid=key_to_id(MetaGraph.NODES_COLL, to_nid)
                )
            )

    # Create Arango graph edges for each edgenode
    with open(graph_file, 'w') as f:
        for edgenode in edges:
            eid, from_id, to_id = edgenode
            f.write(
                '"{_from}";"{_to}";\n'.format(
                    _from=key_to_id(MetaGraph.NODES_COLL, from_id),
                    _to=key_to_id(MetaGraph.NODES_COLL, eid)
                )
            )
            f.write(
                '"{_from}";"{_to}";\n'.format(
                    _from=key_to_id(MetaGraph.NODES_COLL, eid),
                    _to=key_to_id(MetaGraph.NODES_COLL, to_id)
                )
            )


def _add_submetas(f, gf, nid, depth):
    if not depth:
        return

    depth -= 1

    for j in range(1, meta_width + 1):
        sub_nid = '{}-{}'.format(nid, j)
        int_attr = random.randint(1, 100)
        str_attr = 'attr_{}'.format(random.randint(1, 100))
        f.write(
            '"{nid}";{int_attr};"{str_attr}"\n'.format(
                nid=sub_nid, int_attr=int_attr, str_attr=str_attr
            )
        )

        gf.write(
            '"{_from}";"{_to}";true\n'.format(
                _from=key_to_id(MetaGraph.NODES_COLL, sub_nid),
                _to=key_to_id(MetaGraph.NODES_COLL, nid)
            )
        )

        _add_submetas(f, gf, sub_nid, depth)


def write_metas():
    with open(nodes_file, 'a+') as f, open(graph_file, 'a+') as gf:
        for x in range(1, total_metanodes + 1):
            meta_nid = 'm{}'.format(1)

            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))
            f.write(
                '"{nid}";{int_attr};"{str_attr}"\n'.format(
                    nid=meta_nid, int_attr=int_attr, str_attr=str_attr
                )
            )

            _add_submetas(f, gf, meta_nid, meta_depth)


if __name__ == '__main__':
    write_headers()
    # write_nodes()
    # write_edges()
    write_metas()
