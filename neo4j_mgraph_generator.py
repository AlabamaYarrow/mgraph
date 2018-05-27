import os
import random
import sys

from arango_graph.metagraph import MetaGraph
from settings import NEO_DUMPS_DIR

"""
Neo4j data generator for graph model.
"""

_10k = 10000
_50k = 50000
_100k = 100000
_1M = 1000000
_10M = 10000000

TOTAL_NODES = _1M
TOTAL_EDGES = _1M

TOTAL_METANODES = 0
META_WIDTH = 0
META_DEPTH = 0


nodes_file = os.path.join(NEO_DUMPS_DIR, 'graph', 'nodes.csv')
edgenodes_file = os.path.join(NEO_DUMPS_DIR, 'graph', 'edgenodes.csv')
# graph_file = os.path.join(NEO_DUMPS_DIR, 'graph', 'graph_edges.csv')


def write_headers():
    """
    Write csv headers (and truncate files).
    """
    with open(nodes_file, 'w') as f:
        f.write('"_key";"int_attr";"str_attr"\n')

    with open(edgenodes_file, 'w') as f:
        f.write('"_key";"int_attr";"str_attr";"from";"to"\n')

    # with open(graph_file, 'w') as f:
    #     f.write('"_from";"_to";"submeta"\n')


def write_nodes(total_nodes=TOTAL_NODES):
    """
        arangoimp --file nodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
    """

    with open(nodes_file, 'a') as f:
        for x in range(1, total_nodes + 1):
            nid = 'v{}'.format(x)
            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))
            f.write(
                '"{nid}";{int_attr};"{str_attr}"\n'.format(
                    nid=nid, int_attr=int_attr, str_attr=str_attr
                )
            )


def write_edges(total_edges=TOTAL_EDGES, total_nodes=TOTAL_NODES):
    """
        arangoimp --file edgenodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
        arangoimp --file graph_edges.csv --type csv --separator ';' --collection NodesConnections --server.database mgraph --server.password 1234
    """

    edges = []

    with open(edgenodes_file, 'a') as f:
        for x in range(1, total_edges + 1):
            eid = 'e{}'.format(x)
            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))
            from_nid = 'v{}'.format(random.randint(1, total_nodes))
            to_nid = 'v{}'.format(random.randint(1, total_nodes))
            edges.append((eid, from_nid, to_nid))
            f.write(
                '"{eid}";{int_attr};"{str_attr}";"{from_nid}";"{to_nid}"\n'.format(
                    eid=eid, int_attr=int_attr, str_attr=str_attr,
                    from_nid=MetaGraph.key_to_id(from_nid),
                    to_nid=MetaGraph.key_to_id(to_nid)
                )
            )

    # Create Arango graph edges for each edgenode
    with open(graph_file, 'a') as f:
        for edgenode in edges:
            eid, from_id, to_id = edgenode
            f.write(
                '"{_from}";"{_to}";\n'.format(
                    _from=MetaGraph.key_to_id(from_id),
                    _to=MetaGraph.key_to_id(eid)
                )
            )
            f.write(
                '"{_from}";"{_to}";\n'.format(
                    _from=MetaGraph.key_to_id(eid),
                    _to=MetaGraph.key_to_id(to_id)
                )
            )


# Common incremented key for submeta nodes to avoid collision
submeta_pk = 0


def _add_submetas(f, gf, root_nid, meta_nid, depth, meta_width):
    global submeta_pk
    if not depth:
        return

    depth -= 1

    for j in range(1, meta_width + 1):
        sub_nid = '{}-d{}-{}'.format(root_nid, depth, submeta_pk)
        int_attr = random.randint(1, 100)
        str_attr = 'attr_{}'.format(random.randint(1, 100))
        f.write(
            '"{nid}";{int_attr};"{str_attr}"\n'.format(
                nid=sub_nid, int_attr=int_attr, str_attr=str_attr
            )
        )

        submeta_pk += 1

        gf.write(
            '"{_from}";"{_to}";true\n'.format(
                _from=MetaGraph.key_to_id(sub_nid),
                _to=MetaGraph.key_to_id(meta_nid)
            )
        )

        _add_submetas(f, gf, root_nid, sub_nid, depth, meta_width)


def write_metas(start_metanid=1, total_metanodes=TOTAL_METANODES, meta_width=META_WIDTH, meta_depth=META_DEPTH):
    sys.setrecursionlimit(20000)  # sorry Python :(

    with open(nodes_file, 'a') as f, open(graph_file, 'a+') as gf:
        for x in range(start_metanid, start_metanid + total_metanodes):
            meta_nid = 'm{}'.format(x)

            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))
            f.write(
                '"{nid}";{int_attr};"{str_attr}"\n'.format(
                    nid=meta_nid, int_attr=int_attr, str_attr=str_attr
                )
            )

            _add_submetas(f, gf, meta_nid, meta_nid, meta_depth, meta_width)


if __name__ == '__main__':
    write_headers()
    # write_nodes()
    # write_edges()
    write_metas()
