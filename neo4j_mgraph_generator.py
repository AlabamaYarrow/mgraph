import os
import random
import sys

from neo4j_graph.metagraph import MetaGraph
from neo4j_graph.metagraph import NODE_LABEL, EDGENODE_LABEL
from settings import NEO_DUMPS_DIR

"""
Neo4j data generator for graph model.
"""

_10k = 10000
_50k = 50000
_100k = 100000
_1M = 1000000
_10M = 10000000

# TOTAL_NODES = _1M
# TOTAL_EDGES = _1M
TOTAL_NODES = _100k
TOTAL_EDGES = _100k

TOTAL_METANODES = 0
META_WIDTH = 0
META_DEPTH = 0


# dummy_nodes_file = os.path.join(NEO_DUMPS_DIR, 'graph', 'dummy_nodes.csv')
nodes_file = os.path.join(NEO_DUMPS_DIR, 'graph', 'nodes.csv')
edges_file = os.path.join(NEO_DUMPS_DIR, 'graph', 'edges.csv')
# edgenodes_file = os.path.join(NEO_DUMPS_DIR, 'graph', 'edgenodes.csv')
# graph_file = os.path.join(NEO_DUMPS_DIR, 'graph', 'graph_edges.csv')


def write_headers():
    """
    Write csv headers (and truncate files).
    """
    with open(nodes_file, 'w') as f:
        f.write('_id:ID;str_attr:string;int_attr:int;from:string;to:string;:LABEL\n')

    with open(edges_file, 'w') as f:
        f.write(':START_ID;:END_ID;:TYPE\n')

    # with open(graph_file, 'w') as f:
    #     f.write('"_from";"_to";"submeta"\n')


def write_dummy_nodes(total_nodes=TOTAL_NODES, total_edges=TOTAL_EDGES):
    """
    sudo neo4j-admin import --database graph.db --delimiter=';' --array-delimiter=','
    --id-type INTEGER /.../....csv
    """
    with open(nodes_file, 'a') as f:
        nid = 1
        for x in range(1, total_nodes + 1):
            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))
            f.write(
                '{_id};{str_attr};{int_attr};;;{node_label}\n'.format(
                    _id=nid, int_attr=int_attr, str_attr=str_attr, node_label=NODE_LABEL
                )
            )
            nid += 1
        for x in range(1, total_edges + 1):
            from_node = random.randint(1, total_nodes)
            to_node = random.randint(1, total_nodes)
            f.write(
                '{_id};;;{from_node};{to_node};{edgenode_label}\n'.format(
                    _id=nid, from_node=from_node, to_node=to_node, edgenode_label=EDGENODE_LABEL
                )
            )
            nid += 1


def write_metas(start_metanid=1, total_metanodes=TOTAL_METANODES, meta_width=META_WIDTH, meta_depth=META_DEPTH):
    # sys.setrecursionlimit(20000)  # sorry Python :(

    with open(nodes_file, 'a') as f, open(edges_file, 'a') as ef:
        for x in range(start_metanid, start_metanid + total_metanodes):
            meta_nid = 'm{}'.format(x)
            f.write(
                '{_id};{str_attr};{int_attr};{_from};{_to};{label}\n'.format(
                    _id=meta_nid,
                    str_attr='', int_attr='', _from='', _to='', label=NODE_LABEL
                )
            )
            if meta_width > 1:
                # "wide" subnodes
                for j in range(1, meta_width + 1):
                    sub_node_nid = '{}-w{}'.format(meta_nid, j)
                    f.write(
                        '{_id};{str_attr};{int_attr};{_from};{_to};{label}\n'.format(
                            _id=sub_node_nid,
                            str_attr='', int_attr='', _from='', _to='', label=NODE_LABEL
                        )
                    )
                    ef.write(
                        '{_id};{parent_meta_id};{label}\n'.format(
                            _id=sub_node_nid, parent_meta_id=meta_nid, label=MetaGraph.meta_label)
                    )
            if meta_depth > 1:
                # "deep subnodes"
                parent_sub_node_nid = meta_nid
                for j in range(1, meta_depth + 1):
                    sub_node_nid = '{}-d{}'.format(meta_nid, j)
                    f.write(
                        '{_id};{str_attr};{int_attr};{_from};{_to};{label}\n'.format(
                            _id=sub_node_nid,
                            str_attr='', int_attr='', _from='', _to='', label=NODE_LABEL
                        )
                    )
                    ef.write(
                        '{_id};{parent_meta_id};{label}\n'.format(
                            _id=sub_node_nid, parent_meta_id=parent_sub_node_nid, label=MetaGraph.meta_label)
                    )
                    parent_sub_node_nid = sub_node_nid


if __name__ == '__main__':
    pass
    # write_headers()
    # write_nodes()
    # write_edges()
    # write_metas()
