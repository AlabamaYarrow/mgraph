import json
import random

import sys

from arango_graph.metagraph import key_to_id, MetaGraph


_10k = 10000
_50k = 50000
_100k = 100000
_1M = 1000000
_10M = 10000000


total_nodes = _10M
total_edges = _10M

TOTAL_METANODES = 0
META_WIDTH = 0
META_DEPTH = 0


nodes_file = 'doc-nodes.json'
edgenodes_file = 'doc-edgenodes.json'


def trunc_files():
    with open(nodes_file, 'w') as f:
        f.truncate()

    with open(edgenodes_file, 'w') as f:
        f.truncate()


def write_nodes():
    """
        arangoimp --file nodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
    """

    with open(nodes_file, 'a+') as f:
        for x in range(1, total_nodes + 1):
            nid = 'v{}'.format(x)
            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))

            f.write(
                '{{ "_key": "{nid}","int_attr":{int_attr},"str_attr":"{str_attr}" }}\n'.format(
                    nid=nid, int_attr=int_attr, str_attr=str_attr
                )
            )


def write_edges():
    """
        arangoimp --file nodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
    """

    edges = []

    with open(edgenodes_file, 'a+') as f:
        for x in range(1, total_nodes + 1):
            eid = 'e{}'.format(x)
            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))
            from_nid = 'v{}'.format(random.randint(1, total_nodes))
            to_nid = 'v{}'.format(random.randint(1, total_nodes))
            edges.append((eid, from_nid, to_nid))

            f.write(
                '{{ "_key": "{nid}","int_attr":{int_attr},"str_attr":"{str_attr}",'
                '"from":"{from_nid}","to":"{to_nid}" }}\n'.format(
                    nid=eid, int_attr=int_attr, str_attr=str_attr,
                    from_nid=key_to_id(MetaGraph.NODES_COLL, from_nid),
                    to_nid=key_to_id(MetaGraph.NODES_COLL, to_nid)
                )
            )


def _add_submetas(f, meta_nid, depth, meta_width):
    if not depth:
        return

    depth -= 1

    for j in range(1, meta_width + 1):
        sub_nid = '{}-{}'.format(meta_nid, j)
        int_attr = random.randint(1, 100)
        str_attr = 'attr_{}'.format(random.randint(1, 100))

        submeta_keys = []
        if depth:
            submeta_keys = json.dumps(["{}-{}".format(sub_nid, j) for j in range(1, meta_width + 1)])

        f.write(
            '{{ "_key": "{meta_nid}","int_attr":{int_attr},"str_attr":"{str_attr}",'
            '"_submeta":{__submeta},"_supermeta":{__supermeta} }}\n'.format(
                meta_nid=sub_nid, int_attr=int_attr, str_attr=str_attr,
                __submeta=submeta_keys,
                __supermeta=json.dumps([meta_nid])
            )
        )

        _add_submetas(f, sub_nid, depth, meta_width)


def write_metas(start_metanid=1, total_metanodes=TOTAL_METANODES, meta_width=META_WIDTH, meta_depth=META_DEPTH):
    with open(nodes_file, 'a+') as f:
        for x in range(start_metanid, start_metanid+total_metanodes + 1):
            meta_nid = 'm{}'.format(1)

            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))

            submeta_keys = json.dumps(["{}-{}".format(meta_nid, j) for j in range(1, meta_width + 1)])

            f.write(
                '{{ "_key": "{meta_nid}","int_attr":{int_attr},"str_attr":"{str_attr}",'
                '"_submeta":{__submeta},"_supermeta":{__supermeta} }}\n'.format(
                    meta_nid=meta_nid, int_attr=int_attr, str_attr=str_attr,
                    __submeta=submeta_keys,
                    __supermeta=[]
                )
            )

            _add_submetas(f, meta_nid, meta_depth, meta_width)


if __name__ == '__main__':
    trunc_files()

    # write_nodes()
    # write_edges()

    sys.setrecursionlimit(20000)  # sorry Python :(

    write_metas(start_metanid=1, total_metanodes=10, meta_width=10, meta_depth=1)
    write_metas(start_metanid=100, total_metanodes=10, meta_width=100, meta_depth=1)
    write_metas(start_metanid=200, total_metanodes=10, meta_width=1000, meta_depth=1)
    write_metas(start_metanid=300, total_metanodes=1, meta_width=10000, meta_depth=1)

    write_metas(start_metanid=400, total_metanodes=10, meta_width=1, meta_depth=10)
    write_metas(start_metanid=500, total_metanodes=10, meta_width=1, meta_depth=100)
    write_metas(start_metanid=600, total_metanodes=10, meta_width=1, meta_depth=1000)
    write_metas(start_metanid=700, total_metanodes=1, meta_width=1, meta_depth=10000)

