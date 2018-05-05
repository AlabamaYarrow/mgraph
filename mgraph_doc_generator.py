import json
import os
import random
import sys

from arango_graph.metagraph import MetaGraph
from settings import DUMPS_DIR

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


nodes_file = os.path.join(DUMPS_DIR, 'doc', 'doc-nodes.json')
edgenodes_file = os.path.join(DUMPS_DIR, 'doc', 'doc-edgenodes.json')


def trunc_files():
    with open(nodes_file, 'w') as f:
        f.truncate()

    with open(edgenodes_file, 'w') as f:
        f.truncate()


def write_nodes(total_nodes=TOTAL_NODES):
    """
        arangoimp --file nodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
    """
    print('Generating source file for {} nodes...'.format(total_nodes))

    with open(nodes_file, 'a+') as f:
        for x in range(1, total_nodes + 1):
            if not x % 50000:
                print('{} done'.format(x))

            nid = 'v{}'.format(x)
            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))

            f.write(
                '{{ "_key": "{nid}","int_attr":{int_attr},"str_attr":"{str_attr}" }}\n'.format(
                    nid=nid, int_attr=int_attr, str_attr=str_attr
                )
            )


def write_edges(total_nodes=TOTAL_NODES, total_edges=TOTAL_EDGES):
    """
        arangoimp --file nodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
    """
    print('Generating source file for {} edges...'.format(total_edges))

    edges = []

    with open(edgenodes_file, 'a+') as f:
        for x in range(1, total_edges + 1):
            if not x % 50000:
                print('{} done'.format(x))

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
                    from_nid=MetaGraph.key_to_id(from_nid),
                    to_nid=MetaGraph.key_to_id(to_nid)
                )
            )


# Common incremented key for submeta nodes to avoid collision
submeta_pk = 0


def _add_submetas(f, root_nid, meta_nid, depth, meta_width, keys_for_this_level):
    """
    :param root_nid: root metanode
    :param meta_nid: parent metanode for this level
    :param depth: current level (reversed, 0 is lowest)
    :param meta_width: number of nodes to generate on this level
    :param keys_for_this_level: node keys for this level (passed from parent
                node because it must know them)
    """
    global submeta_pk
    if not depth:
        return

    depth -= 1

    for i in range(0, meta_width):
        sub_nid = keys_for_this_level[i]
        int_attr = random.randint(1, 100)
        str_attr = 'attr_{}'.format(random.randint(1, 100))

        submeta_keys = []
        if depth:
            submeta_keys = ['{}-d{}-{}'.format(
                root_nid,
                depth - 1,
                submeta_pk + j)
                for j in range(1, meta_width + 1)]

            submeta_pk += meta_width

        f.write(
            '{{ "_key": "{meta_nid}","int_attr":{int_attr},"str_attr":"{str_attr}",'
            '"_submeta":{_submeta},"_supermeta":{_supermeta} }}\n'.format(
                meta_nid=sub_nid, int_attr=int_attr, str_attr=str_attr,
                _submeta=json.dumps(submeta_keys),
                _supermeta=json.dumps([meta_nid])
            )
        )

        _add_submetas(f, root_nid, sub_nid, depth, meta_width, submeta_keys)


def write_metas(start_metanid=1, total_metanodes=TOTAL_METANODES, meta_width=META_WIDTH, meta_depth=META_DEPTH):
    sys.setrecursionlimit(20000)  # sorry Python :(

    with open(nodes_file, 'a+') as f:
        for x in range(start_metanid, start_metanid + total_metanodes):
            meta_nid = 'm{}'.format(x)

            int_attr = random.randint(1, 100)
            str_attr = 'attr_{}'.format(random.randint(1, 100))

            submeta_keys = ['{}-d{}-{}'.format(meta_nid, meta_depth - 1, j)
                            for j in range(1, meta_width + 1)]

            f.write(
                '{{ "_key": "{meta_nid}","int_attr":{int_attr},"str_attr":"{str_attr}",'
                '"_submeta":{__submeta},"_supermeta":{__supermeta} }}\n'.format(
                    meta_nid=meta_nid, int_attr=int_attr, str_attr=str_attr,
                    __submeta=json.dumps(submeta_keys),
                    __supermeta=[]
                )
            )

            _add_submetas(f, meta_nid, meta_nid, meta_depth, meta_width, submeta_keys)


if __name__ == '__main__':
    pass
    # trunc_files()

    # write_nodes()
    # write_edges()
    #
    # write_metas(start_metanid=1, total_metanodes=10, meta_width=10, meta_depth=1)
    # write_metas(start_metanid=100, total_metanodes=10, meta_width=100, meta_depth=1)
    # write_metas(start_metanid=200, total_metanodes=10, meta_width=1000, meta_depth=1)
    # write_metas(start_metanid=300, total_metanodes=1, meta_width=10000, meta_depth=1)
    #
    # write_metas(start_metanid=400, total_metanodes=10, meta_width=1, meta_depth=10)
    # write_metas(start_metanid=500, total_metanodes=10, meta_width=1, meta_depth=100)
    # write_metas(start_metanid=600, total_metanodes=10, meta_width=1, meta_depth=1000)
    # write_metas(start_metanid=700, total_metanodes=1, meta_width=1, meta_depth=10000)

