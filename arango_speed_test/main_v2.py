import os
import pickle
import subprocess
import time


import settings
from arango_doc.metagraph import MetaGraph as DMetaGraph
from arango_graph.metagraph import MetaGraph as GMetaGraph
from mgraph_doc_generator import (
    write_nodes as doc_write_nodes,
    write_edges as doc_write_edges,
    write_metas as doc_write_metas,
    trunc_files as doc_trunc_files,
)

graphs = (DMetaGraph, GMetaGraph)


def print_debug(s, debug):
    if debug:
        print(s)


def test_add():
    """
    Db content - X nodes, X edges.
    """
    m = DMetaGraph()

    total_nodes = 100

    nids = ['nv' + str(i) for i in range(1, total_nodes + 1)]

    start_time = time.time()
    for i in range(total_nodes):
        m.add_node(nid=nids[i])

    total_time = time.time() - start_time
    print_debug("Total --- %s seconds ---" % total_time, True)
    print_debug("Avg --- %s seconds ---" % (total_time / total_nodes), True)

    """
    100k
    
    1M
    Total --- 0.22845983505249023 seconds ---
    Avg --- 0.002284598350524902 seconds ---
    
    10M
    ?
    """


# Remember ids of meta vertices in graph for test
meta_nids_doc = {}
NIDS_PER_TYPE = 10


def init_doc_dump():
    print("Initializing dump for document db...")

    doc_trunc_files()

    #  TODO not 0
    doc_write_nodes(0)
    doc_write_edges(0, 0)

    current_meta_nid = 1

    total_metanodes = NIDS_PER_TYPE
    depth = 1
    for width in [10, 100, 1000, 10000]:
        # total_metanodes = NIDS_PER_TYPE if depth < 10000 else 1
        meta_nids_doc[(width, depth)] = list(
            range(current_meta_nid, current_meta_nid + total_metanodes)
        )

        doc_write_metas(
            start_metanid=current_meta_nid, total_metanodes=total_metanodes,
            meta_width=width, meta_depth=depth
        )
        current_meta_nid += total_metanodes

    width = 1
    for depth in [10, 100, 1000, 10000]:
        # total_metanodes = NIDS_PER_TYPE if width < 10000 else 1
        meta_nids_doc[(width, depth)] = list(
            range(current_meta_nid, current_meta_nid + total_metanodes)
        )

        doc_write_metas(
            start_metanid=current_meta_nid, total_metanodes=total_metanodes,
            meta_width=width, meta_depth=depth
        )
        current_meta_nid += total_metanodes


def load_doc_dump():
    print("Truncating db...")
    subprocess.call([os.path.join(settings.BASE_DIR, 'arango_trunc_doc.sh')])
    print("Loading dump...")
    subprocess.call([os.path.join(settings.BASE_DIR, 'arango_import_doc.sh')])


def init_graph_dump():
    pass


def main():
    print('Starting test...')
    # init_doc_dump()
    # load_doc_dump()

    m = DMetaGraph()
    # test_add()
    global meta_nids_doc
    print(meta_nids_doc)

    # with open('metanids.data', 'wb') as output:
    #     pickle.dump(meta_nids_doc, output, pickle.HIGHEST_PROTOCOL)

    with open('metanids.data', 'rb') as input:
        meta_nids_doc = pickle.load(input)

    print(meta_nids_doc)

    print(len(m.get_submeta_nodes('m31')))

    # for (width, depth), nids in meta_nids_doc.items():
    #
    #     total_nodes = len(nids)
    #
    #     nids = ['m{}'.format(nid) for nid in nids]
    #
    #     start_time = time.time()
    #     for nid in nids:
    #         nodes = m.get_submeta_nodes(nid)
    #
    #         print(len(nodes), width, depth)
    #         assert len(nodes) == width * depth
    #
    #     total_time = time.time() - start_time
    #     print_debug("Getting sub meta nodes with width {} and depth {}".format(width, depth), True)
    #     print_debug("Total --- %s seconds ---" % total_time, True)
    #     print_debug("Avg --- %s seconds ---" % (total_time / total_nodes), True)


if __name__ == '__main__':
    main()
