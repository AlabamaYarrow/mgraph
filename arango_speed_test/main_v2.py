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
from mgraph_generator import (
    write_nodes as gr_write_nodes,
    write_edges as gr_write_edges,
    write_metas as gr_write_metas,
    write_headers as gr_write_headers,
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


def test_get_submeta(m, meta_nids):

    # global meta_nids_doc
    # print(meta_nids_doc)
    # with open('metanids.data', 'wb') as output:
    #     pickle.dump(meta_nids_doc, output, pickle.HIGHEST_PROTOCOL)
    # with open('metanids.data', 'rb') as input:
    #     meta_nids_doc = pickle.load(input)

    for (width, depth), nids in meta_nids.items():
        print_debug("Getting sub meta nodes with width {} and depth {}".format(width, depth), True)

        total_nodes = len(nids)

        nids = ['m{}'.format(nid) for nid in nids]

        start_time = time.time()
        for nid in nids:
            nodes = m.get_submeta_nodes(nid)
            if settings.DEBUG:
                print(len(nodes), 'width: ', width, 'depth: ', depth)
                assert len(nodes) == width * depth

        total_time = time.time() - start_time
        print_debug("Total --- %s seconds ---" % total_time, True)
        print_debug("Avg --- %s seconds ---" % (total_time / total_nodes), True)


# Remember ids of meta vertices in graph for test
meta_nids_doc = {}
meta_nids_graph = {}
NIDS_PER_TYPE = 10


def init_dump(graph_type):
    if graph_type == 'doc':
        trunc_files = doc_trunc_files
        write_nodes = doc_write_nodes
        write_edges = doc_write_edges
        write_metas = doc_write_metas
        meta_nids = meta_nids_doc
    else:
        trunc_files = gr_write_headers
        write_nodes = gr_write_nodes
        write_edges = gr_write_edges
        write_metas = gr_write_metas
        meta_nids = meta_nids_graph

    trunc_files()

    #  TODO not 0
    write_nodes(0)
    write_edges(0, 0)

    current_meta_nid = 1

    total_metanodes = NIDS_PER_TYPE
    depth = 1
    for width in [10, 100, 1000, 10000]:
        # total_metanodes = NIDS_PER_TYPE if depth < 10000 else 1
        meta_nids[(width, depth)] = list(
            range(current_meta_nid, current_meta_nid + total_metanodes)
        )

        write_metas(
            start_metanid=current_meta_nid, total_metanodes=total_metanodes,
            meta_width=width, meta_depth=depth
        )
        current_meta_nid += total_metanodes

    width = 1
    for depth in [10, 100, 1000, 10000]:
        # total_metanodes = NIDS_PER_TYPE if width < 10000 else 1
        meta_nids[(width, depth)] = list(
            range(current_meta_nid, current_meta_nid + total_metanodes)
        )

        write_metas(
            start_metanid=current_meta_nid, total_metanodes=total_metanodes,
            meta_width=width, meta_depth=depth
        )
        current_meta_nid += total_metanodes


def load_dump(graph_type):
    print("Truncating db...")
    subprocess.call([os.path.join(settings.BASE_DIR, 'arango_trunc_{}.sh'.format(graph_type))])
    print("Loading dump...")
    subprocess.call([os.path.join(settings.BASE_DIR, 'arango_import_{}.sh'.format(graph_type))])


def main():
    print('Starting test...')
    # init_doc_dump()
    # load_doc_dump()

    # test_add()
    # test_get_submeta()
    # test_remove_without_submeta()
    # test_remove_submeta()

    # mgraph_doc = DMetaGraph()
    # mgraph_doc.truncate()
    # init_dump('doc')
    # load_dump('doc')
    # test_get_submeta(mgraph_doc, meta_nids_doc)

    mgraph_graph = GMetaGraph()
    mgraph_graph.truncate()
    init_dump('graph')
    load_dump('graph')
    test_get_submeta(mgraph_graph, meta_nids_graph)


if __name__ == '__main__':
    main()
