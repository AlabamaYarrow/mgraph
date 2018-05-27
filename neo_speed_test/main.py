import logging
import os
import subprocess
import time
from collections import OrderedDict

import settings
from neo4j_graph.metagraph import MetaGraph as NGMetaGraph

from neo4j_mgraph_generator import (
    write_dummy_nodes,
    write_metas,
    write_headers as trunc_files,
)

logger = logging.getLogger()


graphs = (NGMetaGraph,)


def trim_time(_time):
    return "{:.5f}".format(_time)


def log_time(total_time, total_nodes):
    logger.warning("\tTotal \t%s seconds" % trim_time(total_time))
    logger.warning("\tAvg \t%s seconds" % trim_time(total_time / total_nodes))


def test_add_remove_nodes_and_edges(m):
    # Adding new nodes:
    total_nodes = 10
    logger.warning('Testing {} new nodes addition'.format(total_nodes))

    nids = ['new_vertex_' + str(i) for i in range(1, total_nodes + 1)]

    start_time = time.time()
    for i in range(total_nodes):
        m.add_node(nid=nids[i])
    total_time = time.time() - start_time
    log_time(total_time, total_nodes)

    # Updating nodes"
    logger.warning('Testing {} changing node attribute'.format(total_nodes))
    start_time = time.time()
    for i in range(total_nodes):
        m.update_node(node=nids[i], str_attr='new_attr')
    total_time = time.time() - start_time
    log_time(total_time, total_nodes)

    # Adding new edges:
    total_edges = total_nodes - 1
    logger.warning('Testing {} new edges addition'.format(total_edges))
    eids = ['new_edge_' + str(i) for i in range(1, total_edges + 1)]

    start_time = time.time()
    for i in range(total_edges):
        m.add_edge(eid=eids[i], from_node=nids[i], to_node=nids[i+1])
    total_time = time.time() - start_time
    log_time(total_time, total_edges)

    # Removing edges:
    logger.warning('Removing edges (by key)'.format(total_edges))
    start_time = time.time()
    for i in range(total_edges):
        m.remove_node(node=eids[i])
    total_time = time.time() - start_time
    log_time(total_time, total_edges)

    # Removing nodes:
    logger.warning('Removing unconnected nodes'.format(total_edges))
    start_time = time.time()
    for i in range(total_nodes):
        m.remove_node(node=nids[i])
    total_time = time.time() - start_time
    log_time(total_time, total_edges)


def test_add_remove_to_metanode(m):
    # Adding nodes to metanode:
    total_nodes = 10
    logger.warning('Testing adding {} nodes to metanode'.format(total_nodes))

    nids = ['new_subnode_' + str(i) for i in range(1, total_nodes + 1)]
    for i in range(total_nodes):
        m.add_node(nid=nids[i])

    new_metanode = m.add_node(nid='new_metanode')

    start_time = time.time()
    for i in range(total_nodes):
        m.add_to_metanode(node=nids[i], metanode=new_metanode)
    total_time = time.time() - start_time
    log_time(total_time, total_nodes)

    # Removing nodes from metanode:
    logger.warning('Testing removing {} nodes from metanode'.format(total_nodes))
    start_time = time.time()
    for i in range(total_nodes):
        m.remove_from_metanode(node=nids[i], metanode=new_metanode)
    total_time = time.time() - start_time
    log_time(total_time, total_nodes)


def test_get_submeta(m, meta_nids):
    logger.warning('Testing getting sub meta nodes')

    # global meta_nids_doc
    # print(meta_nids_doc)
    # with open('metanids.data', 'wb') as output:
    #     pickle.dump(meta_nids_doc, output, pickle.HIGHEST_PROTOCOL)
    # with open('metanids.data', 'rb') as input:
    #     meta_nids_doc = pickle.load(input)

    for (width, depth), nids in meta_nids.items():
        logger.warning("  getting content of metanode with width {} and depth {}".format(width, depth))

        total_nodes = len(nids)

        nids = ['m{}'.format(nid) for nid in nids]

        start_time = time.time()
        for nid in nids:
            nodes = m.get_submeta_nodes(nid)
            if settings.DEBUG:
                print(len(nodes), 'width: ', width, 'depth: ', depth)
                assert len(nodes) == width * depth

        total_time = time.time() - start_time
        log_time(total_time, total_nodes)


def test_remove_metanodes_deep(m, meta_nids):
    logger.warning('Testing removing metanodes with content')
    for (width, depth), nids in meta_nids.items():
        if isinstance(m, NGMetaGraph) and width > 1000:  # too slow
            continue
        logger.warning("  removing node and sub meta nodes with width {} and depth {}".format(width, depth))

        total_nodes = len(nids)

        nids = ['m{}'.format(nid) for nid in nids]

        start_time = time.time()
        for nid in nids:
            m.remove_node(nid, remove_submeta=True)
            if settings.DEBUG:
                print('width: ', width, 'depth: ', depth)
                assert not m.get_submeta_nodes(nid)

        total_time = time.time() - start_time
        log_time(total_time, total_nodes)


def test_remove_metanodes(m, meta_nids):
    logger.warning('Testing removing metanodes without content')
    for (width, depth), nids in meta_nids.items():
        # testing only on metanodes with depth==1 and many first level subnodes:
        if width == 1:
            continue
        logger.warning("  removing node without submeta,  width {}".format(width, depth))

        total_nodes = len(nids)

        nids = ['m{}'.format(nid) for nid in nids]

        start_time = time.time()
        for nid in nids:
            m.remove_node(nid, remove_submeta=False)

        total_time = time.time() - start_time
        log_time(total_time, total_nodes)


# Remember ids of meta vertices in graph for test
meta_nids_doc = OrderedDict()
meta_nids_graph = OrderedDict()
NIDS_PER_TYPE = 10


def init_dump(graph_type):
    meta_nids = meta_nids_graph

    trunc_files()

    if settings.DEBUG:
        write_dummy_nodes(0, 0)
    else:
        write_dummy_nodes()

    current_meta_nid = 1
    total_metanodes = NIDS_PER_TYPE

    # configs (width, depth)
    #   width - number of submetanodes for each node
    #   depth - number of levels
    meta_configurations = (
        (1, 10),
        (1, 50),
        (1, 100),

        # (1, 1000),
        # (1, 10000),

        (10, 1),
        (100, 1),
        (500, 1),
        (1000, 1),
        (5000, 1),
        (10000, 1),
    )

    # meta_configurations = (
    #     (1, 10),
    #
    #     (10, 1),
    # )

    for width, depth in meta_configurations:
        meta_nids[(width, depth)] = list(
            range(current_meta_nid, current_meta_nid + total_metanodes)
        )

        write_metas(
            start_metanid=current_meta_nid, total_metanodes=total_metanodes,
            meta_width=width, meta_depth=depth
        )
        current_meta_nid += total_metanodes


def load_dump(graph_type):
    # logger.warning("Truncating db...")
    # subprocess.Popen([
    #     '/usr/bin/pkexec',
    #     'sh',
    #     os.path.join(settings.BASE_DIR, 'neo_trunc_{}.sh'.format(graph_type))]
    # ).wait()
    # time.sleep(5)  # because neo4j likes to connrefuse after startup

    # subprocess.call(['/usr/bin/pkexec', 'sh', os.path.join(settings.BASE_DIR, 'neo_trunc_{}.sh'.format(graph_type))])

    logger.warning("Loading dump...")
    subprocess.call([os.path.join(settings.BASE_DIR, 'neo_import_{}.sh'.format(graph_type))])


def run_tests():
    logger.warning('Starting test...')

    logger.warning('\n******NEO GRAPH MODEL******')

    init_dump('graph')

    load_dump('graph')

    mgraph_graph = NGMetaGraph()

    test_add_remove_nodes_and_edges(mgraph_graph)
    test_add_remove_to_metanode(mgraph_graph)
    test_get_submeta(mgraph_graph, meta_nids_graph)
    test_remove_metanodes(mgraph_graph, meta_nids_graph)

    load_dump('graph')
    mgraph_graph = NGMetaGraph()
    test_remove_metanodes_deep(mgraph_graph, meta_nids_graph)


if __name__ == '__main__':
    run_tests()
