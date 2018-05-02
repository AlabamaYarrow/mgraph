from arango_doc.metagraph import MetaGraph as DMetaGraph
from arango_graph.metagraph import MetaGraph as GMetaGraph
import time

graphs = (DMetaGraph, GMetaGraph)


def print_debug(s, debug):
    if debug:
        print(s)


def add_nodes(m, total_nodes, debug=True):
    print_debug('Adding {} nodes'.format(total_nodes), debug)
    nids = ['v' + str(i) for i in range(1, total_nodes + 1)]

    start_time = time.time()
    for i in range(total_nodes):
        m.add_node(nid=nids[i])
    print_debug("--- %s seconds ---" % (time.time() - start_time), debug)


def test_add_nodes():
    m = GMetaGraph()
    m.truncate()

    total_nodes_list = (100, 1000, 1000)
    for total_nodes in total_nodes_list:
        add_nodes(m, total_nodes)
        m.truncate()

    """
    doc
    Adding 100 nodes
    --- 0.21915793418884277 seconds ---
    Adding 1000 nodes
    --- 2.212238311767578 seconds ---
    Adding 10000 nodes
    --- 21.726237535476685 seconds ---
    
    graph
    Adding 100 nodes
    --- 0.22184014320373535 seconds ---
    Adding 1000 nodes
    --- 2.2057831287384033 seconds ---
    Adding 10000 nodes
    --- 23.06024169921875 seconds ---
    """


def connect_nodes(m, total_nodes, debug=True):
    print_debug('Connecting {} nodes'.format(total_nodes), debug)
    nids = ['v' + str(i) for i in range(1, total_nodes + 1)]
    eids = ['e' + str(i) for i in range(1, total_nodes + 1)]

    start_time = time.time()
    for i in range(total_nodes - 1):
        m.add_edge(eid=eids[i], from_node=nids[i], to_node=nids[i+1])
    print_debug("--- %s seconds ---" % (time.time() - start_time), debug)


def test_add_connection():
    m = DMetaGraph()
    m.truncate()

    total_nodes_list = (100, 1000, 10000)

    for total_nodes in total_nodes_list:
        add_nodes(m, total_nodes, debug=False)
        connect_nodes(m, total_nodes, debug=True)
        m.truncate()

    '''
    doc
    Connecting 100 nodes
    --- 0.2300407886505127 seconds ---
    Connecting 1000 nodes
    --- 2.3181328773498535 seconds ---
    Connecting 10000 nodes
    --- 22.464250087738037 seconds ---
    
    
    graph
    Connecting 100 nodes
    --- 0.6676936149597168 seconds ---
    Connecting 1000 nodes
    --- 6.717860698699951 seconds ---
    Connecting 10000 nodes
    --- 76.9126443862915 seconds ---
    '''


def add_to_metanode(m, total_nodes, debug=True):
    meta_node_id = 'mv1'
    m.add_node(nid=meta_node_id)

    nids = ['v' + str(i) for i in range(1, total_nodes + 1)]

    print_debug('Adding {} nodes to metanode'.format(total_nodes), debug)
    start_time = time.time()
    for i in range(total_nodes):
        m.add_to_metanode(nids[i], meta_node_id)
    print_debug("--- %s seconds ---" % (time.time() - start_time), debug)


def test_add_to_metanode():
    m = GMetaGraph()
    m.truncate()

    total_nodes_list = (100, 1000, 10000)

    for total_nodes in total_nodes_list:
        add_nodes(m, total_nodes, debug=False)
        add_to_metanode(m, total_nodes, debug=True)
        m.truncate()

    '''
    doc
    Adding 100 nodes to metanode
    --- 0.49797725677490234 seconds ---
    Adding 1000 nodes to metanode
    --- 5.491415977478027 seconds ---
    Adding 10000 nodes to metanode
    --- 55.47690224647522 seconds ---
    
    graph
    Adding 100 nodes to metanode
    --- 0.2504541873931885 seconds ---
    Adding 1000 nodes to metanode
    --- 2.2059895992279053 seconds ---
    Adding 10000 nodes to metanode
    --- 22.201239347457886 seconds ---
    '''


def test_remove_node_with_adjacent():
    m = DMetaGraph()
    m.truncate()

    total_nodes_list = (100, 1000, 10000)

    for total_nodes in total_nodes_list:
        nids = ['v' + str(i) for i in range(1, total_nodes + 1)]
        eids = ['e' + str(i) for i in range(1, total_nodes + 1)]
        supernode_key = 'supernode'
        m.add_node(nid=supernode_key)
        add_nodes(m, total_nodes, debug=False)

        for i in range(total_nodes):
            m.add_edge(nids[i], supernode_key, eid=eids[i])

        print_debug('Removing node with {} adjacent'.format(total_nodes), debug=True)
        start_time = time.time()
        m.remove_node(supernode_key)
        print_debug("--- %s seconds ---" % (time.time() - start_time), debug=True)

        m.truncate()

    '''
    graph
    Removing node with 100 adjacent
    --- 0.2890477180480957 seconds ---
    Removing node with 1000 adjacent
    --- 2.7139780521392822 seconds ---
    Removing node with 10000 adjacent
    --- 26.949836015701294 seconds ---
    
    doc
    Removing node with 100 adjacent
    --- 0.014901399612426758 seconds ---
    Removing node with 1000 adjacent
    --- 0.015384912490844727 seconds ---
    Removing node with 10000 adjacent
    --- 0.0642392635345459 seconds ---
    '''


def test_remove_node_with_subnodes():
    m = GMetaGraph()
    m.truncate()

    total_nodes_list = (100, 1000, 10000)

    for total_nodes in total_nodes_list:
        nids = ['v' + str(i) for i in range(1, total_nodes + 1)]
        supernode_key = 'supernode'
        m.add_node(nid=supernode_key)
        add_nodes(m, total_nodes, debug=False)

        for i in range(0, total_nodes):
            m.add_to_metanode(nids[i], supernode_key)

        print_debug('Removing node with {} subnodes'.format(total_nodes), debug=True)
        start_time = time.time()
        m.remove_node(supernode_key, remove_submeta=True, recursive=True)
        print_debug("--- %s seconds ---" % (time.time() - start_time), debug=True)

        m.truncate()

    '''
    doc
    Removing node with 100 subnodes
    --- 0.987907886505127 seconds ---
    Removing node with 1000 subnodes
    --- 9.836540460586548 seconds ---
    Removing node with 10000 subnodes
    --- 117.414470911026 seconds ---
    
    graph
    Removing node with 100 subnodes
    --- 0.7898082733154297 seconds ---
    Removing node with 1000 subnodes
    --- 10.11392879486084 seconds ---
    Removing node with 10000 subnodes
    --- 110.25088453292847 seconds ---
    '''


def test_get_node_content():
    m = DMetaGraph()
    m.truncate()

    total_nodes_list = (100, 1000, 10000)

    for total_nodes in total_nodes_list:
        nids = ['v' + str(i) for i in range(1, total_nodes + 1)]
        supernode_key = 'supernode'
        m.add_node(nid=supernode_key)
        add_nodes(m, total_nodes, debug=False)

        for i in range(0, total_nodes):
            m.add_to_metanode(nids[i], supernode_key)

        print_debug('Getting metanode content with {} subnodes'.format(total_nodes), debug=True)
        start_time = time.time()
        m.get_submeta_nodes(supernode_key)
        print_debug("--- %s seconds ---" % (time.time() - start_time), debug=True)

        m.truncate()

        '''
        graph
        Getting metanode content with 100 subnodes
        --- 0.003159046173095703 seconds ---
        Getting metanode content with 1000 subnodes
        --- 0.005234956741333008 seconds ---
        Getting metanode content with 10000 subnodes
        --- 0.032373905181884766 seconds ---
        
        doc
        Getting metanode content with 100 subnodes
        --- 0.0029277801513671875 seconds ---
        Getting metanode content with 1000 subnodes
        --- 0.004237651824951172 seconds ---
        Getting metanode content with 10000 subnodes
        --- 0.013964414596557617 seconds ---

        '''


def main():
    # test_add_nodes()
    # test_add_connection()
    # test_add_to_metanode()

    # test_remove_node_with_adjacent()
    # test_remove_node_with_subnodes()

    test_get_node_content()


if __name__ == '__main__':
    main()
