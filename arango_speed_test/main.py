from app_arango_doc.metagraph import MetaGraph as DMetaGraph
from app_arango.metagraph import MetaGraph as GMetaGraph
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


def main():
    # test_add_nodes()
    test_add_connection()


if __name__ == '__main__':
    main()
