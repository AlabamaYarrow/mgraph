from arango_doc.metagraph import MetaGraph as DMetaGraph
from arango_graph.metagraph import MetaGraph as GMetaGraph
import time

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


def main():
    test_add()


if __name__ == '__main__':
    main()
