#!/usr/bin/env python
from app.metagraph import Metagraph


if __name__ == '__main__':
    Metagraph.flush_db()

    total_nodes = 10

    Metagraph.create_node(label='town', nid=1)
    for i in range(2, total_nodes + 1):
        if not i % 200:
            print(i)
        Metagraph.create_node(label='town', nid=i)
        Metagraph.connect_nodes(from_id=i-1, to_id=i)


    """
    MATCH (t1:town {nid: 1})-[*..]->(t1:town {nid: 49})
    RETURN g1
    """

    # Metagraph.create_node(nid=2)
    # Metagraph.create_node(nid=3)
    # Metagraph.create_node(nid=4)

    # Metagraph.connect_nodes(from_id=1, to_id=2)
    # Metagraph.connect_nodes(from_id=3, to_id=4)
    #
    # Metagraph.add_to_metanode(node_id=1, meta_id=4)
    # Metagraph.add_to_metanode(node_id=2, meta_id=4)

    # Metagraph.match()
