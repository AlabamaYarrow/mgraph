import time
from pyArango.connection import Connection
from app_arango.settings import USERNAME, PASSWORD, DB_NAME


# TODO effect of rawResults=True  batchSize=100 for AQLQuery()


def key_to_id(coll, key):
    return '{}/{}'.format(coll, key)


# noinspection PyProtectedMember
class MetaGraph:
    """
    Arango metagraph API.
    """

    # Arango collections.
    # Possible to have separate collection for different edges / nodes.
    # (requires indexing)
    NODES_COLL = 'Nodes'  # normal nodes and metanodes
    EDGENODE_COLL = 'Nodes'  # actual m-graph edges (stored as nodes)
    EDGES_COLL = 'NodesConnections'  # internal arango edges
    METAEDGES_COLL = 'NodesConnections'  # 'submeta' edges

    GRAPH = 'NodesGraph'

    METAEDGE_LABEL = 'submeta'

    def __init__(self):
        conn = Connection(username=USERNAME, password=PASSWORD)
        self.db = conn[DB_NAME]

        self.nodes = self.db[self.NODES_COLL]
        self.edges = self.db[self.EDGES_COLL]
        self.edges_nodes = self.db[self.EDGENODE_COLL]
        self.meta_edges = self.db[self.METAEDGES_COLL]

    def truncate(self):
        self.nodes.truncate()
        self.edges_nodes.truncate()
        self.edges.truncate()
        self.meta_edges.truncate()

    def add_node_json(self, node_string):
        """
        :param node_string: node json string
            '{"param": "value", ...}'
        """
        aql = '''
            INSERT {node_string} INTO {nodes_collection}
            RETURN NEW
        '''.format(
            node_string=node_string,
            nodes_collection=self.NODES_COLL
        )
        return self.db.AQLQuery(aql)

    def add_node(self, **kwargs):
        """
        Not slower than AQL (tested).
        """
        node = self.nodes.createDocument()
        node._key = kwargs['nid']
        for k, v in kwargs.items():
            node[k] = v
        node.save()
        return node

    def _add_edge(self, from_node, to_node, submeta=False):
        """
        Add internal edge.
        """
        edges_coll = self.meta_edges if submeta else self.edges
        edge = edges_coll.createDocument()
        edge._from = from_node._id
        edge._to = to_node._id
        if submeta:
            edge[self.METAEDGE_LABEL] = True
        edge.save()
        return edge

    def _add_edge_submeta(self, from_node, to_node):
        return self._add_edge(from_node, to_node, submeta=True)

    def add_edge(self, from_node, to_node, **kwargs):
        edge_node = self.nodes.createDocument()
        edge_node['from'] = from_node._id
        edge_node['to'] = to_node._id
        for k, v in kwargs.items():
            edge_node[k] = v
        edge_node.save()
        self._add_edge(from_node=from_node, to_node=edge_node)
        self._add_edge(from_node=edge_node, to_node=to_node)
        return edge_node

    def add_to_metanode(self, node, meta_node):
        self._add_edge_submeta(node, meta_node)

    def add_edge_to_metanode(self, from_node, to_node, meta_node):
        """
        Find edgenode from from_node to to_node and add it to meta_node.
        """
        aql = '''
            FOR n IN {nodes_collection}
            FILTER n.from == '{from_id}' AND n.to == '{to_id}'
            RETURN n
        '''.format(
            from_id=from_node._id, to_id=to_node._id,
            nodes_collection=self.EDGENODE_COLL
        )
        edge_node = self.db.AQLQuery(aql)[0]
        self._add_edge_submeta(edge_node, meta_node)

    def remove_node(self, node, remove_submeta=False):
        """
        Remove node. Must remove node adjacent edges,
        edgenodes and edgenodes adjacent edges.
        :param remove_submeta: remove content of metanode
        """
        def _remove_node(_node_key):
            """
            Remove internal node and its edges.
            """
            # Automatic removal of adjacent edges is
            # probably not supported in Arango yet
            _aql = '''
                LET keys = (FOR v, e IN 1..1 ANY '{node_id}' GRAPH '{graph}' REMOVE e._key IN {edges_collection})
                REMOVE '{node_key}' IN {nodes_collection}
            '''.format(
                node_id=key_to_id(self.NODES_COLL, _node_key),
                graph=self.GRAPH,
                edges_collection=self.EDGES_COLL,
                nodes_collection=self.NODES_COLL,
                edgenodes_collection=self.EDGES_COLL,
                node_key=_node_key
            )
            print(_aql)
            self.db.AQLQuery(_aql)

        if type(node) is str:
            node_key = node
        else:
            node_key = node._key

        _remove_node(node_key)

        aql = '''
            FOR e in {edgenodes_collection} FILTER e.from=='{node_id}' OR e.to=='{node_id}' RETURN e
        '''.format(
            edgenodes_collection=self.EDGENODE_COLL,
            node_id=key_to_id(self.NODES_COLL, node_key),
        )
        print('edge node aql', aql)
        print('node key', key_to_id(self.NODES_COLL, node_key))
        edge_nodes = self.db.AQLQuery(aql)
        print('edge nodes', edge_nodes)
        for edge_node in edge_nodes:
            _remove_node(edge_node._key)

        if remove_submeta:
            pass  # TODO

    def remove_from_metanode(self, node, meta_node):
        # TODO
        pass

    def remove_edge_metanode(self, from_node, to_node, meta_node):
        # TODO
        pass


def main():
    m = MetaGraph()
    m.truncate()

    n1 = m.add_node(name='V1', nid='1')
    n2 = m.add_node(name='V2', nid='2')
    n3 = m.add_node(name='V3', nid='3')
    e12 = m.add_edge(from_node=n1, to_node=n2)  # OR m.add_edge_to_metanode(n1, n2, mv1)
    e13 = m.add_edge(from_node=n1, to_node=n3)  # OR m.add_edge_to_metanode(n1, n2, mv1)

    m.remove_node(n3)

    # mv1 = m.add_node(name='MV1', nid='m1')
    #
    # m.add_to_metanode(n1, mv1)
    # m.add_to_metanode(n2, mv1)
    # m.add_to_metanode(e12, mv1)
    #
    # m.remove_node(n3)

    # print(m.add_node_json('{"name": "Berlin"}'))
    # print(m.add_node(name='Berlin'))

    # total_nodes = 10000

    # start_time = time.time()
    # for i in range(total_nodes):
    #     m.add_node_json('{"name": "Berlin"}')
    # print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
