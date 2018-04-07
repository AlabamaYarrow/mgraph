import time
from pyArango.connection import Connection
from app_arango.settings import USERNAME, PASSWORD, DB_NAME


# TODO effect of rawResults=True  batchSize=100 for AQLQuery()


# noinspection PyProtectedMember
class MetaGraph:
    NODES_COLL = 'Nodes'
    EDGENODE_COLL = 'Nodes'  # m-graph edges are stored as nodes

    EDGES_COLL = 'NodesConnections'  # normal edges
    METAEDGES_COLL = 'NodesConnections'  # for 'submeta' relations

    METAEDGE_LABEL = 'submeta'

    def __init__(self):
        conn = Connection(username=USERNAME, password=PASSWORD)
        self.db = conn[DB_NAME]
        self.nodes = self.db[self.NODES_COLL]
        self.edges = self.db[self.EDGES_COLL]

    def truncate(self):
        self.nodes.truncate()
        self.edges.truncate()

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
        Add internal edges.
        Actual metagraph edges are stored as separate documents.
        """
        edge = self.edges.createDocument()
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
        for k, v in kwargs.items():
            edge_node[k] = v
        edge_node.save()
        self._add_edge(from_node=from_node, to_node=edge_node)
        self._add_edge(from_node=edge_node, to_node=to_node)
        return edge_node

    def add_to_metanode(self, node, meta_node):
        self._add_edge_submeta(node, meta_node)


def main():
    m = MetaGraph()
    m.truncate()

    n1 = m.add_node(name='V1', nid='1')
    n2 = m.add_node(name='V2', nid='2')
    e12 = m.add_edge(from_node=n1, to_node=n2)

    mv1 = m.add_node(name='MV1', nid='3')

    m.add_to_metanode(n1, mv1)
    m.add_to_metanode(n2, mv1)
    m.add_to_metanode(e12, mv1)

    # print(m.add_node_json('{"name": "Berlin"}'))
    # print(m.add_node(name='Berlin'))

    # total_nodes = 10000

    # start_time = time.time()
    # for i in range(total_nodes):
    #     m.add_node_json('{"name": "Berlin"}')
    # print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
