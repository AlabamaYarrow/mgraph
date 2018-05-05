import json

from pyArango.connection import Connection
from arango_graph.settings import USERNAME, PASSWORD, DB_NAME, DEBUG


# TODO effect of rawResults=True  batchSize=100 for AQLQuery()

"""
NB: Creating edge collection and graph
        var graph_module = require("org/arangodb/general-graph");
        var edgeDefinitions = [{
            collection: "NodesConnections", from: [ "Nodes" ], to : [ "Nodes" ]
        }];
        graph = graph_module._create("NodesGraph", edgeDefinitions);
        
NB: edgenodes collection requires 
    hash indexes on "to" and "from"
    (sparse indexes if it's same collection with nodes)
    
    E.g. (arangosh command):
        db.Nodes.ensureIndex({ type: "hash", fields: [ "from" ], sparse: true });
        db.Nodes.ensureIndex({ type: "hash", fields: [ "to" ], sparse: true }); 
"""


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

    # Arango doesn't support unlimited depth traversal
    MAX_DEPTH = 100000

    MAX_BATCH_SIZE = 20000

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
        return self._run_aql(aql)

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

    def add_edge(self, from_node, to_node, **kwargs):
        edge_node = self.edges_nodes.createDocument()
        edge_node._key = kwargs['eid']
        edge_node['from'] = self.key_to_id(self._to_key(from_node))
        edge_node['to'] = self.key_to_id(self._to_key(to_node))
        for k, v in kwargs.items():
            edge_node[k] = v
        edge_node.save()
        self._add_edge(from_node=from_node, to_node=edge_node)
        self._add_edge(from_node=edge_node, to_node=to_node)
        return edge_node

    def add_to_metanode(self, node, metanode):
        self._add_edge_submeta(node, metanode)

    def filter_nodes(self, query=None, **kwargs):
        if not query:
            query = self._build_query_string(**kwargs)

        aql = '''
            FOR n in {nodes_collection}
            FILTER {query}
            RETURN n
        '''.format(
            query=query,
            nodes_collection=self.NODES_COLL
        )
        return self._run_aql(aql)

    def update_node(self, node, **kwargs):
        node_key = self._to_key(node)
        node = self.nodes[node_key]
        for k, v in kwargs.items():
            node[k] = v
        node.save()

    def update_edge(self, edge, **kwargs):
        edge_key = self._to_key(edge)
        edge = self.edges_nodes[edge_key]
        for k, v in kwargs.items():
            edge[k] = v
        edge.save()

    def remove_node(self, node, remove_submeta=False):
        """
        Remove node. Must remove node adjacent edges,
        edgenodes and edgenodes adjacent edges.
        :param remove_submeta: remove content of metanode
        """
        nodes_to_remove = [node]
        if remove_submeta:
            nodes_to_remove.extend(self.get_submeta_nodes(node))

        for removed_node in nodes_to_remove:
            node_key = self._to_key(removed_node)

            # Removing edgenodes
            _aql = '''
                FOR edgenode in {edgenodes_collection}
                FILTER edgenode.from=='{node_id}' OR edgenode.to=='{node_id}'
                    LET removed_inbound = (
                        FOR v, e IN 1..1 ANY edgenode._id GRAPH '{graph}' 
                        REMOVE e._key IN {edges_collection}
                    )
    
                    FOR n IN {nodes_collection}
                    FILTER n._id == edgenode._id
                    REMOVE n IN {nodes_collection}
            '''.format(
                edgenodes_collection=self.EDGENODE_COLL,
                node_id=self.key_to_id(node_key),
                graph=self.GRAPH,
                submeta_label=self.METAEDGE_LABEL,
                edges_collection=self.EDGES_COLL,
                nodes_collection=self.NODES_COLL,
            )
            self._run_aql(_aql)

            # Removing node itself
            self._remove_internal_node(node_key)

    def remove_from_metanode(self, node, metanode):
        node_key = self._to_key(node)
        metanode_key = self._to_key(metanode)
        aql = '''
            FOR e IN {edges_collection}
            FILTER e._from == '{node_id}' AND e._to == '{metanode_id}'
            REMOVE e IN {edges_collection}
        '''.format(
            edges_collection=self.EDGES_COLL,
            node_id=self.key_to_id(node_key),
            metanode_id=self.key_to_id(metanode_key)
        )
        self._run_aql(aql)

        return self._run_aql(aql)

    def get_submeta_nodes(self, node):
        aql = '''
            FOR n,e,p IN 1..{max_depth} INBOUND '{node_id}' {edges_collection}
            FILTER p.edges[*].{submeta_label} ALL == true
            RETURN n
        '''.format(
            max_depth=self.MAX_DEPTH,
            node_id=self.key_to_id(self._to_key(node)),
            edges_collection=self.EDGES_COLL,
            submeta_label=self.METAEDGE_LABEL
        )

        return self._run_aql(aql)

    def _add_edge(self, from_node, to_node, submeta=False):
        """
        Add internal edge.
        :param submeta: edge connects metanode with its content
        """
        edges_coll = self.meta_edges if submeta else self.edges
        edge = edges_coll.createDocument()

        edge._from = self.key_to_id(self._to_key(from_node))
        edge._to = self.key_to_id(self._to_key(to_node))
        if submeta:
            edge[self.METAEDGE_LABEL] = True
        edge.save()
        return edge

    def _add_edge_submeta(self, from_node, to_node):
        return self._add_edge(from_node, to_node, submeta=True)

    def _remove_internal_node(self, node_key):
        """
        Remove internal node and its edges.
        """
        # Automatic removal of adjacent edges is
        # probably not supported in Arango yet
        _aql = '''
            LET removed_inbound = (
                FOR v, e IN 1..1 ANY '{node_id}' GRAPH '{graph}' 
                REMOVE e._key IN {edges_collection}
            )
            
            FOR n IN {nodes_collection}
            FILTER n._key == '{node_key}'
            REMOVE n IN {nodes_collection}
        '''.format(
            node_id=self.key_to_id(node_key),
            graph=self.GRAPH,
            submeta_label=self.METAEDGE_LABEL,
            edges_collection=self.EDGES_COLL,
            nodes_collection=self.NODES_COLL,
            edgenodes_collection=self.EDGES_COLL,
            node_key=node_key
        )

        self._run_aql(_aql)

    @staticmethod
    def _build_query_string(**kwargs):
        query = ''
        for i, (k, v) in enumerate(kwargs.items()):
            if type(v) is str:
                query_template = 'n.{k}=="{v}"'
            else:
                query_template = 'n.{k}=={v}'
            query += query_template.format(k=k, v=v)
            if i != len(kwargs) - 1:
                query += ' AND '
        return query

    @staticmethod
    def _to_key(node):
        if type(node) is str:
            return node
        else:
            return node._key

    @staticmethod
    def key_to_id(key, coll=NODES_COLL):
        return '{}/{}'.format(coll, key)

    def _run_aql(self, aql):
        if DEBUG:
            print(aql)
        return self.db.AQLQuery(aql, batchSize=self.MAX_BATCH_SIZE)


def main():
    m = MetaGraph()
    m.truncate()
    #
    mv0 = m.add_node(nid='m0')

    mv1 = m.add_node(name='MV1', nid='m1')
    mv2 = m.add_node(name='MV2', nid='m2')
    mv3 = m.add_node(name='MV3', nid='m3')
    e12 = m.add_edge(from_node=mv1, to_node=mv2, eid='e12')

    m.add_to_metanode(mv1, mv0)
    m.add_to_metanode(mv2, mv0)
    m.add_to_metanode(mv3, mv0)
    m.add_to_metanode(e12, mv0)



    # for x in range(10):
    #     node = m.add_node(nid=str(x))
    #     m.add_to_metanode(node, mv1)

    m.remove_node(mv0, remove_submeta=False)
    #
    # print(len(m.get_submeta_nodes('m1')))

    # m.add_to_metanode(mv4, mv3)
    # m.add_to_metanode(mv3, mv2)
    # m.add_to_metanode(mv2, mv1)
    # m.add_to_metanode(e32, mv1)
    # m.add_to_metanode(e34, mv3)
    #
    # for node in m.get_submeta_nodes(mv1):
    #     print(node['name'])

    # n1 = m.add_node(nid='v1', name='vertex1')
    # n2 = m.add_node(nid='v2', name='vertex2')
    # mv1 = m.add_node(nid='mv1', name='metavertex1')
    # #
    # e12 = m.add_edge(n1, n2, eid='e12', name='edge12')
    # #
    # m.add_to_metanode(n1, mv1)
    #
    # m.add_to_metanode(n2, mv1)
    # m.add_to_metanode(e12, mv1)

    # n1 = m.add_node(name='V1', nid='1', some_key='hahaha', some_key_2=123)

    # m.filter_nodes(some_key='hahaha', some_key_2=123)
    # m.filter_nodes(some_key_2=123)

    # m.update_edge('e12', xxx='yyy')
    # m.update_node(n1, zzz='yyyzzz')

    # e12 = m.add_node(name='V2', nid='e12')


    # mv1 = m.add_node(name='MV1', nid='m1')

    # m.add_to_metanode(n1, mv1)

    # m.remove_from_metanode(n1, mv1)

    # n1 = m.add_node(name='V1', nid='1')
    # n2 = m.add_node(name='V2', nid='2')
    # n3 = m.add_node(name='V3', nid='3')
    # e12 = m.add_edge(from_node=n1, to_node=n2)
    # e13 = m.add_edge(from_node=n1, to_node=n3)
    # without edgenode:
    #  m.add_edge_to_metanode(n1, n2, mv1)
    #  m.add_edge_to_metanode(n1, n2, mv1)

    # m.remove_node(n3)

    # mv1 = m.add_node(name='MV1', nid='m1')
    # mv2 = m.add_node(name='MV2', nid='m2')
    # emv1mv2 = m.add_edge(from_node=mv1, to_node=mv2)

    # m.add_to_metanode(n2, mv1)
    # m.add_to_metanode(n1, mv1)
    # m.add_to_metanode(e12, mv1)

    # mv1 = m.add_node(name='MV1', nid='m1')
    # mv2 = m.add_node(name='MV2', nid='m2')
    # mv3 = m.add_node(name='MV3', nid='m3')
    #
    # m.add_to_metanode(mv3, mv2)
    # m.add_to_metanode(mv2, mv1)

    # m.remove_node(mv1)
    # m.remove_node(mv1, remove_submeta=True, recursive=False)

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
