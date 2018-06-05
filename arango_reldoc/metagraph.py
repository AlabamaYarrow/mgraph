import sys
from pyArango.connection import Connection
from arango_reldoc.settings import USERNAME, PASSWORD, DB_NAME, DEBUG


"""
NB: edgenodes collection requires 
    hash indexes on "to" and "from"
    (sparse indexes if it's same collection with nodes)
    
    E.g. (arangosh command):
        db.Nodes.ensureIndex({ type: "hash", fields: [ "from" ], sparse: true });
        db.Nodes.ensureIndex({ type: "hash", fields: [ "to" ], sparse: true });
        
        db.Submetas.ensureIndex({ type: "hash", fields: [ "parent" ], sparse: true }); 
        db.Submetas.ensureIndex({ type: "hash", fields: [ "child" ], sparse: true }); 
"""


# noinspection PyProtectedMember
class MetaGraph:
    """
    Arango metagraph API for document storage in relational style.
    """

    # Arango collections.
    NODES_COLL = 'Nodes'  # normal nodes and metanodes
    EDGENODE_COLL = 'Nodes'  # edges
    METAREL_COLL = 'Submetas'  # parent-child relations

    MAX_BATCH_SIZE = 20000

    def __init__(self):
        conn = Connection(username=USERNAME, password=PASSWORD)
        self.db = conn[DB_NAME]

        self.nodes = self.db[self.NODES_COLL]
        self.edges_nodes = self.db[self.EDGENODE_COLL]
        self.meta_relations = self.db[self.METAREL_COLL]

    def truncate(self):
        self.nodes.truncate()
        self.meta_relations.truncate()
        self.edges_nodes.truncate()

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
        node = self.nodes.createDocument()
        node._key = kwargs['nid']
        del kwargs['nid']
        for k, v in kwargs.items():
            node[k] = v
        node.save()
        return node

    def add_edge(self, from_node, to_node, **kwargs):
        edge_node = self.edges_nodes.createDocument()
        edge_node._key = kwargs['eid']
        del kwargs['eid']
        edge_node['from'] = self.key_to_id(self._to_key(from_node))
        edge_node['to'] = self.key_to_id(self._to_key(to_node))
        for k, v in kwargs.items():
            edge_node[k] = v
        edge_node.save()

        return edge_node

    def add_to_metanode(self, node, metanode):
        meta_relation = self.meta_relations.createDocument()
        meta_relation['parent'] = self.key_to_id(self._to_key(metanode))
        meta_relation['child'] = self.key_to_id(self._to_key(node))
        meta_relation.save()

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
        to_remove = [node]
        if remove_submeta:
            to_remove.extend(self.get_submeta_nodes(node))

        for submeta_node in to_remove:
            node_key = self._to_key(submeta_node)
            # Removing connected edgenodes
            aql = '''
                FOR e IN {edgenodes_collection} 
                FILTER e.from=='{node_id}' OR e.to=='{node_id}' 
                REMOVE e IN {edgenodes_collection} 
            '''.format(
                edgenodes_collection=self.EDGENODE_COLL,
                node_id=self.key_to_id(node_key),
            )
            self._run_aql(aql)

            # Removing connection to other metavertices
            aql = '''
                FOR e IN {meta_relations} 
                FILTER e.child=='{node_id}' 
                REMOVE e IN {meta_relations} 
            '''.format(
                meta_relations=self.METAREL_COLL,
                node_id=self.key_to_id(node_key),
            )
            self._run_aql(aql)

            # Removing node itself
            aql = '''
                FOR n IN {node_collection}
                FILTER n._key == '{node_key}'
                REMOVE n IN {node_collection}
            '''.format(
                node_key=node_key,
                node_collection=self.NODES_COLL
            )
            self._run_aql(aql)

    def remove_from_metanode(self, node, metanode):
        node_id = self.key_to_id(self._to_key(node))
        metanode_id = self.key_to_id(self._to_key(metanode))

        aql = '''
            FOR sub_connection IN {meta_relations}
            FILTER sub_connection.parent == '{metanode_id}' 
                AND sub_connection.child == '{node_id}'
                REMOVE sub_connection IN {meta_relations}
        '''.format(
            meta_relations=self.METAREL_COLL,
            node_id=node_id,
            metanode_id=metanode_id,
        )

        self._run_aql(aql)

    def get_submeta_nodes(self, node):
        def _get_submetas(_node):
            node_id = self.key_to_id(self._to_key(_node))
            aql = '''
                FOR sub_connection IN {meta_relations}
                FILTER sub_connection.parent == '{node_id}'
                    FOR node IN {nodes_collection}  

                    FILTER node._id == sub_connection.child
                        RETURN node
            '''.format(
                meta_relations=self.METAREL_COLL,
                node_id=node_id,
                nodes_collection=self.NODES_COLL
            )

            return self._run_aql(aql).response['result']

        submeta_nodes = []
        nodes_to_visit = []

        while True:
            node_submetas = _get_submetas(node)  # [_n['node'] for _n in _get_submetas(node)]
            submeta_nodes.extend(node_submetas)
            nodes_to_visit.extend(node_submetas)
            if not nodes_to_visit:
                break
            node = nodes_to_visit.pop()

        return submeta_nodes

    def _build_query_string(self, **kwargs):
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

    def _to_key(self, node):
        if type(node) is str:
            return node
        else:
            return node['_key']

    def key_to_id(self, key, coll=NODES_COLL):
        return '{}/{}'.format(coll, key)

    def _run_aql(self, aql):
        if DEBUG:
            print(aql)
        return self.db.AQLQuery(aql, batchSize=self.MAX_BATCH_SIZE)


def main():
    m = MetaGraph()
    m.truncate()

    mv1 = m.add_node(nid='mv1', name='metavertex1')
    mv2 = m.add_node(nid='mv2', name='metavertex2')
    mv3 = m.add_node(nid='mv3', name='metavertex3')
    mv4 = m.add_node(nid='mv4', name='metavertex3')
    mv5 = m.add_node(nid='mv5', name='metavertex3')
    mv6 = m.add_node(nid='mv6', name='metavertex3')
    mv7 = m.add_node(nid='mv7', name='metavertex3')
    e32 = m.add_edge(mv3, mv2, eid='e32', name='edge32')
    # e31 = m.add_edge(mv3, mv1, eid='e31', name='edge31')

    # m.add_to_metanode(mv2, mv1)
    # m.add_to_metanode(mv3, mv1)
    # m.add_to_metanode(mv4, mv1)
    # m.add_to_metanode(mv5, mv1)
    # m.add_to_metanode(mv6, mv1)
    # m.add_to_metanode(mv7, mv1)
    # m.add_to_metanode(e32, mv1)

    print(len(m.get_submeta_nodes(mv1)))
    # m.remove_from_metanode(mv2, mv1)
    # print(len(m.get_submeta_nodes(mv1)))

    # m.remove_node(mv1, remove_submeta=True)


    # mv1 = m.add_node(name='MV1', nid='m1')
    # for x in range(1000):
    #     node = m.add_node(nid=str(x))
    #     m.add_to_metanode(node, mv1)
    # import time
    # s = time.time()
    # print(len(m.get_submeta_nodes('m1')))
    # print(time.time() - s)

    # mv1 = m.add_node(nid='mv1', name='metavertex1')
    # mv2 = m.add_node(nid='mv2', name='metavertex2')
    # mv3 = m.add_node(nid='mv3', name='metavertex3')
    # e32 = m.add_edge(mv3, mv2, eid='e12', name='edge12')
    #
    # m.add_to_metanode(mv3, mv2)
    # m.add_to_metanode(mv2, mv1)
    # m.add_to_metanode(e32, mv1)
    #
    # print(m.get_submeta_nodes(mv1))
    # print(m.remove_node(mv1, remove_submeta=True))

    # n1 = m.add_node(nid='v1', name='vertex1')
    # n2 = m.add_node(nid='v2', name='vertex2')
    # mv1 = m.add_node(nid='mv1', name='metavertex1')
    #
    # e12 = m.add_edge(n1, n2, eid='e12', name='edge12')
    #
    # m.add_to_metanode(n1, mv1)
    #
    # m.add_to_metanode(n2, mv1)
    # m.add_to_metanode(e12, mv1)

    # m.remove_node(mv1)
    #
    # print(m.get_submeta_nodes(mv1))

    # m.filter_nodes(name='mv1')
    #
    # m.filter_nodes(name='mv1')
    #
    # m.filter_nodes(name='mv1')

    # m.update_edge(e12, name='new1234')
    # m.update_node(n1, name='new_v1')
    #
    # mv1 = m.add_node(nid='mv1', name='mv1')
    # mv2 = m.add_node(nid='mv2', name='mv2')
    #
    # m.add_to_metanode(mv1, mv2)
    #
    # m.remove_from_metanode(mv1, mv2)

    # m.remove_node(mv3, remove_submeta=True, recursive=True)


if __name__ == '__main__':
    main()
