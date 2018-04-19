import time
from pyArango.connection import Connection
from app_arango_doc.settings import USERNAME, PASSWORD, DB_NAME, DEBUG


def key_to_id(coll, key):
    return '{}/{}'.format(coll, key)

"""
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
    Arango metagraph API for document storage.
    """

    # Arango collections.
    NODES_COLL = 'Nodes'  # normal nodes and metanodes
    EDGENODE_COLL = 'Nodes'  # edges

    META_ATTR = '_submeta'  # meta child nodes
    SUPERMETA_ATTR = '_supermeta'  # meta parent nodes

    IN_ATTR = '_incoming'
    OUT_ATTR = '_outcoming'

    def __init__(self):
        conn = Connection(username=USERNAME, password=PASSWORD)
        self.db = conn[DB_NAME]

        self.nodes = self.db[self.NODES_COLL]
        self.edges_nodes = self.db[self.EDGENODE_COLL]

    def truncate(self):
        self.nodes.truncate()
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
        edge_node['from'] = key_to_id(self.NODES_COLL, self._to_key(from_node))
        edge_node['to'] = key_to_id(self.NODES_COLL, self._to_key(to_node))
        for k, v in kwargs.items():
            edge_node[k] = v
        edge_node.save()

        return edge_node

    def add_to_metanode(self, node, metanode):
        self._add_to_list(metanode, node, self.META_ATTR)
        self._add_to_list(node, metanode, self.SUPERMETA_ATTR)

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

    def remove_node(self, node, remove_submeta=False, recursive=False):
        """
        Remove node. Must remove node adjacent edges,
        edgenodes and edgenodes adjacent edges.
        :param remove_submeta: remove content of metanode
        :param recursive: remove content of submetanodes
        """
        if recursive and not remove_submeta:
            raise ValueError('Recursive removal only allowed when removing submeta')

        node_key = self._to_key(node)

        # Removing node key from supermeta nodes
        aql = '''
            FOR e IN Nodes FILTER e._key == '{node_key}'
                FOR supermeta_key in e._supermeta OR []
                    FOR supermeta_node IN {nodes_collection} 
                    FILTER supermeta_node._key == supermeta_key
                        UPDATE supermeta_node 
                        WITH {{ _submeta: REMOVE_VALUE(supermeta_node._submeta, e._key)}} 
                        IN {nodes_collection}
        '''.format(
            node_key=node_key,
            nodes_collection=self.NODES_COLL
        )
        self._run_aql(aql)

        if not remove_submeta:
            # If not removing submeta node, remove link from them
            aql = '''
                FOR e IN Nodes FILTER e._key == '{node_key}'
                    FOR submeta_key in e._submeta OR []
                        FOR submeta_node IN {nodes_collection} 
                        FILTER submeta_node._key == submeta_key
                            UPDATE submeta_node 
                            WITH {{ _supermeta: REMOVE_VALUE(submeta_node._supermeta, e._key)}} 
                            IN {nodes_collection}
            '''.format(
                node_key=node_key,
                nodes_collection=self.NODES_COLL
            )
            self._run_aql(aql)

        # Removing adjacent edges
        aql = '''
            FOR e IN {edgenodes_collection} 
            FILTER e.from=='{node_id}' OR e.to=='{node_id}' 
            REMOVE e IN {edgenodes_collection} 
        '''.format(
            edgenodes_collection=self.EDGENODE_COLL,
            node_id=key_to_id(self.NODES_COLL, node_key),
        )
        self._run_aql(aql)

        # Removing node content
        if remove_submeta:
            node_content = self.nodes[node_key][self.META_ATTR] or []

            for subnode in node_content:
                self.remove_node(subnode, remove_submeta=recursive, recursive=recursive)

        # Removing node
        # 'REMOVE key IN collection' raises exception if key not present. Is it faster?
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
        node_key = self._to_key(node)
        metanode_key = self._to_key(metanode)

        aql = '''
            FOR supermeta_node IN {nodes_collection} 
            FILTER supermeta_node._key == '{metanode_key}'
                UPDATE supermeta_node 
                WITH {{ _submeta: REMOVE_VALUE(supermeta_node._submeta, '{node_key}')}} 
                IN {nodes_collection}
        '''.format(
            node_key=node_key,
            metanode_key=metanode_key,
            nodes_collection=self.NODES_COLL
        )
        self._run_aql(aql)

        aql = '''
            FOR node IN {nodes_collection} 
            FILTER node._key == '{node_key}'
                UPDATE node 
                WITH {{ _supermeta: REMOVE_VALUE(node._supermeta, '{metanode_key}')}} 
                IN {nodes_collection}
        '''.format(
            node_key=node_key,
            metanode_key=metanode_key,
            nodes_collection=self.NODES_COLL
        )
        self._run_aql(aql)

    def get_submeta_nodes(self, node):
        node_key = self._to_key(node)
        aql = '''
            FOR e IN Nodes FILTER e._key == '{node_key}'
                FOR submeta_key in e._submeta OR []
                    FOR node IN {nodes_collection} 
                    FILTER node._key == submeta_key
                        RETURN node
        '''.format(
            node_key=node_key,
            nodes_collection=self.NODES_COLL
        )
        # TODO recursion?
        return self._run_aql(aql)

    def _add_to_list(self, node, added_node, list_name):
        aql = '''
            LET node_doc = DOCUMENT('{node_id}')
            UPDATE node_doc WITH {{ {list_name}: PUSH(node_doc.{list_name}, '{added_node_key}') }}
            IN {nodes_collection}
        '''.format(
            node_id=key_to_id(self.NODES_COLL, self._to_key(node)),
            list_name=list_name,
            added_node_key=self._to_key(added_node),
            nodes_collection=self.NODES_COLL
        )
        self._run_aql(aql)

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
            return node._key

    def _run_aql(self, aql):
        if DEBUG:
            print(aql)
        return self.db.AQLQuery(aql)


def main():
    m = MetaGraph()
    m.truncate()
    #
    n1 = m.add_node(nid='v1', name='vertex1')
    n2 = m.add_node(nid='v2', name='vertex2')
    mv1 = m.add_node(nid='mv1', name='metavertex1')
    #
    e12 = m.add_edge(n1, n2, eid='e12', name='edge12')
    #
    m.add_to_metanode(n1, mv1)

    m.add_to_metanode(n2, mv1)
    m.add_to_metanode(e12, mv1)

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
