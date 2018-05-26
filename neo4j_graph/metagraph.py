import logging
from textwrap import dedent as dt

from neo4j.v1 import Record

from neo4j_graph.connection import Connection
from neo4j_graph.utils import get_data_str

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


# TODO index _id !!!
# TODO index edgenode from and to


class MetaGraph:
    meta_label = 'meta_sub'

    def truncate(self):
        def _truncate(tx):
            return tx.run('MATCH (n) DETACH DELETE n')

        return self._read(_truncate)

    def match(self, query=''):
        def _match(tx):
            return tx.run("MATCH (n {query}) RETURN n".format(query=query))

        return self._read(_match)

    def write(self, query):
        def _write(tx):
            return tx.run(query)

        return self._write(_write)

    def read(self, query):
        def _read(tx):
            return tx.run(query)

        return self._read(_read)

    def add_node(self, nid, label='Node', **kwargs):
        data = {}
        for k, v in kwargs.items():
            data[k] = v
        data['_id'] = nid

        statement = 'CREATE (n:{label} {data}) RETURN n'.format(
            label=label, data=get_data_str(data)
        )

        def _create_node(tx):
            return tx.run(statement)

        return self._write(_create_node).single()[0]

    def add_edge(self, eid, from_node, to_node, label='EdgeNode', **kwargs):
        data = {}
        for k, v in kwargs.items():
            data[k] = v
        data['_id'] = eid
        data['from'] = self._to_id(from_node)
        data['to'] = self._to_id(to_node)

        statement = 'CREATE (n:{label} {data}) RETURN n'.format(
            label=label, data=get_data_str(data)
        )

        def _create_node(tx):
            return tx.run(statement)

        edge_node = self._write(_create_node).single()[0]

        self._add_edge(from_node, edge_node)
        self._add_edge(edge_node, to_node)

        return edge_node

    def add_to_metanode(self, node, meta_node):
        node_id = self._to_id(node)
        meta_id = self._to_id(meta_node)
        self._add_edge(node_id, meta_id, self.meta_label)

    def filter_nodes(self, **kwargs):
        query = get_data_str(kwargs)
        return self.match(query)

    def update_node(self, node, **kwargs):
        node_id = self._to_id(node)
        if len(kwargs) > 1:
            raise ValueError('Only one field per update possible')
        field, value = list(kwargs.items())[0]
        query = """
            MATCH (n {{ _id: '{node_id}' }})
            SET n.{field} = {value}
            RETURN n
        """.format(
            node_id=node_id,
            field=field,
            value=value
        )

        self.write(query)

    def update_edge(self, edge, **kwargs):
        self.update_node(edge, **kwargs)

    def get_submeta_nodes(self, node):
        node_id = self._to_id(node)
        query = """ 
            MATCH (n {{ _id: '{node_id}' }})<-[:{meta_label}*..]-(subnode)
            RETURN subnode
        """.format(
            node_id=node_id,
            meta_label=self.meta_label
        )
        logger.info(query)
        return self.read(query)

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
            node_id = self._to_id(removed_node)

            # Removing edgenodes
            # (we have to remove them explicitly, because edgenode cannot exist
            # without one of node it is connecting, but edgenode may not be part
            # of removed metanode)
            query = '''
                MATCH (edgenode) 
                WHERE edgenode.from = '{node_id}' OR edgenode.to = '{node_id}'
                DETACH DELETE edgenode
            '''.format(node_id=node_id)
            logger.info(query)
            self.write(query)

            # Removing node itself
            query = '''
                MATCH (node)
                WHERE node._id = '{node_id}'
                DETACH DELETE node
            '''.format(node_id=node_id)
            logger.info(query)
            self.write(query)


    def _read(self, tx_method):
        conn = Connection()
        with conn.driver.session() as s:
            return s.read_transaction(tx_method)

    def _write(self, tx_method, *args):
        conn = Connection()
        with conn.driver.session() as s:
            return s.write_transaction(tx_method, *args)

    def _add_edge(self, from_node, to_node, edge_label='x'):
        from_id = self._to_id(from_node)
        to_id = self._to_id(to_node)
        statement = dt("""
            MATCH (n_fr) WITH n_fr WHERE n_fr._id = '{from_id}'
            MATCH (n_to) WITH n_fr, n_to WHERE n_to._id = '{to_id}'
            CREATE (n_fr)-[ r:{label} ]->(n_to) RETURN r
            """).format(
            from_id=from_id,
            to_id=to_id,
            label=edge_label
        )

        logger.info(statement)

        def _connect_nodes(tx):
            return tx.run(statement)

        return self._write(_connect_nodes).single()[0]

    @staticmethod
    def _to_id(node):
        if type(node) is str:
            return node
        elif isinstance(node, Record):
            return node.values()[0].properties.get('_id')
        else:
            return node.properties.get('_id')


def main():
    m = MetaGraph()
    m.truncate()
    # mv1 = m.add_node(name='MV1', nid='m1')
    # mv2 = m.add_node(name='MV2', nid='m2')
    # mv3 = m.add_node(name='MV3', nid='m3')
    # e12 = m.add_edge(from_node=mv1, to_node=mv3, eid='e12')
    #
    # m.add_to_metanode(mv2, mv1)
    # m.add_to_metanode(mv3, mv2)
    #
    # m.filter_nodes(_id='m1').single()
    #
    # m.update_node(mv1, new_field='123')
    #
    # print(m.get_submeta_nodes(mv1))

    # mv1 = m.add_node(name='MV1', nid='m1')
    # mv2 = m.add_node(name='MV2', nid='m2')
    # e12 = m.add_edge(from_node=mv1, to_node=mv2, eid='e12')
    # mv3 = m.add_node(name='MV3', nid='m3')
    #
    # m.add_to_metanode(mv1, mv2)
    # m.add_to_metanode(mv2, mv3)
    #
    # subs = m.get_submeta_nodes(mv3)
    #
    # print(m.get_submeta_nodes(mv3))
    #
    # m.remove_node(mv3, remove_submeta=True)


if __name__ == '__main__':
    main()
