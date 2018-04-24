import logging
from textwrap import dedent as dt

from app.connection import Connection
from app.utils import get_data_str

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class Metagraph:
    direct_edge = '->'
    indirect_edge = '-'
    meta_label = 'meta_sub'

    @classmethod
    def match(cls):
        def _match(tx):
            return tx.run("MATCH (n) RETURN n")

        return cls._read(_match)

    @classmethod
    def add_to_metanode(cls, node_id, meta_id):
        cls.connect_nodes(node_id, meta_id, cls.meta_label)

    @classmethod
    def connect_nodes(cls, from_id, to_id, edge_label='x'):
        statement = dt("""
            MATCH (n_fr) WITH n_fr WHERE n_fr.nid = {from_id}
            MATCH (n_to) WITH n_fr, n_to WHERE n_to.nid = {to_id}
            CREATE (n_fr)-[r:{label}]->(n_to) RETURN r
            """).format(
                from_id=from_id,
                to_id=to_id,
                label=edge_label
        )

        logger.info(statement)

        def _connect_nodes(tx):
            tx.run(statement)

        cls._write(_connect_nodes)

    @classmethod
    def create_node(cls, nid, label='', data=None):
        data = {} if data is None else data
        data['nid'] = nid

        if label:
            label = ':{}'.format(label)

        statement = 'CREATE ({label} {data})'.format(
            label=label, data=get_data_str(data)
        )

        def _create_node(tx):
            tx.run(statement)

        cls._write(_create_node)

    @classmethod
    def flush_db(cls):
        def _flush_db(tx):
            tx.run('MATCH (n) DETACH DELETE n')
        cls._write(_flush_db)

    @classmethod
    def _read(cls, tx_method):
        conn = Connection()
        with conn.driver.session() as s:
            return s.read_transaction(tx_method)

    @classmethod
    def _write(cls, tx_method, *args):
        conn = Connection()
        with conn.driver.session() as s:
            return s.write_transaction(tx_method, *args)
