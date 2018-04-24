import logging

from neo4j.v1 import GraphDatabase
from app.settings import USERNAME, PASSWD, URI


logger = logging.getLogger(__name__)


class Connection:
    _connection = None

    def __init__(self):
        self.driver = self._connection.driver

    def __new__(cls, *args, **kwargs):
        if Connection._connection is None:
            Connection._connection = super().__new__(cls)
            logger.info("Opening connection")
            Connection._connection.driver = GraphDatabase.driver(
                URI, auth=(USERNAME, PASSWD)
            )

        return Connection._connection

    def __del__(self):
        logger.info("Closing connection")
        self.driver.close()
