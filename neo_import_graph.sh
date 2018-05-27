#!/bin/sh
BASEDIR=$(dirname "$0")


cp -r $BASEDIR/neo_dumps /tmp

sudo neo4j stop
sudo rm -rf /var/lib/neo4j/data/databases/graph.db
sudo neo4j-admin import --database graph.db --delimiter=';' --array-delimiter=',' --nodes /tmp/neo_dumps/graph/nodes.csv  --relationships /tmp/neo_dumps/graph/edges.csv
sudo neo4j start
sleep 10s

cat $BASEDIR/neo_import_graph_script.cypher | cypher-shell -u neo4j -p 1234
# waiting for index generation
sleep 5s