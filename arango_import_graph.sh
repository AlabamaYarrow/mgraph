#!/bin/sh
BASEDIR=$(dirname "$0")
arangoimp --file $BASEDIR/arango_dumps/graph/nodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
arangoimp --file $BASEDIR/arango_dumps/graph/edgenodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
arangoimp --file $BASEDIR/arango_dumps/graph/graph_edges.csv --type csv --separator ';' --collection NodesConnections --server.database mgraph --server.password 1234