#!/bin/sh
BASEDIR=$(dirname "$0")
arangoimp --file $BASEDIR/arango_dumps/reldoc/nodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph-reldoc --server.password 1234
arangoimp --file $BASEDIR/arango_dumps/reldoc/edgenodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph-reldoc --server.password 1234
arangoimp --file $BASEDIR/arango_dumps/reldoc/meta_relations.csv --type csv --separator ';' --collection Submetas --server.database mgraph-reldoc --server.password 1234