#!/bin/sh
BASEDIR=$(dirname "$0")
arangoimp --file $BASEDIR/arango_dumps/doc/doc-nodes.json --collection Nodes --server.database mgraph-doc --server.password 1234
arangoimp --file $BASEDIR/arango_dumps/doc/doc-edgenodes.json --collection Nodes --server.database mgraph-doc --server.password 1234
