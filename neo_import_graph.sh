#!/bin/sh
BASEDIR=$(dirname "$0")


cat $BASEDIR/neo_import_graph_script.cypher | cypher-shell -u neo4j -p 1234