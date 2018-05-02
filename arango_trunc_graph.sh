#!/bin/sh
BASEDIR=$(dirname "$0")
arangosh --server.password 1234 --javascript.execute $BASEDIR/arango_scripts/truncate_graph.js
