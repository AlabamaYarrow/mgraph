#!/bin/sh
echo "Killing neo..."
sudo neo4j stop
echo "Killed"

echo "Truncating database..."
sudo rm -rf /var/lib/neo4j/data/databases/graph.db
echo "Truncated"

echo "Starting neo..."
sudo neo4j start
echo "Neo started"