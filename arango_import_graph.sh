arangoimp --file nodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
arangoimp --file edgenodes.csv --type csv --separator ';' --collection Nodes --server.database mgraph --server.password 1234
arangoimp --file graph_edges.csv --type csv --separator ';' --collection NodesConnections --server.database mgraph --server.password 1234