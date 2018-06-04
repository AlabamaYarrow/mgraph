// Init graph db
db._createDatabase("mgraph");
db._useDatabase("mgraph");

db._create('Nodes');
db._createEdgeCollection('NodesConnections');

var graph_module = require("org/arangodb/general-graph");
var edgeDefinitions = [{
    collection: "NodesConnections", from: [ "Nodes" ], to : [ "Nodes" ]
}];
graph = graph_module._create("NodesGraph", edgeDefinitions);

db.Nodes.ensureIndex({ type: "hash", fields: [ "from" ], sparse: true });
db.Nodes.ensureIndex({ type: "hash", fields: [ "to" ], sparse: true });


// Init doc db
db._createDatabase("mgraph-doc");
db._useDatabase("mgraph-doc");

db._create('Nodes');

db.Nodes.ensureIndex({ type: "hash", fields: [ "from" ], sparse: true });
db.Nodes.ensureIndex({ type: "hash", fields: [ "to" ], sparse: true });


// Init doc-rel db
db._createDatabase("mgraph-reldoc");
db._useDatabase("mgraph-reldoc");

db._create('Nodes');
db._create('Submetas');

db.Nodes.ensureIndex({ type: "hash", fields: [ "from" ], sparse: true });
db.Nodes.ensureIndex({ type: "hash", fields: [ "to" ], sparse: true });
db.Submetas.ensureIndex({ type: "hash", fields: [ "parent" ], sparse: true });
db.Submetas.ensureIndex({ type: "hash", fields: [ "child" ], sparse: true });
