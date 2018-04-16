/*

Модель метаграфа в аранго
// node 1
{
	name: "meta_parent_1",

}


*/



db._useDatabase("mgraph");

db.Nodes.truncate();
db.NodesConnections.truncate();

tr = function() {
	db.Nodes.truncate();
	db.NodesConnections.truncate();
}

db._query(`
	for c in Nodes return c
`);

db._query(`
	for c in NodesConnections return c
`);


// NODES
db._query(`
	let data = [
	{_key: "1", name: "Berlin"}, 
	{_key: "2", name: "London"},
	{_key: "3", name: "DC Columbia", sub_cities: [{name: "Washington"}]}
	]
	for d in data insert d into Nodes
	
`);

// CONNECTIONS
db._query(`
	insert { _from: "Nodes/1", _to: "Nodes/2" } into NodesConnections	
`);


db._query(`
	FOR e, v, p IN ANY 'NodesConnections'
`);



// Create named graph
var graph_module = require("org/arangodb/general-graph");
var edgeDefinitions = [{
	collection: "NodesConnections", from: [ "Nodes" ], to : [ "Nodes" ]
}];
graph = graph_module._create("NodesGraph", edgeDefinitions);

// Simple query over named graph
db._query(`
	FOR v IN 1..1 OUTBOUND "Nodes/1" GRAPH "NodesGraph"
	RETURN v.name
`);

// what for? graph._addVertexCollection('Nodes');


// for c in Nodes return c
// for c in NodesConnections return c


// TOWNS CSV TEST
// arangoimp --file data/t100k/town_with_types.arango.csv --separator ';' --type csv --collection towns --create-collection true
arangoimp --file data/t1M/town_with_types.arango.csv --separator ';' --type csv --collection towns --create-collection true


db._useDatabase("_system");
db.towns.ensureIndex({ type: "skiplist", fields: [ "type" ], sparse: false }); 
db.towns.ensureIndex({ type: "skiplist", fields: [ "name" ], sparse: false }); 
db.towns.ensureIndex({ type: "skiplist", fields: [ "nid" ], sparse: false }); 

db.towns.truncate();



// Метаграфы через граф
// Базовый конфиг - две связанные вершины внутри метавершины.
  // Вершины
  db._query(`
  	let data = [
	{
		_key: "1",
		name: "v1",
		type: "vertex"
	},
	{
		_key: "2",
		name: "v2",
		type: "vertex"
	},
	{
		_key: "3",
		name: "mv1",
		type: "metavertex"
	},
	{
		_key: "4",
		name: "e1",
		type: "edge"
	}]
	for d in data insert d into Nodes 
 `);

  // Связи
    db._query(`
  	let data = [
	{
		"_from":"Nodes/1",
		"_to":"Nodes/4"
	},
	{
		"_from":"Nodes/4",
		"_to":"Nodes/2"
	},
	{
		"_from":"Nodes/4",
		"_to":"Nodes/3"
	},
	{
		"_from":"Nodes/2",
		"_to":"Nodes/3"
	},
	{
		"_from":"Nodes/1",
		"_to":"Nodes/3"
	}
	]
	for d in data insert d into NodesConnections
  `);

















