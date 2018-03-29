db._useDatabase("mgraph");

db.Nodes.truncate();
db.NodesConnections.truncate();

tr = function() {
	db.Nodes.truncate();
	db.NodesConnections.truncate();
}

db._query(`
	for c in Nodes 
	return c
`);


db._query(`
	let data = [
	{_key: "1", "name": "Berlin"}, 
	{_key: "2", "name": "London"}
	]
	for d in data insert d into Nodes
	
`);

db._query(`
	insert { _from: "Nodes/1", _to: "Nodes/2" } into NodesConnections	
`);






// for c in Nodes return c
// for c in NodesConnections return c