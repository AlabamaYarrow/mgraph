// https://neo4j.com/blog/bulk-data-import-neo4j-3-0/

// nodes
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 'file:///neo_dumps/graph/nodes.csv' AS node_row FIELDTERMINATOR ';'
CREATE (node:Node {
_id:node_row._id,
str_attr:node_row.str_attr,
int_attr:node_row.int_attr
}) WITH node, node_row
MATCH (parent:Node {_id:node_row.parent_meta_id})
CREATE (node)-[:submeta]->(parent);

// edgenodes
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 'file:///neo_dumps/graph/edgenodes.csv' AS node_row FIELDTERMINATOR ';'
CREATE (edgenode:EdgeNode {
_id:node_row._id,
from:node_row.from,
to:node_row.to
}) WITH edgenode, node_row
MATCH   (to_node:Node {_id:node_row.to}),
    (from_node:Node {_id:node_row.from})
CREATE  (edgenode)-[:x]->(to_node),
    (from_node)-[:x]->(edgenode);
