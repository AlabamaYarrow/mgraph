LOAD CSV WITH HEADERS 'neo_dumps/graph/nodes.csv' CSV AS node_row
CREATE (n:Node {
nid:row.nid,
_id:row._id,
str_attr:row.str_attr,
int_attr:row.int_attr
})


