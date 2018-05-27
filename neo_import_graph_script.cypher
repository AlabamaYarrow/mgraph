// https://neo4j.com/blog/bulk-data-import-neo4j-3-0/

CREATE INDEX ON :Node(_id);
CREATE INDEX ON :EdgeNode(_id);
CREATE INDEX ON :EdgeNode(from);
CREATE INDEX ON :EdgeNode(to);
