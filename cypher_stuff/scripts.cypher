# Neo4j shell disabled by default
# Enable a remote shell server which Neo4j Shell clients can log in to.
dbms.shell.enabled=true

# Рестарт (neo4j stop не работал)
 sudo pkill -f "neo4j" && sudo neo4j start


# For import:
$ ln -s ~ /var/lib/neo4j/import/home

# Run file

cat /home/ivan/workspace/metagraph_neo4j/cypher_stuff/init.cypher | cypher-shell -u neo4j -p 1234


# Neo4j location
/var/lib/neo4

# Flush db


# CSV
	Финальная рабочая версия импорта:
		Важно: Эта штука работает только для инициализации базы. Перед импортом надо вырубить сервер (либо рестарнуть после импорта?).
			Перед импортом надо выпилить базу (можно сразу &&): 

			sudo rm -rf /var/lib/neo4j/data/databases/graph.db && sudo neo4j-admin import --database graph.db --delimiter=';' --array-delimiter=',' --id-type INTEGER --nodes:Node /home/ivan/workspace/metagraph_neo4j/data/t100k/town_with_types.neo.csv

			Загружавшийся csv:
				nid:ID;name;type:int
				1;town1;1
				2;town2;2
				3;town3;3




50 000 nodes, 20 from each

# 10 ms
MATCH p=(t1:Town {nid: 1})-[*1]->(:Town) RETURN p;

# 30 ms
MATCH p=(t1:Town {nid: 1})-[*2]->(:Town) RETURN p;

# 100 ms, 8000 nodes
MATCH p=(t1:Town {nid: 1})-[*3]->(:Town) RETURN p;

# 5 sec, 200000 nodes
MATCH p=(t1:Town {nid: 1})-[*4]->(:Town) RETURN p;

# shit, ages
MATCH p=(t1:Town {nid: 1})-[*5]->(:Town) RETURN p;




# count children at level
MATCH (t1:Town {nid: 1})-[*4]->(t4:Town) RETURN count(t4);



# Query path:
match p=(t1:Town {nid: 1})-[r:access_to*]-(tn:Town {nid: 999})  return p;


MATCH p=(t1:Town {nid: 1})-[*..]->(:Town {nid: 4999}) RETURN p;

# specify length?

neo4j> match (t1:Town {nid: 1})-[r:access_to*19]->(tn:Town {nid: 20})  return tn;



LOAD CSV FROM 'file:///home/workspace/metagraph_neo4j/cropped_town_connections.csv' AS line FIELDTERMINATOR ';' 
MATCH (t1:Town { nid: toInteger(line[0])}), (t2:Town { nid: toInteger(line[1])}) WITH t1, t2
RETURN t1, t2;







MATCH (t:Town {nid: 1}) WITH t
MATCH (t1:Town {nid: t.nid + 1}) WITH t,t1
RETURN t
