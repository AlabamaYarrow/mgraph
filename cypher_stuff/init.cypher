// Init graph database;
// cat /home/ivan/workspace/metagraph_neo4j/cypher_stuff/init.cypher | cypher-shell -u neo4j -p 1234


sed -n 10,50p t1M/town.csv > t1M/town3.csv




sed -n 1,500000p t10M/town_with_types.csv > t10M/town_with_types1.csv


start=1
stop=500000
step=500000
for i in `seq 1 20`; do
	params="${start},${stop}p"
	export_file="t10M/town_with_types${i}.csv"
	sed -n $params t10M/town_with_types.csv > $export_file
	start=$(($start+step))
	stop=$(($start+step))
done

// TODO должны быть более оптимальные пути

	!!!for initial graph population only
	
	neo/bin/neo4j-import AKA neo4j-admin import
		https://neo4j.com/blog/import-10m-stack-overflow-questions/

	neo4j-admin import --database graph.db --nodes:Town /home/ivan/workspace/metagraph_neo4j/data/t100k/town.csv

	neo4j-admin import --database graph.db --nodes /home/ivan/workspace/metagraph_neo4j/data/t100k/town.csv

	Финальная рабочая версия импорта:
		Важно: Эта штука работает только для инициализации базы. Перед импортом надо вырубить сервер (либо рестарнуть после импорта?).
			Перед импортом надо выпилить базу (можно сразу &&): 

			sudo rm -rf /var/lib/neo4j/data/databases/graph.db && sudo neo4j-admin import --database graph.db --delimiter=';' --array-delimiter=',' --nodes:Town /home/ivan/workspace/metagraph_neo4j/data/t100k/town_with_types.neo.csv

			Загружавшийся csv:
				nid:ID;name;type:int
				1;town1;1
				2;town2;2
				3;town3;3




MATCH (n) DETACH DELETE n;

LOAD CSV FROM 'file:///home/workspace/metagraph_neo4j/town.csv' AS line FIELDTERMINATOR ';' CREATE (:Town { nid: toInteger(line[0]), name: (line[1])});



LOAD CSV FROM 'file:///home/workspace/metagraph_neo4j/sql_town_connections.csv' AS line FIELDTERMINATOR ';' 
MATCH (t1:Town { nid: toInteger(line[0])}), (t2:Town { nid: toInteger(line[1])}) WITH t1, t2
CREATE (t1)-[:access_to]->(t2);




LOAD CSV FROM 'file:///home/workspace/metagraph_neo4j/cropped_town_connections.csv' AS line FIELDTERMINATOR ';' 
MATCH (t1:Town { nid: toInteger(line[0])}), (t2:Town { nid: toInteger(line[1])}) WITH t1, t2
CREATE (t1)-[:access_to]->(t2);


LOAD CSV FROM 'file:///home/workspace/metagraph_neo4j/data/t1M/town_with_types1.csv' AS line FIELDTERMINATOR ';' CREATE (:Town { nid: toInteger(line[0]), name: (line[1]), type: toInteger(line[2])});

LOAD CSV FROM 'file:///home/workspace/metagraph_neo4j/town_with_types.csv' AS line FIELDTERMINATOR ';' CREATE (:Town { nid: toInteger(line[0]), name: (line[1]), type: toInteger(line[2])});

LOAD CSV FROM 'file:///home/workspace/metagraph_neo4j/data/t10M/town_with_types1.csv' AS line FIELDTERMINATOR ';' CREATE (:Town { nid: toInteger(line[0]), name: (line[1]), type: toInteger(line[2])});






RETURN { from_nid: toInteger(line[0]), to_nid: toInteger(line[1])};

CREATE (:Town { nid: toInteger(line[0]), name: (line[1])});



CREATE INDEX ON :Town(nid);
// CREATE INDEX ON :Town(name);
// DROP INDEX ON :Town(nid);

// MATCH (t:Town) WITH t
// MATCH (t1:Town {nid: t.nid + 1}) WITH t,t1
// CREATE (t) -[:access_to]-> (t1);