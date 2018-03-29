-- sudo -u postgres psql -d towns -f ~/workspace/metagraph_neo4j/init.sql

DROP TABLE IF EXISTS Town;
DROP TABLE IF EXISTS TownConnection;

CREATE TABLE IF NOT EXISTS Town(
  nid	 	INTEGER	NOT NULL,
--   next_nid INTEGER NULL,
  name 	VARCHAR	NULL,
  CONSTRAINT nid_key PRIMARY KEY(nid)
);

-- CREATE INDEX ON Town (name);
-- CREATE INDEX ON Town (type);


CREATE TABLE  IF NOT EXISTS TownConnection(
  from_nid INTEGER	NOT NULL,
  to_nid INTEGER	NOT NULL,
  CONSTRAINT from_to_key PRIMARY KEY(from_nid, to_nid)
);

	CREATE TABLE  IF NOT EXISTS TownConnection(
	  from_nid INTEGER	NOT NULL,
	  to_nid INTEGER	NOT NULL
	);


CREATE INDEX ON TownConnection (from_nid);
CREATE INDEX ON TownConnection (to_nid);


COPY TownConnection FROM '/home/ivan/workspace/metagraph_neo4j/sql_town_connections.csv' DELIMITER ';';
COPY Town FROM '/home/ivan/workspace/metagraph_neo4j/town.csv' DELIMITER ';';
COPY Town FROM '/home/ivan/workspace/metagraph_neo4j/town_with_types.csv' DELIMITER ';';



-- 2 ms
SELECT t1.nid, t2.nid, t3.nid, t4.nid 
FROM Town t1
JOIN TownConnection tc1 on tc1.from_nid = t1.nid 
JOIN Town t2 ON t2.nid = tc1.to_nid
WHERE t1.nid = 1;



-- 30 ms
SELECT t1.nid, t2.nid, t3.nid, t4.nid 
FROM Town t1
JOIN TownConnection tc1 on tc1.from_nid = t1.nid 
JOIN Town t2 ON t2.nid = tc1.to_nid
JOIN TownConnection tc2 on tc2.from_nid = t2.nid 
JOIN Town t3 ON t3.nid = tc2.to_nid
JOIN TownConnection tc3 on tc3.from_nid = t3.nid 
JOIN Town t4 ON t4.nid = tc3.to_nid
WHERE t1.nid = 1;

-- 200 ms
SELECT t1.nid, t2.nid, t3.nid, t4.nid, t5.nid
FROM Town t1
JOIN TownConnection tc1 on tc1.from_nid = t1.nid 
JOIN Town t2 ON t2.nid = tc1.to_nid
JOIN TownConnection tc2 on tc2.from_nid = t2.nid 
JOIN Town t3 ON t3.nid = tc2.to_nid
JOIN TownConnection tc3 on tc3.from_nid = t3.nid 
JOIN Town t4 ON t4.nid = tc3.to_nid
JOIN TownConnection tc4 on tc4.from_nid = t4.nid 
JOIN Town t5 ON t5.nid = tc4.to_nid
WHERE t1.nid = 1;

-- 2 sec
SELECT t1.nid, t2.nid, t3.nid, t4.nid, t5.nid, t6.nid
FROM Town t1
JOIN TownConnection tc1 on tc1.from_nid = t1.nid 
JOIN Town t2 ON t2.nid = tc1.to_nid
JOIN TownConnection tc2 on tc2.from_nid = t2.nid 
JOIN Town t3 ON t3.nid = tc2.to_nid
JOIN TownConnection tc3 on tc3.from_nid = t3.nid 
JOIN Town t4 ON t4.nid = tc3.to_nid
JOIN TownConnection tc4 on tc4.from_nid = t4.nid 
JOIN Town t5 ON t5.nid = tc4.to_nid
JOIN TownConnection tc5 on tc5.from_nid = t5.nid 
JOIN Town t6 ON t6.nid = tc5.to_nid
WHERE t1.nid = 1;





-- search arbitrary path

SELECT t1.nid, t2.nid, t3.nid, t4.nid 
FROM Town t1
JOIN TownConnection tc1 on tc1.from_nid = t1.nid 
JOIN Town t2 ON t2.nid = tc1.to_nid
JOIN TownConnection tc2 on tc2.from_nid = t2.nid 
JOIN Town t3 ON t3.nid = tc2.to_nid
JOIN TownConnection tc3 on tc3.from_nid = t3.nid 
JOIN Town t4 ON t4.nid = tc3.to_nid
WHERE t1.nid = 1 AND (t2.nid=4 OR t3.nid=4 OR t4.nid=4);


-- search arbitrary path
-- still 2 sec
SELECT t1.nid, t2.nid, t3.nid, t4.nid, t5.nid, t6.nid
FROM Town t1
JOIN TownConnection tc1 on tc1.from_nid = t1.nid 
JOIN Town t2 ON t2.nid = tc1.to_nid
JOIN TownConnection tc2 on tc2.from_nid = t2.nid 
JOIN Town t3 ON t3.nid = tc2.to_nid
JOIN TownConnection tc3 on tc3.from_nid = t3.nid 
JOIN Town t4 ON t4.nid = tc3.to_nid
JOIN TownConnection tc4 on tc4.from_nid = t4.nid 
JOIN Town t5 ON t5.nid = tc4.to_nid
JOIN TownConnection tc5 on tc5.from_nid = t5.nid 
JOIN Town t6 ON t6.nid = tc5.to_nid
WHERE t1.nid = 1 AND (t2.nid=4 OR t3.nid=4 OR t4.nid=4 OR t5.nid=4 OR t6.nid=4);





WITH RECURSIVE path AS (
   SELECT nid, next_nid FROM Town WHERE nid = 1
   UNION SELECT t.nid, t.next_nid
   FROM Town t INNER JOIN path p ON p.next_nid = t.nid
) SELECT * FROM path;



-- DO $$
-- BEGIN
--   FOR r IN 1..50000 LOOP
--     INSERT INTO Town(nid) VALUES(r);
--   END LOOP;
-- END;
-- $$;
--
-- DO $$
-- BEGIN
--   FOR r IN 1..100000 LOOP
--     INSERT INTO TownConnection(from_nid, to_nid)
--     VALUES(round( random() * 50000 ), round( random() * 50000 ));
--   END LOOP;
-- END;
-- $$;



-- DO $$
-- BEGIN
--   FOR r IN 1..50000 LOOP
--     INSERT INTO Town(nid, next_nid) VALUES(r, r+1);
--   END LOOP;
-- END;
-- $$;

-- DO $$
-- BEGIN
--   FOR r IN 1..1000000 LOOP
--     INSERT INTO Town(nid, next_nid)
--     VALUES(r, round( random() * 1000000 ));
--   END LOOP;
-- END;
-- $$;


-- UPDATE Town SET next_nid = NULL WHERE nid = 50000;

-- WITH RECURSIVE path AS (
--   SELECT nid, next_nid FROM Town WHERE nid = 1
--   UNION SELECT t.nid, t.next_nid
--   FROM Town t INNER JOIN path p ON p.next_nid = t.nid
-- ) SELECT * FROM path;

-- SELECT t1.nid, t2.nid, t3.nid, t4.nid FROM Town t1
-- JOIN Town t2 ON t1.next_nid = t2.nid
-- JOIN Town t3 ON t2.next_nid = t3.nid
-- JOIN Town t4 ON t3.next_nid = t4.nid
-- WHERE t1.nid = 1;

