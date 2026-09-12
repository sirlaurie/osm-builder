PRAGMA foreign_keys=OFF;
PRAGMA user_version=1;
BEGIN TRANSACTION;
CREATE TABLE objects (
            kind TEXT NOT NULL, id INTEGER NOT NULL, tags TEXT NOT NULL,
            lat REAL, lon REAL, poi INTEGER NOT NULL,
            PRIMARY KEY (kind, id)
        ) WITHOUT ROWID;
INSERT INTO objects VALUES('node',1,'{"amenity":"cafe","name":"Cafe"}',-33.9,151.1,1);
CREATE TABLE refs (
            kind TEXT NOT NULL, id INTEGER NOT NULL, position INTEGER NOT NULL,
            target_kind TEXT NOT NULL, target_id INTEGER NOT NULL,
            PRIMARY KEY (kind, id, position)
        ) WITHOUT ROWID;
CREATE TABLE bounds (
            kind TEXT NOT NULL, id INTEGER NOT NULL,
            west REAL NOT NULL, south REAL NOT NULL, east REAL NOT NULL, north REAL NOT NULL,
            PRIMARY KEY (kind, id)
        ) WITHOUT ROWID;
CREATE TABLE pois (cell TEXT NOT NULL, id TEXT NOT NULL, payload BLOB NOT NULL,
            PRIMARY KEY (cell, id)) WITHOUT ROWID;
INSERT INTO pois VALUES('5610_33110','osm_node_1',x'7b226964223a226f736d5f6e6f64655f31222c226c6174223a2d33332e392c226c6f6e223a3135312e312c2274616773223a7b22616d656e697479223a2263616665222c226e616d65223a2243616665227d7d');
CREATE TABLE incomplete_relations (id INTEGER PRIMARY KEY);
CREATE TABLE validated_geometry (
            kind TEXT NOT NULL, id INTEGER NOT NULL, PRIMARY KEY (kind, id)
        ) WITHOUT ROWID;
CREATE TABLE control (
                id INTEGER PRIMARY KEY CHECK (id = 1), metadata TEXT NOT NULL,
                replication_url TEXT NOT NULL, receipt TEXT NOT NULL
            );
INSERT INTO control VALUES(1,'{"coverage":{"coordinates":[[[150,-35],[152,-35],[152,-32],[150,-32],[150,-35]]],"type":"Polygon"},"region":"au-nsw","sourceSHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sourceSequence":10,"sourceTimestamp":"2020-01-01T00:00:00Z"}','https://download.geofabrik.de/australia-oceania/australia/new-south-wales-updates','{"count":1,"excludedIncompleteRelationCount":0,"manifest":"4e58add341337a85f35b95578905f854572b5095f5006dd1392820873315baf2","region":"au-nsw","sourceTimestamp":"2020-01-01T00:00:00Z"}');
CREATE TABLE cell_blocks (cell TEXT PRIMARY KEY, hashes TEXT NOT NULL) WITHOUT ROWID;
INSERT INTO cell_blocks VALUES('5610_33110','["edae935dff0357a5d0368efb9a1fb1d856cd04809c07733bbeb5ba0722e3d5b6"]');
CREATE TABLE exclusions (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE missing_members (
                relation_id INTEGER NOT NULL, owner INTEGER NOT NULL,
                kind TEXT NOT NULL, target_id INTEGER NOT NULL,
                PRIMARY KEY (relation_id, owner, kind, target_id)
            ) WITHOUT ROWID;
CREATE INDEX refs_target ON refs(target_kind, target_id, kind, id);
CREATE UNIQUE INDEX pois_id ON pois(id);
COMMIT;
