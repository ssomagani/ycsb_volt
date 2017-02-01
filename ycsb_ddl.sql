file -inlinebatch drops

drop procedure Get if exists;
drop procedure Put if exists;
drop procedure Scan if exists;
drop table Store if exists;
 
drops



CREATE TABLE Store
(
    keyspace VARCHAR(128)  NOT NULL
,   key      VARCHAR(128)    NOT NULL
,   value    VARBINARY(1024) NOT NULL
,   PRIMARY KEY (keyspace, key)
);
PARTITION TABLE Store ON COLUMN key;

-- Update classes from jar so that the server will know about classes, but not procedures yet.
LOAD CLASSES ycsb-procs.jar;

-- Batch load CREATE PROCEDURE statements
file -inlinebatch END_OF_BATCH

CREATE PROCEDURE PARTITION ON TABLE Store COLUMN key PARAMETER 1 FROM CLASS com.procedures.Put;

CREATE PROCEDURE PARTITION ON TABLE Store COLUMN key FROM CLASS com.procedures.Scan;

CREATE PROCEDURE Get PARTITION ON TABLE Store COLUMN key PARAMETER 1 AS
    SELECT value FROM Store WHERE keyspace = ? AND key = ?;

END_OF_BATCH
