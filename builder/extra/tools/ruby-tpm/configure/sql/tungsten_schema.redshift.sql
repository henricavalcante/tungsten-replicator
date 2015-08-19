CREATE TABLE consistency
(
  db CHAR(64),
  tbl CHAR(64),
  id INT,
  row_offset INT,
  row_limit INT,
  this_crc CHAR(40),
  this_cnt INT,
  master_crc CHAR(40),
  master_cnt INT,
  ts TIMESTAMP,
  method CHAR(32),
  PRIMARY KEY (db, tbl, id)
);

CREATE TABLE heartbeat
(
  id BIGINT,
  seqno BIGINT,
  eventid VARCHAR(512) /* VARCHAR(128) */,
  source_tstamp TIMESTAMP,
  target_tstamp TIMESTAMP,
  lag_millis BIGINT,
  salt BIGINT,
  name VARCHAR(512) /* VARCHAR(128) */,
  PRIMARY KEY (id)
);

CREATE TABLE trep_commit_seqno
(
  task_id INT,
  seqno BIGINT,
  fragno SMALLINT,
  last_frag BOOLEAN,
  source_id VARCHAR(128) /* VARCHAR(128) */,
  epoch_number BIGINT,
  eventid VARCHAR(128) /* VARCHAR(128) */,
  applied_latency INT,
  update_timestamp TIMESTAMP,
  shard_id VARCHAR(128) /* VARCHAR(128) */,
  extract_timestamp TIMESTAMP,
  PRIMARY KEY (task_id)
);

CREATE TABLE trep_shard
(
  shard_id VARCHAR(512) /* VARCHAR(128) */,
  master VARCHAR(512) /* VARCHAR(128) */,
  critical SMALLINT /* TINYINT(4) */,
  PRIMARY KEY (shard_id)
);

CREATE TABLE trep_shard_channel
(
  shard_id VARCHAR(512) /* VARCHAR(128) */,
  channel INT,
  PRIMARY KEY (shard_id)
);