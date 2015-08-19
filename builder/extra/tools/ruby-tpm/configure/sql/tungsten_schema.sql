/*!40101 SET character_set_client = utf8 */;

CREATE TABLE `consistency` (
  `db` char(64) NOT NULL DEFAULT '',
  `tbl` char(64) NOT NULL DEFAULT '',
  `id` int(11) NOT NULL DEFAULT '0',
  `row_offset` int(11) NOT NULL,
  `row_limit` int(11) NOT NULL,
  `this_crc` char(40) DEFAULT NULL,
  `this_cnt` int(11) DEFAULT NULL,
  `master_crc` char(40) DEFAULT NULL,
  `master_cnt` int(11) DEFAULT NULL,
  `ts` timestamp NULL DEFAULT NULL,
  `method` char(32) DEFAULT NULL,
  PRIMARY KEY (`db`,`tbl`,`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `heartbeat` (
  `id` bigint(20) NOT NULL DEFAULT '0',
  `seqno` bigint(20) DEFAULT NULL,
  `eventid` varchar(128) DEFAULT NULL,
  `source_tstamp` timestamp NULL DEFAULT NULL,
  `target_tstamp` timestamp NULL DEFAULT NULL,
  `lag_millis` bigint(20) DEFAULT NULL,
  `salt` bigint(20) DEFAULT NULL,
  `name` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `trep_commit_seqno` (
  `task_id` int(11) NOT NULL DEFAULT '0',
  `seqno` bigint(20) DEFAULT NULL,
  `fragno` smallint(6) DEFAULT NULL,
  `last_frag` char(1) DEFAULT NULL,
  `source_id` varchar(128) DEFAULT NULL,
  `epoch_number` bigint(20) DEFAULT NULL,
  `eventid` varchar(128) DEFAULT NULL,
  `applied_latency` int(11) DEFAULT NULL,
  `update_timestamp` timestamp NULL DEFAULT NULL,
  `shard_id` varchar(128) DEFAULT NULL,
  `extract_timestamp` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `trep_shard` (
  `shard_id` varchar(128) NOT NULL DEFAULT '',
  `master` varchar(128) DEFAULT NULL,
  `critical` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`shard_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `trep_shard_channel` (
  `shard_id` varchar(128) NOT NULL DEFAULT '',
  `channel` int(11) DEFAULT NULL,
  PRIMARY KEY (`shard_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;