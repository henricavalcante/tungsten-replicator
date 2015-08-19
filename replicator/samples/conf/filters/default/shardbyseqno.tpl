# Shards transactions by seqno. Caution: use with order-independed transactions only! 
replicator.filter.shardbyseqno=com.continuent.tungsten.replicator.filter.JavaScriptFilter
replicator.filter.shardbyseqno.script=${replicator.home.dir}/support/filters-javascript/shardbyseqno.js
# How much shards to utilize (can be larger than channel count to utilize all channels).
replicator.filter.shardbyseqno.shards=1000
