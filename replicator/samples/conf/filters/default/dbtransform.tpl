# Database transform filter.  Transforms database names that match the 
# from_regex are transformed into the to_regex.  
replicator.filter.dbtransform=com.continuent.tungsten.replicator.filter.DatabaseTransformFilter
replicator.filter.dbtransform.transformTables=false
# The filter can transform up to 3 schema names
replicator.filter.dbtransform.from_regex1=foo
replicator.filter.dbtransform.to_regex1=bar
# replicator.filter.dbtransform.from_regex2=foobar
# replicator.filter.dbtransform.to_regex2=barfoo
# replicator.filter.dbtransform.from_regex3=baz
# replicator.filter.dbtransform.to_regex3=zab

