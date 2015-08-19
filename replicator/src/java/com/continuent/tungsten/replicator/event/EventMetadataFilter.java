/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *      
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.event;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.MySQLCommentEditor;
import com.continuent.tungsten.replicator.database.SqlCommentEditor;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlOperationMatcher;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This filter events newly extracted from the database log to answer the
 * following key questions. It must run as an auto-filter whenever we want
 * sharding and multi-master replication to work.
 * <ul>
 * <li>Is this an ordinary transaction or a Tungsten catalog update?</li>
 * <li>What is the original service of this event?</li>
 * <li>What is the shard ID of this event?</li>
 * </ul>
 * These questions are answered as follows. We begin with the assumption that
 * any event is from the local service and does not contain Tungsten catalog
 * metadata updates. We then modify assumptions and assign the shard ID as
 * follows.
 * <ul>
 * <li>Case 0: No database identified. Sadly, this is possible. Warn and assign
 * to the default shard ID.</li>
 * <li>Case 1: Single ordinary database. Mark shard with database name.</li>
 * <li>Case 2: 1 or more dbs including a tungsten_<svc> database. Mark the
 * service, mark tungsten metadata, and assign the shard name using the
 * tungsten_<svc> database name.</li>
 * <li>Case 3: Multiple ordinary databases. Assign the shard to the default ID.</li>
 * </ul>
 * Finally, we should note that this filter needs to be fast and to minimize
 * memory usage. The indexing structure used in the implementation is a local
 * hash table of database schema names and reference counts, which takes up
 * virtually no space.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EventMetadataFilter implements Filter
{
    // Settable properties.
    private boolean             unknownSqlUsesDefaultDb = false;
    private static String       STRINGENT               = "stringent";
    private static String       RELAXED                 = "relaxed";

    // Class and instance variables.
    private static Logger       logger                  = Logger.getLogger(EventMetadataFilter.class);
    private PluginContext       context;
    private SqlOperationMatcher opMatcher;
    private SqlCommentEditor    commentEditor;
    private boolean             sqlCommentsEnabled      = true;
    private String              serviceCommentRegex     = "___SERVICE___ = \\[([a-zA-Z0-9-_]+)\\]";
    private Pattern             serviceCommentPattern   = Pattern
                                                                .compile(
                                                                        serviceCommentRegex,
                                                                        Pattern.CASE_INSENSITIVE);

    private Pattern             dropTablePattern        = Pattern
                                                                .compile(
                                                                        "^\\s*drop\\s*(?:temporary\\s*)?table\\s*(?:if\\s+exists\\s+)?[`\"]*(?:TUNGSTEN_INFO[`\"]*\\.)[`\"]*([a-zA-Z0-9_]+)",
                                                                        Pattern.CASE_INSENSITIVE);

    // State to track service names and shard ID across fragments. If a first
    // fragment has a service name set, all succeeding fragments must
    // have the same service name. Similarly, all succeeding fragments
    private long                curSeqno                = -1;
    private String              curService              = null;
    private String              curShardId              = null;

    /** If set to true, use default database for unknown SQL operations. */
    public void setUnknownSqlUsesDefaultDb(boolean unknownSqlUsesDefaultDb)
    {
        this.unknownSqlUsesDefaultDb = unknownSqlUsesDefaultDb;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("Scanning for basic event metadata: seqno="
                    + event.getSeqno());

        // Provisionally mark the event as coming from local service. We assume
        // this is a normal transaction, hence do not mark it as Tungsten
        // catalog metadata. N.B.: default SERVICE and SHARD_ID must be
        // non-null or later processing may fail.
        Map<String, String> metadataTags = new TreeMap<String, String>();
        metadataTags.put(ReplOptionParams.SERVICE, context.getServiceName());
        metadataTags.put(ReplOptionParams.SHARD_ID,
                ReplOptionParams.SHARD_ID_UNKNOWN);

        // Empty events are always considered to be local.
        if (event.getDBMSEvent() instanceof DBMSEmptyEvent)
        {
            return adornEvent(event, metadataTags, false);
        }

        // Get the DBMS data values. If there are none, this event is local.
        ArrayList<DBMSData> dbmsDataValues = event.getData();
        if (dbmsDataValues.size() == 0)
        {
            logger.warn("Empty event generated: seqno=" + event.getSeqno()
                    + " eventId=" + event.getEventId());
            return adornEvent(event, metadataTags, false);
        }

        // Ugly MySQL to prevent replication loops on DDL statements: For
        // transactions with a single statement (as in DDL) check whether
        // there is a 'service' name added as a comment at the end of the
        // command. If not, we need to add one. This is the only way we
        // can identify the service for DDL as we don't see the Tungsten
        // update on trep_commit_seqno.
        boolean needsServiceSessionVar = false;
        String serviceSessionVar = null;

        StatementData sd = null;
        if (dbmsDataValues.size() >= 1
                && dbmsDataValues.get(0) instanceof StatementData)
        {
            sd = (StatementData) dbmsDataValues.get(0);
        }
        else if (dbmsDataValues.size() >= 2
                && dbmsDataValues.get(0) instanceof RowIdData
                && dbmsDataValues.get(1) instanceof StatementData)
        {
            sd = (StatementData) dbmsDataValues.get(1);
        }
        if (sd != null)
        {
            String query = sd.getQuery();
            SqlOperation op = (SqlOperation) sd.getParsingMetadata();
            if (op == null)
            {
                op = opMatcher.match(query);
                sd.setParsingMetadata(op);
            }

            if (op.dropTable())
            {
                // Check if this statement contains TUNGSTEN metadata
                Matcher m = dropTablePattern.matcher(query);

                if (logger.isDebugEnabled())
                    logger.debug("Checking whether DROP TABLE query contains Tungsten metadata : "
                            + query);
                if (m.find())
                {
                    serviceSessionVar = m.group(1);
                    if (logger.isDebugEnabled())
                        logger.debug("Found DROP TABLE from service "
                                + serviceSessionVar);
                }
                else if (logger.isDebugEnabled())
                    logger.debug("DROP TABLE does not contain Tungsten metadata");
            }
            else
            {
                String serviceComment = commentEditor.fetchComment(query, op);
                if (serviceComment != null)
                {
                    Matcher m = serviceCommentPattern.matcher(serviceComment);
                    if (m.find())
                    {
                        serviceSessionVar = m.group(1);
                    }
                }
            }

            // If we don't have a value already, we need to set one. This
            // indicates a DDL statement may be in the transaction.
            if (serviceSessionVar == null)
            {
                needsServiceSessionVar = true;
            }
        }

        // Create an index to count database references.
        EventSchemaStatistics schemaStats = new EventSchemaStatistics();

        // Iterate through values inferring the database name.
        for (DBMSData dbmsData : dbmsDataValues)
        {
            if (dbmsData instanceof StatementData)
            {
                // Statement data can have the database either from the
                // schema name on the event or from the database on the
                // statement
                // itself.
                StatementData statData = (StatementData) dbmsData;
                String query = statData.getQuery();

                // See if there is an explicit schema on the statement.
                SqlOperation op = (SqlOperation) statData.getParsingMetadata();
                if (op == null)
                {
                    op = opMatcher.match(query);
                    statData.setParsingMetadata(op);
                }
                String opSchema = op.getSchema();

                // Determine the affected schema.
                String affectedSchema = null;
                if (opSchema != null)
                {
                    // Parsed the schema from the SQL.
                    affectedSchema = opSchema;
                }
                else if (op.isGlobal())
                {
                    // Global operations are not assigned to a shard.
                    affectedSchema = ReplOptionParams.SHARD_ID_UNKNOWN;
                }
                else if (statData.getDefaultSchema() != null)
                {
                    // Use default schema unless we don't recognize SQL and
                    // use does not want default.
                    if (op.getObjectType() == SqlOperation.UNRECOGNIZED
                            && !this.unknownSqlUsesDefaultDb)
                        affectedSchema = ReplOptionParams.SHARD_ID_UNKNOWN;
                    else
                        affectedSchema = statData.getDefaultSchema();
                }

                // If we found a schema, add it to the list. Statements have
                // have null schema at this point use the schema from a previous
                // statement in the transaction, so our work is already done.
                if (affectedSchema != null)
                    schemaStats.incrementSchema(affectedSchema);

                // Check for unsafe statements for bi-directional replication.
                if (op.isBidiUnsafe())
                    metadataTags.put(ReplOptionParams.BIDI_UNSAFE, "true");
            }
            else if (dbmsData instanceof RowChangeData)
            {
                RowChangeData rd = (RowChangeData) dbmsData;
                for (OneRowChange orc : rd.getRowChanges())
                {
                    schemaStats.incrementSchema(orc.getSchemaName());
                }
            }
            else if (dbmsData instanceof LoadDataFileFragment)
            {
                // Add whatever we have to the index.
                String affectedSchema = ((LoadDataFileFragment) dbmsData)
                        .getDefaultSchema();
                schemaStats.incrementSchema(affectedSchema);
            }
            else if (dbmsData instanceof RowIdData)
            {
                // This event type does not have a schema as it's just a
                // session variable.
            }
            else
            {
                logger.warn("Unsupported DbmsData class: "
                        + dbmsData.getClass().getName());
            }
        }

        // Process schema counts and assign shards plus metadata according to
        // rules in the header comment. (See above.)
        schemaStats.countSchemas();

        if (schemaStats.getDbMap().size() == 0)
        {
            // In this case we can't find the schema name, hence cannot assign
            // a specific shard. Use the default shard ID.
            event.getDBMSEvent().addMetadataOption(ReplOptionParams.SHARD_ID,
                    ReplOptionParams.SHARD_ID_UNKNOWN);
            if (logger.isDebugEnabled())
                logger.debug("Unable to infer database: seqno="
                        + event.getSeqno());
        }
        else if (schemaStats.getDbMap().size() == schemaStats
                .getNormalDbCount())
        {
            // Need to split this into a couple of cases...
            if (serviceSessionVar == null)
            {
                if (schemaStats.getNormalDbCount() == 1)
                {
                    // This is a normal transaction. Assign the shard using the
                    // inferred schema name.
                    metadataTags.put(ReplOptionParams.SHARD_ID,
                            schemaStats.getSingleDbName());
                }
                else
                {
                    // More than one schemas are used inside the transaction.
                    // Assigning to UNKNOWN shard
                    metadataTags.put(ReplOptionParams.SHARD_ID,
                            ReplOptionParams.SHARD_ID_UNKNOWN);
                }
            }
            else
            {
                // This is likely a DDL statement from another service. Set the
                // service but also assign the shard using Tungsten shard
                // service conventions.
                metadataTags.put(ReplOptionParams.SERVICE, serviceSessionVar);
                String tungstenDbName = "tungsten_" + serviceSessionVar;
                metadataTags.put(ReplOptionParams.SHARD_ID, tungstenDbName);
            }
        }
        else if (schemaStats.getTungstenDbCount() > 0)
        {
            // This transaction contains Tungsten metadata. The fact that this
            // is logged means we must be from a remote service. Note these
            // facts and use the Tungsten catalog schema as the shard name.
            if (schemaStats.getService() == null)
            {
                // This should not be possible and we should inform the proper
                // authorities.
                logger.warn("Ambiguous service name: seqno=" + event.getSeqno()
                        + " index=" + schemaStats.toString());
                metadataTags.put(ReplOptionParams.SHARD_ID,
                        ReplOptionParams.SHARD_ID_UNKNOWN);
                metadataTags.put(ReplOptionParams.TUNGSTEN_METADATA, "true");
            }
            else
            {
                // This looks legal.
                metadataTags.put(ReplOptionParams.SHARD_ID,
                        schemaStats.getSingleDbName());
                metadataTags.put(ReplOptionParams.SERVICE,
                        schemaStats.getService());
                metadataTags.put(ReplOptionParams.TUNGSTEN_METADATA, "true");
            }
        }
        else
        {
            // This is possible but cannot be processed as a shard. Assign to
            // the default shard.
            logger.debug("Multiple user databases in one transaction: seqno="
                    + event.getSeqno() + " index=" + schemaStats.toString());
            metadataTags.put(ReplOptionParams.SHARD_ID,
                    ReplOptionParams.SHARD_ID_UNKNOWN);
        }

        // Return the event.
        return adornEvent(event, metadataTags, needsServiceSessionVar);
    }

    // Adds current tags to the event.
    private ReplDBMSEvent adornEvent(ReplDBMSEvent event,
            Map<String, String> tags, boolean needsServiceSessionVar)
    {
        // Shard IDs may not be an empty string. This can occur due to
        // transactions extracted from an older Tungsten 1.5 replicator.
        if ("".equals(tags.get(ReplOptionParams.SHARD_ID)))
        {
            tags.put(ReplOptionParams.SHARD_ID,
                    ReplOptionParams.SHARD_ID_UNKNOWN);
            logger.info("Overriding empty shard ID: seqno=" + event.getSeqno()
                    + " fragno=" + event.getFragno());
        }

        // Service names need to be consistent across all fragments. We store
        // the service name from the first fragment and use it for all
        // succeeding fragments.
        if (event.getFragno() == 0)
        {
            // Store state for first fragment.
            curSeqno = event.getSeqno();
            if (event.getLastFrag())
            {
                curService = null;
                curShardId = null;
            }
            else
            {
                curService = tags.get(ReplOptionParams.SERVICE);
                curShardId = tags.get(ReplOptionParams.SHARD_ID);
            }
        }
        else
        {
            // All succeeding fragments get the same service name, so we
            // override any other value.
            if (curSeqno == event.getSeqno())
            {
                // First ensure we have a consistent service name.
                String service = tags.get(ReplOptionParams.SERVICE);
                if (!curService.equals(service))
                {
                    tags.put(ReplOptionParams.SERVICE, curService);
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Overriding service name: seqno="
                                + event.getSeqno() + " fragno="
                                + event.getFragno() + " old service=" + service
                                + " new service=" + curService);
                    }
                }

                // Next ensure we have a consistent shard. Mixed shards
                // can cause serious problems with parallel replication.
                String shardId = tags.get(ReplOptionParams.SHARD_ID);
                if (!curShardId.equals(shardId))
                {
                    tags.put(ReplOptionParams.SHARD_ID, curShardId);
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Overriding shard Id: seqno="
                                + event.getSeqno() + " fragno="
                                + event.getFragno() + " old shard=" + shardId
                                + " new shard=" + curShardId);
                    }
                }
            }
            else
            {
                // This indicates a possible problem with restart--we must
                // have started in the middle of a fragment.
                logger.warn("Potential out-of-sequence event detected; "
                        + "this may indicate an extractor restart problem: "
                        + "event seqno=" + event.getSeqno() + " event fragno="
                        + event.getFragno() + " expected seqno=" + curSeqno);
            }
        }

        // Beautiful code: set metadata tags.
        for (String name : tags.keySet())
        {
            event.getDBMSEvent().addMetadataOption(name, tags.get(name));
        }

        // Put service name in a trailing comment if requested for MySQL DDL.
        // IMPORTANT: We know the type and presence of the StatementData
        // instance below thanks to previous check whether session variable is
        // required.
        if (needsServiceSessionVar && sqlCommentsEnabled)
        {
            // However, don't let replication break if we are somehow wrong.
            try
            {
                StatementData sd;
                // Here StatementData is either in first position or in second
                // position if it comes after a RowId event
                if (event.getData().get(0) instanceof StatementData)
                    sd = (StatementData) event.getData().get(0);
                else
                    sd = (StatementData) event.getData().get(1);

                String query = sd.getQuery();
                SqlOperation op = (SqlOperation) sd.getParsingMetadata();
                if (op == null)
                {
                    op = opMatcher.match(query);
                    sd.setParsingMetadata(op);
                }

                if (op.dropTable())
                {
                    String queryCommented = this.commentEditor.addComment(
                            query, op, tags.get(ReplOptionParams.SERVICE));
                    sd.setQuery(queryCommented);
                }
                else
                {
                    String comment = "___SERVICE___ = ["
                            + tags.get(ReplOptionParams.SERVICE) + "]";
                    String appendableComment = this.commentEditor
                            .formatAppendableComment(op, comment);
                    if (appendableComment == null)
                    {
                        // Have to edit the SQL because we don't have a way to
                        // make
                        // an appendable comment.
                        String queryCommented = this.commentEditor.addComment(
                                query,
                                op,
                                "___SERVICE___ = ["
                                        + tags.get(ReplOptionParams.SERVICE)
                                        + "]");
                        sd.setQuery(queryCommented);
                    }
                    else
                        sd.appendToQuery(appendableComment);
                }
            }
            catch (Exception e)
            {
                logger.warn("Assumption for service session variable violated",
                        e);
            }
        }

        return event;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // Record the plugin context.
        this.context = context;

        // Set the policy for assigning schema from the default.
        TungstenProperties replProps = context.getReplicatorProperties();
        String defaultSchema = replProps.getString(
                ReplicatorConf.SHARD_DEFAULT_DB_USAGE, STRINGENT, true);
        if (STRINGENT.equals(defaultSchema))
            unknownSqlUsesDefaultDb = false;
        else if (RELAXED.equals(defaultSchema))
            unknownSqlUsesDefaultDb = true;
        else
            throw new ReplicatorException("Unknown property value for "
                    + ReplicatorConf.SHARD_DEFAULT_DB_USAGE
                    + "; values must be stringent or relaxed");

        logger.info("Use default schema for unknown SQL statements: "
                + unknownSqlUsesDefaultDb);

        // Check to see if SQL commenting is enabled. WARNING: comments can
        // corrupt statements that have mixed or mis-labeled character sets.
        sqlCommentsEnabled = replProps.getBoolean(
                ReplicatorConf.SERVICE_COMMENTS_ENABLED,
                ReplicatorConf.SERVICE_COMMENTS_ENABLED_DEFAULT, true);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Create a Database instance for the local database so that we can get
        // a proper pattern matcher for operations.
        String url = context.getJdbcUrl(context.getReplicatorSchemaName());
        String user = context.getJdbcUser();
        String password = context.getJdbcPassword();
        try
        {
            Database db = DatabaseFactory.createDatabase(url, user, password);
            opMatcher = db.getSqlNameMatcher();
            commentEditor = new MySQLCommentEditor();
            commentEditor.setCommentEditingEnabled(sqlCommentsEnabled);
            commentEditor.setCommentRegex(serviceCommentRegex);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to create database connection: " + url, e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // Nothing to do.
    }
}