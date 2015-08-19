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

package com.continuent.tungsten.replicator.applier;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.database.TableMetadataCache;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * Implements an applier for MongoDB. This class handles only row updates, as
 * SQL statements are meaningless in MongoDB. We use a local version of the
 * Tungsten trep_commit_seqno table to keep track of updates.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MongoApplier implements RawApplier
{
    private static Logger      logger        = Logger.getLogger(MongoApplier.class);

    // Task management information.
    private int                taskId;
    private String             serviceSchema;

    // Latest event.
    private ReplDBMSHeader     latestHeader;

    // Parameters for the applier.
    private String             connectString = null;
    private boolean            autoIndex     = false;

    // Private connection management.
    private Mongo              m;

    // Table metadata to support auto-indexing.
    private TableMetadataCache tableMetadataCache;

    /** Set the MongoDB connect string, e.g., "myhost:27071". */
    public void setConnectString(String connectString)
    {
        this.connectString = connectString;
    }

    /**
     * If set to true, generate indexes automatically on keys whenever we see a
     * table for the first time.
     */
    public void setAutoIndex(boolean autoIndex)
    {
        this.autoIndex = autoIndex;
    }

    /**
     * Applies row updates to MongoDB. Statements are discarded. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean,
     *      boolean)
     */
    @Override
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
            boolean doRollback) throws ReplicatorException,
            ConsistencyException, InterruptedException
    {
        ArrayList<DBMSData> dbmsDataValues = event.getData();

        // Iterate through values inferring the database name.
        for (DBMSData dbmsData : dbmsDataValues)
        {
            if (dbmsData instanceof StatementData)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring statement");
            }
            else if (dbmsData instanceof RowChangeData)
            {
                RowChangeData rd = (RowChangeData) dbmsData;
                for (OneRowChange orc : rd.getRowChanges())
                {
                    // Get the action as well as the schema & table name.
                    ActionType action = orc.getAction();
                    String schema = orc.getSchemaName();
                    String table = orc.getTableName();
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Processing row update: action=" + action
                                + " schema=" + schema + " table=" + table);
                    }

                    // Process the action.
                    if (action.equals(ActionType.INSERT))
                    {
                        // Connect to the schema and collection.
                        DB db = m.getDB(schema);
                        DBCollection coll = db.getCollection(table);

                        // Fetch column names.
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();

                        // Make a document and insert for each row.
                        Iterator<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues().iterator();
                        while (colValues.hasNext())
                        {
                            BasicDBObject doc = new BasicDBObject();
                            ArrayList<ColumnVal> row = colValues.next();
                            for (int i = 0; i < row.size(); i++)
                            {
                                Object value = row.get(i).getValue();
                                setValue(doc, colSpecs.get(i), value);
                            }
                            if (logger.isDebugEnabled())
                                logger.debug("Adding document: doc="
                                        + doc.toString());
                            coll.insert(doc);
                        }
                    }
                    else if (action.equals(ActionType.UPDATE))
                    {
                        // Connect to the schema and collection.
                        DB db = m.getDB(schema);
                        DBCollection coll = db.getCollection(table);

                        // Ensure required indexes are present.
                        ensureIndexes(coll, orc);

                        // Fetch key and column names.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc
                                .getKeyValues();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc
                                .getColumnValues();

                        // Iterate across the rows.
                        for (int row = 0; row < columnValues.size()
                                || row < keyValues.size(); row++)
                        {
                            List<ColumnVal> keyValuesOfRow = keyValues.get(row);
                            List<ColumnVal> colValuesOfRow = columnValues
                                    .get(row);

                            // Prepare key values query to search for rows.
                            DBObject query = new BasicDBObject();
                            for (int i = 0; i < keyValuesOfRow.size(); i++)
                            {
                                setValue(query, keySpecs.get(i), keyValuesOfRow
                                        .get(i).getValue());
                            }

                            BasicDBObject doc = new BasicDBObject();
                            for (int i = 0; i < colValuesOfRow.size(); i++)
                            {
                                setValue(doc, colSpecs.get(i), colValuesOfRow
                                        .get(i).getValue());
                            }
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("Updating document: query="
                                        + query + " doc=" + doc);
                            }
                            DBObject updatedRow = coll
                                    .findAndModify(query, doc);
                            if (logger.isDebugEnabled())
                            {
                                if (updatedRow == null)
                                    logger.debug("Unable to find document for update: query="
                                            + query);
                                else
                                    logger.debug("Documented updated: doc="
                                            + doc);
                            }
                        }
                    }
                    else if (action.equals(ActionType.DELETE))
                    {
                        // Connect to the schema and collection.
                        DB db = m.getDB(schema);
                        DBCollection coll = db.getCollection(table);

                        // Ensure required indexes are present.
                        ensureIndexes(coll, orc);

                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc
                                .getKeyValues();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc
                                .getColumnValues();

                        // Iterate across the rows.
                        for (int row = 0; row < columnValues.size()
                                || row < keyValues.size(); row++)
                        {
                            List<ColumnVal> keyValuesOfRow = keyValues.get(row);

                            // Prepare key values query to search for rows.
                            DBObject query = new BasicDBObject();
                            for (int i = 0; i < keyValuesOfRow.size(); i++)
                            {
                                setValue(query, keySpecs.get(i), keyValuesOfRow
                                        .get(i).getValue());
                            }

                            if (logger.isDebugEnabled())
                            {
                                logger.debug("Deleting document: query="
                                        + query);
                            }
                            DBObject deletedRow = coll.findAndRemove(query);
                            if (logger.isDebugEnabled())
                            {
                                if (deletedRow == null)
                                    logger.debug("Unable to find document for delete");
                                else
                                    logger.debug("Documented deleted: doc="
                                            + deletedRow);
                            }
                        }
                    }
                    else
                    {
                        logger.warn("Unrecognized action type: " + action);
                        return;
                    }
                }
            }
            else if (dbmsData instanceof LoadDataFileFragment)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring load data file fragment");
            }
            else if (dbmsData instanceof RowIdData)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring row ID data");
            }
            else
            {
                logger.warn("Unsupported DbmsData class: "
                        + dbmsData.getClass().getName());
            }
        }

        // Mark the current header and commit position if requested.
        this.latestHeader = header;
        if (doCommit)
            commit();
    }

    /**
     * @param doc
     * @param columnSpec
     * @param value
     * @throws ReplicatorException
     */
    private void setValue(DBObject doc, ColumnSpec columnSpec, Object value)
            throws ReplicatorException
    {
        String name = columnSpec.getName();

        if (value == null)
            doc.put(name, value);
        else if (value instanceof SerialBlob)
            doc.put(name, deserializeBlob(name, (SerialBlob) value));
        else if (columnSpec.getType() == Types.TIME)
        {
            if (value instanceof Timestamp)
            {
                Timestamp timestamp = ((Timestamp) value);
                StringBuffer time = new StringBuffer(new Time(
                        timestamp.getTime()).toString());
                if (timestamp.getNanos() > 0)
                {
                    time.append(".");
                    time.append(String.format("%09d", timestamp.getNanos()));
                }
                doc.put(name, time.toString());
            }
            else
            {
                Time t = (Time) value;
                doc.put(name, t.toString());
            }
        }
        else
            doc.put(name, value.toString());
    }

    // Ensure that a collection has required indexes.
    private void ensureIndexes(DBCollection coll, OneRowChange orc)
    {
        // If we have not seen this table before, check whether it
        // needs an index.
        if (autoIndex)
        {
            String schema = orc.getSchemaName();
            String table = orc.getTableName();
            Table t = tableMetadataCache.retrieve(schema, table);
            if (t == null)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Ensuring index exists on collection: db="
                            + schema + " collection=" + table);
                }

                // Compute required index keys and ensure they
                // exist in MongoDB.
                List<ColumnSpec> keySpecs = orc.getKeySpec();
                if (keySpecs.size() > 0)
                {
                    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
                    for (ColumnSpec keySpec : keySpecs)
                    {
                        builder.add(keySpec.getName(), 1);
                    }
                    coll.ensureIndex(builder.get());
                }

                // Note that we have processed the table.
                t = new Table(schema, table);
                tableMetadataCache.store(t);
            }
        }
    }

    // Deserialize a blob value. This assumes there are some kind of
    // characters in the byte array that can be translated to a string.
    private String deserializeBlob(String name, SerialBlob blob)
            throws ReplicatorException
    {
        try
        {
            long length = blob.length();
            if (length > 0)
            {
                // Try to deserialize.
                byte[] byteArray = blob.getBytes(1, (int) length);
                String value = new String(byteArray);
                return value;
            }
            else
            {
                // The blob is empty, so just return an empty string.
                return "";
            }
        }
        catch (SerialException e)
        {
            throw new ReplicatorException(
                    "Unable to deserialize blob value: column=" + name, e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#commit()
     */
    @Override
    public void commit() throws ReplicatorException, InterruptedException
    {
        // If we don't have a last header, there is nothing to be done.
        if (latestHeader == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Unable to commit; last header is null");
            return;
        }

        // Connect to the schema and collection.
        DB db = m.getDB(serviceSchema);
        DBCollection trepCommitSeqno = db.getCollection("trep_commit_seqno");

        // Construct query.
        DBObject query = new BasicDBObject();
        query.put("task_id", taskId);

        // Construct update.
        BasicDBObject doc = new BasicDBObject();
        doc.put("task_id", taskId);
        doc.put("seqno", latestHeader.getSeqno());
        // Short seems to cast to Integer in MongoDB.
        doc.put("fragno", latestHeader.getFragno());
        doc.put("last_frag", latestHeader.getLastFrag());
        doc.put("source_id", latestHeader.getSourceId());
        doc.put("epoch_number", latestHeader.getEpochNumber());
        doc.put("event_id", latestHeader.getEventId());
        doc.put("extract_timestamp", latestHeader.getExtractedTstamp()
                .getTime());

        // Update trep_commit_seqno.
        DBObject updatedDoc = trepCommitSeqno.findAndModify(query, null, null,
                false, doc, true, true);
        if (logger.isDebugEnabled())
        {
            if (updatedDoc == null)
                logger.debug("Unable to update/insert trep_commit_seqno: query="
                        + query + " doc=" + doc);
            else
                logger.debug("Trep_commit_seqno updated: updatedDoc="
                        + updatedDoc);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#getLastEvent()
     */
    @Override
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        // Connect to the schema and collection.
        DB db = m.getDB(serviceSchema);
        DBCollection trepCommitSeqno = db.getCollection("trep_commit_seqno");

        // Construct query.
        DBObject query = new BasicDBObject();
        query.put("task_id", taskId);

        // Find matching trep_commit_seqno value.
        DBObject doc = trepCommitSeqno.findOne(query);

        // Return a constructed header or null, depending on whether we found
        // anything.
        if (doc == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("trep_commit_seqno is empty: taskId=" + taskId);
            return null;
        }
        else
        {
            if (logger.isDebugEnabled())
                logger.debug("trep_commit_seqno entry found: doc=" + doc);

            long seqno = (Long) doc.get("seqno");
            // Cast to integer in MongoDB.
            int fragno = (Integer) doc.get("fragno");
            boolean lastFrag = (Boolean) doc.get("last_frag");
            String sourceId = (String) doc.get("source_id");
            long epochNumber = (Long) doc.get("epoch_number");
            String eventId = (String) doc.get("event_id");
            String shardId = (String) doc.get("shard_id");
            long extractTimestamp = (Long) doc.get("extract_timestamp");
            ReplDBMSHeaderData header = new ReplDBMSHeaderData(seqno,
                    (short) fragno, lastFrag, sourceId, epochNumber, eventId,
                    shardId, new Timestamp(extractTimestamp), 0);
            return header;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#rollback()
     */
    @Override
    public void rollback() throws InterruptedException
    {
        // Does nothing for now.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    @Override
    public void setTaskId(int id)
    {
        this.taskId = id;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        this.serviceSchema = "tungsten_" + context.getServiceName();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Connect to MongoDB.
        if (logger.isDebugEnabled())
        {
            logger.debug("Connecting to MongoDB: connectString="
                    + connectString);
        }
        m = null;
        try
        {
            if (connectString == null)
                m = new Mongo();
            else
                m = new Mongo(connectString);
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to connect to MongoDB: connection="
                            + this.connectString, e);
        }

        // Initialize table metadata cache.
        tableMetadataCache = new TableMetadataCache(5000);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Close connection to MongoDB.
        if (m != null)
        {
            m.close();
            m = null;
        }

        // Release table cache.
        if (tableMetadataCache != null)
        {
            tableMetadataCache.invalidateAll();
            tableMetadataCache = null;
        }
    }
}