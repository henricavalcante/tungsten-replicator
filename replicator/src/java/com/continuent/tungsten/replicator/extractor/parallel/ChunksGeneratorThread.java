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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.parallel;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.datasource.UniversalDataSource;
import com.continuent.tungsten.replicator.extractor.parallel.ChunkDefinitions.ChunkRequest;

/**
 * Implements a chunk generator thread that is responsible of generating chunks
 * for tables.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ChunksGeneratorThread extends Thread
{

    /**
     * Hold the min, the max and the count values for a table.
     * 
     * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
     * @version 1.0
     */
    private class MinMax
    {

        private long   count;

        private Number min;

        private Number max;

        public MinMax(Number min, Number max, long count)
        {
            this.min = min;
            this.max = max;
            this.count = count;
        }

        /**
         * Returns the count value.
         * 
         * @return Returns the count.
         */
        protected long getCount()
        {
            return count;
        }

        /**
         * Returns the min value.
         * 
         * @return Returns the min.
         */
        protected Number getMin()
        {
            return min;
        }

        /**
         * Returns the max value.
         * 
         * @return Returns the max.
         */
        protected Number getMax()
        {
            return max;
        }

        protected boolean isDecimal()
        {
            return !(this.min instanceof Long);
        }

    }

    private static Logger        logger    = Logger.getLogger(ChunksGeneratorThread.class);
    private Database             connection;
    private BlockingQueue<Chunk> chunks;
    private String               chunkDefFile;
    private ChunkDefinitions     chunkDefinition;
    private int                  extractChannels;
    private long                 chunkSize = 1000;
    private String               eventId   = null;
    private String               whereClause;
    private UniversalDataSource  dataSource;

    /**
     * Creates a new <code>ChunksGeneratorThread</code> object
     * 
     * @param extractChannels
     * @param chunks
     * @param chunkDefinitionFile
     * @param chunkSize
     */
    public ChunksGeneratorThread(UniversalDataSource datasource,
            int extractChannels, BlockingQueue<Chunk> chunks,
            String chunkDefinitionFile, long chunkSize)
    {
        this.setName("ChunkGeneratorThread");
        this.dataSource = datasource;
        this.chunks = chunks;
        this.chunkDefFile = chunkDefinitionFile;
        this.extractChannels = extractChannels;
        if (chunkSize > 0)
            this.chunkSize = chunkSize;
    }

    public void run()
    {
        try
        {
            runTask();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Perform the chunk generation
     */
    private void runTask()
    {
        connection = null;
        try
        {
            // Establish a connection to the data source.
            logger.info("Connecting to data source");

            // Create a connection, suppressing logging if desired.
            connection = (Database) dataSource.getConnection();
        }
        catch (ReplicatorException e)
        {
            e.printStackTrace();
        }

        try
        {
            connection.connect();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }

        // Check whether we have to use a chunk definition file
        if (chunkDefFile != null)
        {
            logger.info("Using definition from file " + chunkDefFile);
            chunkDefinition = new ChunkDefinitions(chunkDefFile);
            try
            {
                chunkDefinition.parseFile();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            catch (ReplicatorException e)
            {
                e.printStackTrace();
            }

            LinkedList<ChunkRequest> chunksDefinitions = chunkDefinition
                    .getChunksDefinitions();
            for (ChunkRequest chunkRequest : chunksDefinitions)
            {
                if (chunkRequest.getTable() != null)
                {
                    try
                    {
                        Table table = connection.findTable(
                                chunkRequest.getSchema(),
                                chunkRequest.getTable(), true);

                        if (table != null)
                            generateChunksForTable(table,
                                    chunkRequest.getChunkSize(),
                                    chunkRequest.getColumns());
                        else
                            logger.warn("Failed while processing table "
                                    + chunkRequest.getSchema() + "."
                                    + chunkRequest.getTable()
                                    + " : table not found.");
                    }
                    catch (SQLException e)
                    {
                        e.printStackTrace();
                    }
                    catch (ReplicatorException e)
                    {
                        e.printStackTrace();
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
                else if (chunkRequest.getSchema() != null)
                {
                    generateChunksForSchema(chunkRequest.getSchema());
                }
            }
        }
        else
        {
            try
            {
                DatabaseMetaData databaseMetaData = connection
                        .getDatabaseMetaData();
                ResultSet schemasRs = databaseMetaData.getSchemas();
                while (schemasRs.next())
                {
                    String schemaName = schemasRs.getString("TABLE_SCHEM");
                    // TODO: System schemas could be needed -> this needs a
                    // setting
                    if (!connection.isSystemSchema(schemaName))
                    {
                        generateChunksForSchema(schemaName);
                    }
                }
                schemasRs.close();
            }
            catch (SQLException e)
            {
                logger.error(e);
            }
            catch (Exception e)
            {
                logger.error(e);
            }
        }

        // Stop threads
        for (int i = 0; i < extractChannels; i++)
        {
            logger.info("Posting job complete request " + i);
            try
            {
                chunks.put(new NumericChunk());
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }

        if (logger.isDebugEnabled())
            logger.debug(this.getName() + " done.");
    }

    /**
     * Generates chunk definitions for the whole given schema.
     * 
     * @param schemaName Name of the schema for which chunk definitions are
     *            generated.
     */
    private void generateChunksForSchema(String schemaName)
    {
        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Getting list of tables from " + schemaName);

            ArrayList<Table> tablesFromSchema = connection.getTables(
                    schemaName, true, true);
            if (logger.isDebugEnabled())
                logger.debug("Tables : " + tablesFromSchema);
            if (tablesFromSchema != null && tablesFromSchema.size() > 0)
            {
                for (Table table : tablesFromSchema)
                {
                    generateChunksForTable(table, -1, null);
                }

            }
        }
        catch (Exception e)
        {
            e.printStackTrace();

        }
    }

    /**
     * Generates chunk definitions for the given table, eventually using a given
     * chunk size and a given column list.
     * 
     * @param table Table for which chunk definitions are generated.
     * @param tableChunkSize Chunk size to be used for the given table.
     * @param columns List of columns to be extracted for the given table.
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    private void generateChunksForTable(Table table, long tableChunkSize,
            String[] columns) throws ReplicatorException, InterruptedException
    {
        long chunkSize;

        Integer pkType = getPKType(table);

        if (tableChunkSize == 0)
        {
            chunks.put(new NoChunk(table, columns));
            // No chunks for this table (all table at once)
            return;
        }
        else if (tableChunkSize < 0)
        {
            // Use default chunk size
            chunkSize = this.chunkSize;
        }
        else
        {
            chunkSize = tableChunkSize;
        }

        if (logger.isDebugEnabled())
            logger.debug("Processing table " + table.getSchema() + "."
                    + table.getName());

        if (pkType == null)
            chunkLimit(table);
        else if (pkType == Types.NUMERIC)
            chunkNumericPK(table, columns, chunkSize);
        else if (pkType == Types.VARCHAR)
            chunkVarcharPK(table);
    }

    /**
     * TODO: chunkNumericPK definition.
     * 
     * @param table
     * @param columns
     * @param chunkSize
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    private void chunkNumericPK(Table table, String[] columns, long chunkSize)
            throws ReplicatorException, InterruptedException
    {
        // Retrieve PK range
        MinMax minmax = retrieveMinMaxCountPK(connection, table);

        if (minmax != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Min = " + minmax.getMin() + " -- Max = "
                        + minmax.getMax() + " -- Count = " + minmax.getCount());

            if (minmax.getCount() <= chunkSize)
                // Get the whole table at once
                chunks.put(new NumericChunk(table, columns));
            else
            {
                // Share the joy among threads,
                // if primary key is evenly distributed
                if (!minmax.isDecimal())
                {
                    long gap = (Long) minmax.getMax() - (Long) minmax.getMin();
                    long blockSize = chunkSize * gap / minmax.getCount();

                    long nbBlocks = gap / blockSize;
                    if (gap % blockSize > 0)
                        nbBlocks++;

                    long start = (Long) minmax.getMin() - 1;
                    long end;
                    do
                    {
                        end = start + blockSize;
                        if (end > (Long) minmax.getMax())
                            end = (Long) minmax.getMax();
                        NumericChunk e = new NumericChunk(table, start, end,
                                columns, nbBlocks);
                        chunks.put(e);
                        start = end;
                    }
                    while (start < (Long) minmax.getMax());
                }
                else
                {
                    BigInteger start = ((BigDecimal) minmax.getMin())
                            .setScale(0, RoundingMode.FLOOR).toBigInteger()
                            .subtract(BigInteger.valueOf(1));

                    BigInteger max = ((BigDecimal) minmax.getMax()).setScale(0,
                            RoundingMode.CEILING).toBigInteger();

                    BigInteger gap = max.subtract(start);

                    BigInteger blockSize = gap.multiply(
                            BigInteger.valueOf(chunkSize)).divide(
                            BigInteger.valueOf(minmax.getCount()));

                    long nbBlocks = gap.divide(blockSize).longValue();

                    if (!gap.remainder(blockSize).equals(BigInteger.ZERO))
                    {
                        nbBlocks++;
                        blockSize = gap.divide(BigInteger.valueOf(nbBlocks))
                                .add(gap.remainder(blockSize).equals(
                                        BigInteger.ZERO)
                                        ? BigInteger.ZERO
                                        : BigInteger.ONE);
                    }
                    BigInteger end;
                    do
                    {
                        end = start.add(blockSize);
                        if (end.compareTo((((BigDecimal) minmax.getMax())
                                .setScale(0, RoundingMode.CEILING))
                                .toBigInteger()) == 1)
                            end = (((BigDecimal) minmax.getMax()).setScale(0,
                                    RoundingMode.CEILING)).toBigInteger();

                        NumericChunk e = new NumericChunk(table, start, end,
                                columns, nbBlocks);
                        chunks.put(e);
                        start = end;
                    }
                    while (start.compareTo((((BigDecimal) minmax.getMax())
                            .setScale(0, RoundingMode.CEILING)).toBigInteger()) == -1);

                }

            }
        }
        else
        {
            // table is empty or does not have a
            // good candidate as a PK for chunking.
            // Fall back to limit method
            chunks.put(new NumericChunk(table, columns));
        }
    }

    /**
     * Retrieve maximum or minimum value of a table's primary key. Table must
     * have a single-column numeric key for this to work correctly.
     */
    private MinMax retrieveMinMaxCountPK(Database conn, Table table)
            throws ReplicatorException
    {

        String pkName = table.getPrimaryKey().getColumns().get(0).getName();
        String sql = String.format(
                "SELECT MIN(%s),MAX(%s), COUNT(%s) FROM %s",
                pkName,
                pkName,
                pkName,
                conn.getDatabaseObjectName(table.getSchema()) + '.'
                        + conn.getDatabaseObjectName(table.getName()));

        sql += AbstractChunk.getFlashbackQueryClause(conn, eventId);

        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery(sql);
            if (rs.next())
            {
                Object min = rs.getObject(1);
                Object max = rs.getObject(2);
                if (min instanceof Long && max instanceof Long)
                {
                    return new MinMax(((Long) min), ((Long) max), rs.getLong(3));

                }
                else if (min instanceof BigDecimal && max instanceof BigDecimal)
                {
                    return new MinMax(((BigDecimal) min), ((BigDecimal) max),
                            rs.getLong(3));

                }
                return null;
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to retrieve min, max and count values for PK "
                    + pkName + " in table "
                    + conn.getDatabaseObjectName(table.getSchema()) + '.'
                    + conn.getDatabaseObjectName(table.getName()));
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                    e.printStackTrace();
                }
            }
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException e)
                {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * TODO: getPKType definition.
     * 
     * @param table
     */
    private Integer getPKType(Table table)
    {
        if (table.getPrimaryKey() == null)
        {
            logger.warn(table.getName() + " has no PK");
            return null;
        }
        else if (table.getPrimaryKey().getColumns().size() != 1)
        {
            logger.warn(table.getName() + " - PK is not a single-column one "
                    + table.getPrimaryKey().getColumns());
            return null;
        }
        else
        {
            // Check whether primary key is NUMBER based
            int type = table.getPrimaryKey().getColumns().get(0).getType();

            if (type == Types.VARCHAR
                    || (type == Types.OTHER && table.getPrimaryKey()
                            .getColumns().get(0).getTypeDescription()
                            .contains("VARCHAR")))
            {
                return Types.VARCHAR;
            }
            else if ((type == Types.BIGINT || type == Types.INTEGER
                    || type == Types.SMALLINT || type == Types.DECIMAL))
            {
                return Types.NUMERIC;
            }
            else
            {
                logger.warn(table.getName()
                        + " - PK is not a supported chunking datatype ");
                return null;
            }
        }
    }

    private void chunkVarcharPK(Table table) throws InterruptedException
    {
        String pkName = table.getPrimaryKey().getColumns().get(0).getName();
        String fqnTable = connection.getDatabaseObjectName(table.getSchema())
                + '.' + connection.getDatabaseObjectName(table.getName());
        // Get Count
        String sql = String.format("SELECT COUNT(%s) as cnt FROM %s", pkName,
                fqnTable);

        sql += AbstractChunk.getFlashbackQueryClause(connection, eventId);

        // if count <= Chunk size, we are done
        long count = 0;
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = connection.createStatement();
            rs = st.executeQuery(sql);
            if (rs.next())
            {
                count = rs.getLong("cnt");
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to retrieve count value for PK " + pkName
                    + " in table " + fqnTable, e);
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                    e.printStackTrace();
                }
            }
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException e)
                {
                    e.printStackTrace();
                }
            }
        }

        if (count == 0)
            return;

        if (count <= chunkSize)
        {
            chunks.put(new StringChunk(table, null, null));
            return;
        }

        // Else (count > CHUNK_SIZE) : chunk again in smaller parts
        long nbBlocks = count / chunkSize;

        if (count % chunkSize > 0)
            nbBlocks++;

        long blockSize = count / nbBlocks;

        PreparedStatement pstmt = null;

        // Note : This does not perform very well. It is better to scan the
        // index
        // than to try to use a query based on rownum

        StringBuffer sqlBuf = new StringBuffer("SELECT MIN(");
        sqlBuf.append(pkName);
        sqlBuf.append(") as min, MAX(");
        sqlBuf.append(pkName);
        sqlBuf.append(") as max, COUNT(");
        sqlBuf.append(pkName);
        sqlBuf.append(") as cnt FROM ( select sub.*, ROWNUM rnum from ( SELECT ");
        sqlBuf.append(pkName);
        sqlBuf.append(" FROM ");
        sqlBuf.append(fqnTable);
        sqlBuf.append(" ORDER BY ");
        sqlBuf.append(pkName);

        sqlBuf.append(AbstractChunk
                .getFlashbackQueryClause(connection, eventId));

        sqlBuf.append(") sub where ROWNUM <= ? ) where rnum >= ?");

        sql = sqlBuf.toString();

        try
        {
            pstmt = connection.prepareStatement(sql);
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }

        try
        {
            for (long i = 0; i < count; i += blockSize + 1)
            {
                try
                {
                    pstmt.setLong(1, i + blockSize);
                    pstmt.setLong(2, i);
                    rs = pstmt.executeQuery();

                    if (rs.next())
                        chunks.put(new StringChunk(table, rs.getString("min"),
                                rs.getString("max"), nbBlocks));

                }
                catch (SQLException e)
                {
                    e.printStackTrace();
                }
                finally
                {
                    try
                    {
                        rs.close();
                    }
                    catch (SQLException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
        finally
        {
            try
            {
                pstmt.close();
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
    }

    private void chunkLimit(Table table) throws InterruptedException
    {
        PreparedStatement pStmt = null;

        // Table does not have a primary key. Let's chunk using limit.
        String fqnTable = connection.getDatabaseObjectName(table.getSchema())
                + '.' + connection.getDatabaseObjectName(table.getName());

        // Get Count
        String sql = String.format("SELECT COUNT(*) as cnt FROM %s", fqnTable);

        sql += AbstractChunk.getFlashbackQueryClause(connection, eventId);

        long count = 0;
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = connection.createStatement();
            if (logger.isDebugEnabled())
                logger.debug("Running " + sql);
            rs = st.executeQuery(sql);
            if (rs.next())
            {
                count = rs.getLong("cnt");
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to retrieve row count values for table "
                    + fqnTable, e);
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                    logger.warn("Error while closing resultset", e);
                }
            }
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException e)
                {
                    logger.warn("Error while closing statement", e);
                }
            }
        }
        if (count == 0)
            return;

        if (count <= chunkSize)
        {
            chunks.put(new LimitChunk(table));
            return;
        }

        // Else (count > CHUNK_SIZE) : chunk again in smaller parts
        long nbBlocks = count / chunkSize;

        if (count % chunkSize > 0)
            nbBlocks++;

        long blockSize = (long) Math.ceil((double) count / (double) nbBlocks);

        try
        {
            pStmt = generateChunkingPreparedStatement(table, blockSize);
            if (pStmt == null)
            {
                chunks.put(new NoChunk(table, null));
                // No chunks for this table (all table at once)
                return;
            }

        }
        catch (SQLException e)
        {
            logger.warn("Error while preparing chunking prepared statement", e);
        }

        Object[] fromValues = null, toValues = null;

        ResultSet result = null;
        try
        {
            result = pStmt.executeQuery();

            long chunkSize = 0;
            while (result.next())
            {
                chunkSize++;
                if (chunkSize % blockSize == 0)
                {
                    // We reached the desired chunk size : send the chunk
                    // definition.
                    toValues = new Object[result.getMetaData().getColumnCount()];
                    for (int j = 0; j < toValues.length; j++)
                    {
                        toValues[j] = result.getObject(j + 1);
                    }

                    chunks.put(new LimitChunk(table, 0, 0 + blockSize,
                            nbBlocks, fromValues, toValues, whereClause,
                            blockSize));

                    fromValues = toValues;
                }
                else if (chunkSize >= count)
                {
                    // Last chunk : we eventually did not reach the chunk size,
                    // but the table was fully processed : send the last chunk
                    // definition.
                    chunks.put(new LimitChunk(table, 0, 0 + blockSize,
                            nbBlocks, fromValues, null, whereClause, blockSize));
                }
            }
        }
        catch (SQLException e1)
        {
            logger.warn("Error while executing chunking query", e1);
        }
        finally
        {
            if (result != null)
            {
                try
                {
                    result.close();
                }
                catch (SQLException e)
                {
                    logger.warn("Error while closing resultset", e);
                }
            }
            if (pStmt != null)
                try
                {
                    pStmt.close();
                    pStmt = null;
                }
                catch (SQLException e)
                {
                    logger.warn(
                            "Error while closing chunking prepared statement",
                            e);
                }
        }
    }

    private PreparedStatement generateChunkingPreparedStatement(Table table,
            long blockSize) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Handling table " + table.toExtendedString());

        String fqnTable = connection.getDatabaseObjectName(table.getSchema())
                + '.' + connection.getDatabaseObjectName(table.getName());

        StringBuffer sqlBuffer = new StringBuffer("SELECT ");
        StringBuffer colBuf = new StringBuffer();
        whereClause = new String();

        Key key;

        if (table.getPrimaryKey() != null)
        {
            // No dedicated chunking algorithm for this type of pk (either
            // composite or datatype not handled)
            key = table.getPrimaryKey();
        }
        else
        {
            // This is a unique key that can be used
            key = table.getPKFromUniqueIndex();

            if (key == null)
            {
                logger.info("getPKFromUniqueIndex returned null key for table "
                        + table);
                return null;
            }
        }
        ArrayList<Column> colsList = key.getColumns();

        if (logger.isDebugEnabled())
            logger.debug("colsList = " + colsList);

        Column[] columns = new Column[colsList.size()];
        int i = 0;
        for (Column column : colsList)
        {
            columns[i] = column;
            i++;
        }

        whereClause = buildWhereClause(columns, 0);

        for (int j = 0; j < columns.length; j++)
        {
            if (j > 0)
            {
                colBuf.append(", ");
            }
            colBuf.append(columns[j].getName());
        }

        sqlBuffer.append(colBuf);

        sqlBuffer.append(" FROM ");
        sqlBuffer.append(fqnTable);

        sqlBuffer.append(AbstractChunk.getFlashbackQueryClause(connection,
                eventId));

        sqlBuffer.append(" ORDER BY ");
        sqlBuffer.append(colBuf);

        String sql = sqlBuffer.toString();
        if (logger.isDebugEnabled())
            logger.debug("Generated statement :" + sql);
        PreparedStatement pStmt = connection.prepareStatement(sql);
        // This should probably be a setting (fetch size). For now, let it be
        // hardcoded.
        pStmt.setFetchSize(100);
        return pStmt;
    }

    private String buildWhereClause(Column[] columns, int index)
    {
        if (index == columns.length - 1)
        {
            return columns[index].getName() + " > ? ";
        }
        else
            return columns[index].getName() + " > ? OR ("
                    + columns[index].getName() + " = ? AND "
                    + buildWhereClause(columns, index + 1) + ")";
    }

    public void setEventId(String eventId)
    {
        this.eventId = eventId;
    }

}