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
 * Initial developer(s): Scott Martin
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.extractor.oracle;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Defines a replication event extractor, which reads events from Oracle via a
 * scoket connection to the actual C based Oracle extractor.
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 */
public class OracleExtractor implements RawExtractor
{

    private static Logger      logger       = Logger
                                                    .getLogger(OracleExtractor.class);

    private OracleCommunicator communicator = null;
    private OracleParser       parser       = null;
    private int                callCount    = 0;
    private String             mapping;
    private int                dsPort;
    private boolean            initialized  = false;
    private String             lastEventID;
    private String             lastSCN;
    private boolean            lastIsRow;
    private boolean            rowLevel     = true;
    private Database           database     = null;

    protected String           host         = null;
    protected String           instance     = null;
    protected String           user         = null;
    protected String           password     = null;
    protected String           port         = "1521";
    protected String           url          = null;
    protected String           startingSCN  = null;

    /**
     * Return the name of the host associated with the extractor
     * 
     * @return the host
     */
    public String getHost()
    {
        return host;
    }

    /**
     * Set the host name associated with the extractor
     * 
     * @param host new host name
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * Return the current schemaName.tableName mapping string.
     * 
     * @return the mapping
     */
    public String getMapping()
    {
        return mapping;
    }

    /**
     * Set the current schemaName.tableName mapping string
     * 
     * @param mapping New mapping
     */
    public void setMapping(String mapping)
    {
        this.mapping = mapping;
    }

    /**
     * get the current Oracle user name associated with the extractor.
     * 
     * @return the user
     */
    public String getUser()
    {
        return user;
    }

    /**
     * Set the current Oracle user name associated with the extractor
     * 
     * @param user new user name
     */
    public void setUser(String user)
    {
        this.user = user;
    }

    /**
     * Get the Oracle sid of the extractor
     * 
     * @return the instance
     */
    public String getInstance()
    {
        return instance;
    }

    /**
     * Set the Oracle sid associated with the extractor.
     * 
     * @param instance new sid
     */
    public void setInstance(String instance)
    {
        this.instance = instance;
    }

    /**
     * Get the Oracle password.
     * 
     * @return the password
     */
    public String getPassword()
    {
        return password;
    }

    /**
     * Set the Oracle password.
     * 
     * @param password New password
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Get starting system commit number.
     * 
     * @return the startingSCN
     */
    public String getStartingSCN()
    {
        return startingSCN;
    }

    /**
     * Set the starting system commit number.
     * 
     * @param startingSCN new starting system commit number.
     */
    public void setStartingSCN(String startingSCN)
    {
        this.startingSCN = startingSCN;
    }

    /**
     * Get the port we attached to Oracle with.
     * 
     * @return the port
     */
    public String getPort()
    {
        return port;
    }

    /**
     * Set the port we attach to Oracle with.
     * 
     * @param port new port
     */
    public void setPort(String port)
    {
        this.port = port;
    }

    /**
     * Get the port we attach to dslisten with.
     * 
     * @return the DS port
     */
    public int getDsport()
    {
        return dsPort;
    }

    /**
     * Return TRUE IFF we are performing row level extraction..
     * 
     * @return the row level
     */
    public boolean getRowlevel()
    {
        return rowLevel;
    }

    /**
     * Set boolean that controls row level extraction..
     * 
     * @param rowLevel new value for row level.
     */
    public void setRowlevel(boolean rowLevel)
    {
        this.rowLevel = rowLevel;
    }

    /**
     * Set the port value used to communicate with dslisten.
     * 
     * @param portAsString new port value.
     */
    public void setDsport(String portAsString)
    {
        this.dsPort = Integer.parseInt(portAsString);
    }

    /**
     * Return the last event id retrieved from dslisten.
     * 
     * @return the log position
     */
    protected String getLogPosition()
    {
        if (logger.isDebugEnabled())
            logger.debug("getLogPosition: returning " + lastEventID);

        return lastEventID;
    }

    /**
     * Get next operation from dslisten - typically either a new statement or a new row.
     * 
     * @return the next operation
     * @throws ExtractorException
     */
    private String getNextOp() throws InterruptedException, ExtractorException
    {
        String sql;
        callCount++;
        int currentIndex=0;
        int nextIndex=0;

        try
        {
            sql = communicator.getSQL();
            if (logger.isDebugEnabled())
                logger.debug("Extracted SQL: " + sql);

            // current format is "%s:%c:%s", eventID, ('S' || 'R'), sql_text */
            // 'S' -> statement coming back from extractor
            // 'R' -> row coming back from extractor
            nextIndex = sql.indexOf(':', currentIndex);
            lastEventID = sql.substring(currentIndex, nextIndex);
            currentIndex = nextIndex;

            if (sql.charAt(currentIndex+1) == 'R') lastIsRow = true;
            else lastIsRow = false;
            currentIndex += 3;

            String data = sql.substring(currentIndex);

            // now parse SCN from eventID.  EventID = SCN.a.b.c.d
            currentIndex = 0;
            nextIndex = lastEventID.indexOf('.', currentIndex);
            try
            {
                lastSCN = sql.substring(currentIndex, nextIndex);
            }
            catch (Exception e)
            {
                // logger.info("sql = " + sql);
                // logger.info("lastEventID = " + lastEventID);
                throw new ExtractorException("Bad index from Oracle extractor", e);
            }

            // The following can be useful for debugging restartability
            //if (callCount >= 11)
            //{
            //  logger.debug("Testing restartability by terminating...");
            //  logger.debug("Terminating at message " + data);
            //  throw new ExtractorException("Testing message failure at count " + callCount);
            //}

            //logger.debug("lastSCN     = " + lastSCN);
            //logger.debug("lastEventID = " + lastEventID);
            //logger.debug("lastIsRow   = " + (lastIsRow ? "TRUE" : "FALSE"));
            //logger.debug("callcount   = " + callCount);
            //logger.debug("data        = " + data);

            //logger.info("size data    = " + data.length());
            //logger.info("data         = " + data);

            return data;
        }
        catch (InterruptedException e)
        {
            // This occurs during normal shutdown this is how it gets reported back to THL.run()
            throw e;
        }
        catch (Exception e)
        {
            String msg = "Error while fetching SQL from Oracle communicator";
            logger.error(msg, e);
            throw new ExtractorException(msg, e);
        }
    }

    /**
     * Return the next row level event from received from dslisten.
     * 
     * @return next DBMSEvent found in the logs
     * @throws ExtractorException
     */
    protected DBMSEvent extractEventRowLevel(String firstData) throws InterruptedException, ExtractorException
    {
        DBMSEvent dbmsEvent = null;
        RowChangeData rowChangeData = new RowChangeData();
        OneRowChange oneRowChange = null;
        boolean first = true;

        if (!initialized)
            throw new ExtractorException("Extractor not initialized");

        ArrayList<DBMSData> trx = new ArrayList<DBMSData>(128);

        while (true)
        {
            String data;

            if (first) data = firstData;
            else       data = getNextOp();
          
            first = false;

            oneRowChange = parser.parse(data, lastSCN);

            if (oneRowChange == null)
            {
                trx.add(rowChangeData);
                dbmsEvent = new DBMSEvent(lastEventID, trx, new Timestamp(System.currentTimeMillis()));
                break;
            }
            else
            {
                rowChangeData.appendOneRowChange(oneRowChange);
            }
        }
        return dbmsEvent;
    }

    /**
     * Return the next statement extracted from dslisten
     * 
     * @return next DBMSEvent found in the logs
     * @throws ExtractorException
     */
    protected DBMSEvent extractEventStatementLevel(String firstData)
        throws InterruptedException, ExtractorException
    {
        boolean doCommit;
        boolean first = true;

        if (!initialized)
            throw new ExtractorException("Extractor not initialized");

        ArrayList<DBMSData> trx = new ArrayList<DBMSData>(128);

        // This is statement level replication code path
        while (true)
        {
            DBMSEvent dbmsEvent = null;
            String queryString;

            if (first) queryString = firstData;
            else       queryString = getNextOp();
          
            first = false;

            if (queryString.compareTo("COMMIT") == 0)
            {
                doCommit = true;
            }
            else
            {
                doCommit = false;
            }

            if (queryString.indexOf("truncate") == 0
                    || queryString.indexOf("TRUNCATE") == 0)
            {
                trx.add(new StatementData(queryString));
                doCommit = true;
            }

            if (doCommit)
            {
                dbmsEvent = new DBMSEvent(lastEventID, trx, new Timestamp(System.currentTimeMillis()));
            }
            else
            {
                // trx.add("USE REPADM");
                trx.add(new StatementData(queryString));
            }

            if (dbmsEvent != null)
            {
                return dbmsEvent;
            }
        }

        /*
         * // This is row level replication code path ArrayList<DBMSData>
         * dataArray = new ArrayList<DBMSData>(); DBMSEvent dbmsEvent = null;
         * String test = ""; test =
         * "0020.000b.0000057b:069d.00001161.0010:U:000016279:AAAD+XAAEAAABZrAAA:" ;
         * test += "0024000200018002c1020003000cScott
         * Martin:0003000100028003c26464"; RowChangeData rowChangeData =
         * parser.parse(test); dataArray.add(rowChangeData); dbmsEvent = new
         * DBMSEvent(lastSCN, new DBMSMetadata(), dataArray); try
         * {Thread.sleep(10000);} catch (InterruptedException e) {} return
         * dbmsEvent;
         */
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract()
     */
    public synchronized DBMSEvent extract() throws InterruptedException,
            ExtractorException
    {
        String data = getNextOp();

        if (lastIsRow)
            return extractEventRowLevel(data);
        else
            return extractEventStatementLevel(data);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract(java.lang.String)
     */
    public DBMSEvent extract(String id) throws InterruptedException,
            ExtractorException
    {
        String data = getNextOp();

        if (lastIsRow)
            return extractEventRowLevel(data);
        else
            return extractEventStatementLevel(data);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ExtractorException
    {
        String lastLogPosition;

        if (initialized)
            throw new ExtractorException("Extractor already initialized");

        initialized = true;

        if (eventId == null)
        {
            lastLogPosition = "0";
        } else {
            lastLogPosition = eventId;
        }

        logger.info("Starting from transaction id: " + lastLogPosition);

        try
        {
            communicator.connect(user, password, startingSCN, rowLevel, lastLogPosition);
        }
        catch (Exception e)
        {
            String msg = "Error while connecting to Oracle communicator";
            logger.error(msg, e);
            throw new ExtractorException(msg, e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(PluginContext
     *      context)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        logger.info("OracleExtractor PREPARE:");
        logger.info("host         = " + host);
        logger.info("instance     = " + instance);
        logger.info("user         = " + user);
        logger.info("password     = " + password);
        logger.info("Oracle port  = " + port);
        logger.info("Java/C port  = " + dsPort);
        if (startingSCN != null)
            logger.info("Starting SCN = " + startingSCN);

        url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + instance;

        try
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Connecting with \"" + url + "\"");
            }
            database = DatabaseFactory.createDatabase(url, user, password);
            database.connect();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }

        communicator = new OracleCommunicator(host, instance, dsPort);
        parser = new OracleParser(database);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(PluginContext
     *      context)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // Send message to dslisten to have it shut down.
        try
        {
            if (communicator != null) communicator.disconnect();
        }
        catch (ExtractorException e)
        {
            // eat the errors here since there are many ways 
            // the Java and C based extractor can shutdown and all
            // of ways lead to different signal paths.
            // String msg = "Error while connecting to Oracle communicator";
            // logger.error(msg, e);
            // throw new ReplicatorException(msg, e);
        }
        communicator = null;
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ExtractorException,
            InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("getCurrentResourceEventId: returning " + lastEventID);

        return lastEventID;
    }
}
