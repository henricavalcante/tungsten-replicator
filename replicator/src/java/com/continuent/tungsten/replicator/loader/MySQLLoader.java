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
 * Initial developer(s): Jeff Mace
 */

package com.continuent.tungsten.replicator.loader;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;

public class MySQLLoader extends JdbcLoader
{
    private static Logger logger = Logger.getLogger(MySQLLoader.class);

    /**
     * Creates a new <code>MySQLLoader</code> object
     */
    public MySQLLoader()
    {
        super();

        setLockTables(true);
    }

    /**
     * Build a MySQL JDBC connection string {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.loader.JdbcLoader#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        if (url == null)
        {
            StringBuffer sb = new StringBuffer();
            sb.append("jdbc:mysql:thin://");
            sb.append(uri.getHost());
            if (uri.getPort() > 0)
            {
                sb.append(":");
                sb.append(uri.getPort());
            }
            sb.append("/");
            if (uri.getPath() != null)
                sb.append(uri.getPath());
            if (uri.getQuery() != null)
                sb.append(uri.getQuery());

            url = sb.toString();
        }
        else if (logger.isDebugEnabled())
            logger.debug("Property url already set; ignoring host and port properties");

        super.configure(context);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.loader.JdbcLoader#lockTables()
     */
    public void lockTables() throws SQLException
    {
        logger.info("Run FLUSH TABLES to lock out changes");
        statement.execute("FLUSH NO_WRITE_TO_BINLOG TABLES WITH READ LOCK");
    }

    /**
     * {@inheritDoc}
     * 
     * @throws SQLException
     * @see com.continuent.tungsten.replicator.loader.JdbcLoader#buildCreateSchemaStatement()
     */
    protected String buildCreateSchemaStatement() throws ReplicatorException
    {
        ResultSet createSchemaResult = null;

        try
        {
            createSchemaResult = statement
                    .executeQuery("SHOW CREATE DATABASE IF NOT EXISTS `"
                            + importTables.getString("TABLE_SCHEM") + "`");
            if (createSchemaResult.first() != true)
            {
                throw new ReplicatorException(
                        "Unable to extract the CREATE DATABASE statement for "
                                + importTables.getString("TABLE_SCHEM"));
            }

            return createSchemaResult.getString("Create Database");
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
        finally
        {
            if (createSchemaResult != null)
            {
                try
                {
                    createSchemaResult.close();
                }
                catch (SQLException e)
                {
                    throw new ReplicatorException(e);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @throws SQLException
     * @see com.continuent.tungsten.replicator.loader.JdbcLoader#buildCreateTableStatement()
     */
    protected String buildCreateTableStatement() throws ReplicatorException
    {
        ResultSet createTableResult = null;

        try
        {
            createTableResult = statement.executeQuery("SHOW CREATE TABLE `"
                    + importTables.getString("TABLE_SCHEM") + "`.`"
                    + importTables.getString("TABLE_NAME") + "`");
            if (createTableResult.first() != true)
            {
                throw new ReplicatorException(
                        "Unable to extract the CREATE TABLE statement for "
                                + importTables.getString("TABLE_SCHEM") + "."
                                + importTables.getString("TABLE_NAME"));
            }

            return createTableResult.getString("Create Table");
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
        finally
        {
            if (createTableResult != null)
            {
                try
                {
                    createTableResult.close();
                }
                catch (SQLException e)
                {
                    throw new ReplicatorException(e);
                }
            }
        }
    }

    /**
     * Use the output of SHOW MASTER STATUS to get the extractor Event ID
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#getCurrentResourceEventId()
     */
    @Override
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        ResultSet masterStatus = null;
        String currentResourceEventId = super.getCurrentResourceEventId();

        if (currentResourceEventId == null)
        {
            try
            {
                masterStatus = statement.executeQuery("SHOW MASTER STATUS");

                if (masterStatus.next())
                {
                    String fileName = masterStatus.getString("File");
                    int dotIndex = fileName.indexOf('.');
                    if (dotIndex == -1)
                    {
                        throw new ReplicatorException(
                                "There was a problem parsing the MASTER STATUS filename");
                    }

                    currentResourceEventId = fileName.substring(dotIndex + 1)
                            + ":" + masterStatus.getString("Position");
                }
                else
                {
                    throw new ReplicatorException(
                            "Unable to determine the current event id");
                }
            }
            catch (SQLException e)
            {
                throw new ReplicatorException(e);
            }
            finally
            {
                try
                {
                    masterStatus.close();
                }
                catch (SQLException e)
                {
                    throw new ReplicatorException(e);
                }
            }
        }

        return currentResourceEventId;
    }
}
