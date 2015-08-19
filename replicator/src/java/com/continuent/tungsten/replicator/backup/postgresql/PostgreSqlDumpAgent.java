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

package com.continuent.tungsten.replicator.backup.postgresql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;

import com.continuent.tungsten.replicator.backup.AbstractBackupAgent;
import com.continuent.tungsten.replicator.backup.BackupCapabilities;
import com.continuent.tungsten.replicator.backup.BackupException;
import com.continuent.tungsten.replicator.backup.BackupLocator;
import com.continuent.tungsten.replicator.backup.BackupSpecification;
import com.continuent.tungsten.replicator.backup.FileBackupLocator;
import com.continuent.tungsten.replicator.util.ProcessHelper;

/**
 * Implements a backup agent that works using pg_dump to dump data and
 * pg_restore to restore.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class PostgreSqlDumpAgent extends AbstractBackupAgent
{
    // Backup parameters.
    private String   host               = "localhost";
    private int      port               = 5432;
    private String   user               = "postgres";
    private String   password           = "";
    private String   dumpDirName        = "/tmp";
    private String   pgdumpOptions      = "-Fc";
    private String   pgrestoreOptions   = "-Fc";
    private String   ignoreDatabaseList = "postgres template0 template1";
    private String   databaseToConnect  = "template1";
    private boolean  hotBackupEnabled   = true;
    private String   url                = null;
    private String   driver             = "org.postgresql.Driver";

    // Private data.
    private File     dumpDir;
    private String   dbSelect;
    private String[] pgdumpCommandArray;
    private String[] pgrestoreCommandArray;
    private String[] dropDBCommandArray;
    private String[] createDBCommandArray;

    /**
     * Creates a new <code>PostgreSqlDumpAgent</code> object
     */
    public PostgreSqlDumpAgent()
    {
    }

    /**
     * Sets the host to be used while backuping / restoring to the given value.
     * 
     * @param host the host name or ip to be used
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * Sets the port to be used while connecting to the database.
     * 
     * @param port the port number
     */
    public void setPort(int port)
    {
        this.port = port;
    }

    /**
     * Sets the user to be used while connecting to the database.
     * 
     * @param user user name to be used while connecting to the database
     */
    public void setUser(String user)
    {
        this.user = user;
    }

    /**
     * Sets the password to be used while connecting to the database.
     * 
     * @param password the password to be used
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Sets the directory name where dumps should be stored.
     * 
     * @param dumpDirName a path to the directory
     */
    public void setDumpDir(String dumpDirName)
    {
        this.dumpDirName = dumpDirName;
    }

    /**
     * Sets the list of databases that should not be dumped by a backup
     * operation.
     * 
     * @param ignoreDatabaseList Space separated list of databases that will be
     *            ignored
     */
    public void setIgnoreDatabaseList(String ignoreDatabaseList)
    {
        this.ignoreDatabaseList = ignoreDatabaseList;
    }

    /**
     * Sets the database to be used while connecting. This database will then be
     * used to get the list of all databases hosted on this database backend.
     * 
     * @param databaseToConnect the database name
     */
    public void setDatabaseToConnect(String databaseToConnect)
    {
        this.databaseToConnect = databaseToConnect;
    }

    /**
     * Sets options to use for pg_dump.
     */
    public void setPgdumpOptions(String pgdumpOptions)
    {
        this.pgdumpOptions = pgdumpOptions;
    }

    /**
     * Sets options to provide pg_restore utility.
     */
    public void setPgrestoreOptions(String pgrestoreOptions)
    {
        this.pgrestoreOptions = pgrestoreOptions;
    }

    /**
     * If true, hot backup is enabled.
     */
    public void setHotBackupEnabled(boolean hotBackupEnabled)
    {
        this.hotBackupEnabled = hotBackupEnabled;
    }

    /**
     * Sets the URL used to connect to the PostgreSQL instance. If unset, this
     * value is generated automatically.
     */
    public void setUrl(String url)
    {
        this.url = url;
    }

    /**
     * Set the PostgreSQL JDBC driver name. If unset this defaults to the usual
     * PostgreSQL driver.
     */
    public void setDriver(String driver)
    {
        this.driver = driver;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.AbstractBackupAgent#backup()
     */
    public BackupSpecification backup() throws BackupException
    {
        BackupSpecification spec = new BackupSpecification();
        spec.setBackupDate(new Date());

        File dumpFile = null;
        FileWriter fw = null;

        try
        {
            ArrayList<String> databaseList = new ArrayList<String>();
            Connection conn = null;
            try
            {
                logger.info("Fetching list of databases to dump");
                conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(dbSelect);
                while (rs.next())
                {
                    databaseList.add(rs.getString(1));
                }
            }
            finally
            {
                releaseConnection(conn);
            }

            // For each database, run pg_dump command
            for (String databaseName : databaseList)
            {
                databaseName = databaseName.trim();
                if (databaseName.length() == 0)
                    continue;

                // Create temp file and add it with commands to turn off
                // logging.
                dumpFile = File.createTempFile("postgresqldump-" + databaseName
                        + "-", ".sql", dumpDir);
                logger.info("Selecting temp file for database dump: "
                        + dumpFile.getAbsolutePath());

                logger.warn("Dumping database " + databaseName);
                processHelper.exec("Dumping database using pgdump",
                        processHelper.mergeArrays(pgdumpCommandArray,
                                new String[]{databaseName}), null, dumpFile,
                        null, true, false);

                spec.addBackupLocator(new FileBackupLocator(databaseName,
                        dumpFile, true));
            }
        }
        catch (BackupException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new BackupException("Unexpected error on backup: "
                    + e.getMessage(), e);
        }
        finally
        {
            if (fw != null)
            {
                try
                {
                    fw.close();
                }
                catch (IOException e)
                {
                }
            }
        }
        return spec;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.AbstractBackupAgent#restoreOneLocator(com.continuent.tungsten.replicator.backup.BackupLocator)
     */
    protected void restoreOneLocator(BackupLocator locator)
            throws BackupException, FileNotFoundException
    {
        FileInputStream fis = null;

        String databaseName = locator.getDatabaseName();
        logger.info("Dropping database " + databaseName + " before restoring");
        try
        {
            processHelper.exec("Dropping database " + databaseName,
                    processHelper.mergeArrays(dropDBCommandArray,
                            new String[]{databaseName}));
        }
        catch (BackupException e)
        {
            // Just log and let go : an error here could mean that the
            // database did not exist. If this failed for another
            // reason, the next step will fail.
            logger.warn("Failed to drop " + databaseName + " database", e);
        }

        logger.info("Creating database " + databaseName + " before restoring");
        processHelper.exec("Creating database " + databaseName, processHelper
                .mergeArrays(createDBCommandArray, new String[]{databaseName}));

        // Execute pgrestore utility to restore.
        logger.info("Restoring database file: "
                + locator.getContents().getAbsolutePath());

        try
        {
            fis = new FileInputStream(locator.getContents());
            processHelper.exec("Restoring database " + databaseName
                    + " using pg_restore", processHelper.mergeArrays(
                    pgrestoreCommandArray, new String[]{"-d" + databaseName}),
                    fis, null, null, false, false);
        }
        finally
        {
            if (fis != null)
            {
                try
                {
                    fis.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#configure()
     */
    public void configure() throws BackupException
    {
        // Configure process helper. We don't need a command prefix as
        // pgdump/pgrestore can run using any login.
        processHelper = new ProcessHelper();
        processHelper.configure();

        // Ensure the dump directory exists.
        dumpDir = new File(dumpDirName);
        if (dumpDir.canRead() && dumpDir.isDirectory())
            logger.info("Setting dump directory for backups: "
                    + dumpDir.getAbsolutePath());
        else
            throw new BackupException(
                    "Dump directory not found or not readable: "
                            + dumpDir.getAbsolutePath());

        // Create the pg_dump command.
        String[] pgdumpBase = {"pg_dump", "-U" + user, "-h" + host, "-p" + port};

        pgdumpCommandArray = processHelper.mergeArrays(pgdumpBase,
                pgdumpOptions.split("\\s"));

        // Generate URL.
        if (url == null)
        {
            StringBuffer sb = new StringBuffer();
            sb.append("jdbc:postgresql://");
            sb.append(host);
            if (port > 0)
            {
                sb.append(":");
                sb.append(port);
            }
            sb.append("/");
            sb.append(databaseToConnect);

            url = sb.toString();
        }

        // Create the psql command used to get a list of databases.
        dbSelect = "SELECT datname FROM pg_catalog.pg_database WHERE datname not in ("
                + join(ignoreDatabaseList.split("\\s"), ",") + ")";

        // Create the drop database command
        dropDBCommandArray = new String[]{"dropdb", "-U" + user, "-h" + host,
                "-p" + port};

        // Create the create database command
        createDBCommandArray = new String[]{"createdb", "-U" + user,
                "-h" + host, "-p" + port};

        // Create the pg_restore command.
        String[] pgrestoreBase = {"pg_restore", "-U" + user, "-h" + host,
                "-p" + port};
        pgrestoreCommandArray = processHelper.mergeArrays(pgrestoreBase,
                pgrestoreOptions.split("\\s"));

        // Define capabilities.
        capabilities = new BackupCapabilities();
        capabilities.setHotBackupEnabled(hotBackupEnabled);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#release()
     */
    public void release() throws BackupException
    {
        // Nothing to do!
    }

    /**
     * join concatenates every String objects found in a String array, separated
     * by the given delimiter
     * 
     * @param s the string array
     * @param delimiter the delimiter
     * @return a string containing the concatenation of every string object
     *         found in the array, separated by the given delimiter
     */
    private String join(String[] s, String delimiter)
    {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < s.length; i++)
        {
            buffer.append("'");
            buffer.append(s[i]);
            buffer.append("'");
            if (i < s.length - 1)
            {
                buffer.append(delimiter);
            }
        }
        return buffer.toString();
    }

    /**
     * Fetch connection to PostgreSQL.
     */
    private Connection getConnection() throws BackupException
    {
        try
        {
            Class.forName(driver);
            return DriverManager.getConnection(url, user, password);
        }
        catch (ClassNotFoundException e)
        {
            throw new BackupException("Unable to load JDBC driver: " + driver,
                    e);
        }
        catch (SQLException e)
        {
            throw new BackupException("Unable to connect to database: " + url,
                    e);
        }
    }

    /**
     * Release a fetched connection safely
     */
    private void releaseConnection(Connection conn)
    {
        if (conn != null)
            try
            {
                conn.close();
            }
            catch (SQLException e)
            {
                logger.warn("Unable to close database connection: " + url);
            }
    }
}