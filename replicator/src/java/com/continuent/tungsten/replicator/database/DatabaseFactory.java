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
 * Contributor(s): Marcus Eriksson, Linas Virbalas, Stephane Giron
 * DRIZZLE CODE DONATED UNDER TUNGSTEN CODE CONTRIBUTION AGREEMENT
 */

package com.continuent.tungsten.replicator.database;

import java.sql.SQLException;

/**
 * This class defines a DatabaseFactory
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class DatabaseFactory
{
    /**
     * Shorthand method to allocate a non-privileged connection with no vendor
     * identification.
     */
    static public Database createDatabase(String url, String user,
            String password) throws SQLException
    {
        return createDatabase(url, user, password, false, null);
    }

    /**
     * Shorthand method to allocate a privileged connection without vendor
     * identification.
     */
    static public Database createDatabase(String url, String user,
            String password, boolean privileged) throws SQLException
    {
        return createDatabase(url, user, password, privileged, null);
    }

    /**
     * Creates a new connection to a database.
     * 
     * @param url JDBC url
     * @param user Database loging
     * @param password Password for same
     * @param privileged If true, this account has SUPER/SYSDBA privileges. This
     *            may increase the capabilities of the account.
     * @param vendor Optional vendor string
     * @return A database connection instance
     * @throws SQLException Thrown if there is a failure creating the connection
     */
    static public Database createDatabase(String url, String user,
            String password, boolean privileged, String vendor)
            throws SQLException
    {
        Database database;
        if (url.startsWith("jdbc:drizzle"))
            database = new DrizzleDatabase();
        else if (url.startsWith("jdbc:mysql:thin"))
            database = new MySQLDrizzleDatabase();
        else if (url.startsWith("jdbc:mysql"))
            database = new MySQLDatabase();
        else if (url.startsWith("jdbc:oracle"))
            database = new OracleDatabase();
        else if (url.startsWith("jdbc:derby"))
            database = new DerbyDatabase();
        else if (url.startsWith("jdbc:postgresql")
                && (vendor == null || vendor.equals("postgresql")))
            database = new PostgreSQLDatabase();
        else if (url.startsWith("jdbc:postgresql")
                && (vendor != null && vendor.equals("greenplum")))
            database = new GreenplumDatabase();
        else if (url.startsWith("jdbc:postgresql")
                && (vendor != null && vendor.equals("redshift")))
            database = new RedshiftDatabase();
        else if (url.startsWith("jdbc:vertica"))
            database = new VerticaDatabase();
        else
            throw new RuntimeException("Unsupported URL type: " + url);

        database.setUrl(url);
        database.setUser(user);
        database.setPassword(password);
        database.setPrivileged(privileged);

        return database;
    }

}
