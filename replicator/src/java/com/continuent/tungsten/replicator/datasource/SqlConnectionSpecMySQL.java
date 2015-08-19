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

package com.continuent.tungsten.replicator.datasource;

/**
 * Denotes a class that can generate URLs for a specific DBMS type.
 */
public class SqlConnectionSpecMySQL extends SqlConnectionSpecGeneric
{
    // Extra properties supported by MySQL connections.
    protected String jdbcHeader;
    protected String urlOptions;

    /**
     * Instantiate URLa specification for MySQL with InnoDB as default table type.
     */
    public SqlConnectionSpecMySQL()
    {
        this.tableType = "InnoDB";
    }

    /**
     * Returns the JDBC URL header, e.g., a prefix like "jdbc:mysql:thin://" or
     * null if we are to use a default.
     */
    public String getJdbcHeader()
    {
        return jdbcHeader;
    }

    public void setJdbcHeader(String jdbcHeader)
    {
        this.jdbcHeader = jdbcHeader;
    }

    /**
     * Returns extra URL options added at the discretion of clients.
     */
    public String getUrlOptions()
    {
        return urlOptions;
    }

    public void setUrlOptions(String urlOptions)
    {
        this.urlOptions = urlOptions;
    }

    /**
     * Indicate that MySQL can create database from the URL.
     */
    @Override
    public boolean supportsCreateDB()
    {
        return true;
    }

    /**
     * Generates a MySQL URL with or without the createDB=true option. This
     * option should *only* be used the first time we connect.
     */
    public String createUrl(boolean createDB)
    {
        // If we have an URL already just use that.
        if (url != null)
            return url;

        // Otherwise compute the MySQL DBMS URL.
        StringBuffer sb = new StringBuffer();
        if (jdbcHeader == null)
            sb.append("jdbc:mysql:thin://");
        else
            sb.append(jdbcHeader);
        sb.append(host);
        sb.append(":");
        sb.append(port);
        sb.append("/");
        sb.append(schema);
        if (urlOptions != null && urlOptions.length() > 0)
        {
            // Prepend ? if needed to make the URL options syntactically
            // correct, then add the option string.
            if (!urlOptions.startsWith("?"))
                sb.append("?");
            sb.append(urlOptions);

            if (createDB)
            {
                sb.append("&createDB=true");
            }

            if (sslEnabled)
            {
                sb.append("&useSSL=true");
            }
        }
        else if (createDB)
        {
            sb.append("?createDB=true");
            if (sslEnabled)
            {
                sb.append("&useSSL=true");
            }
        }
        else if (sslEnabled)
        {
            sb.append("?useSSL=true");
        }
        return sb.toString();
    }
}