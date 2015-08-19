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
public class SqlConnectionSpecOracle extends SqlConnectionSpecGeneric
{
    // Extra properties supported by MySQL connections.
    protected String jdbcHeader;
    protected String urlOptions;
    private String   serviceName = null;
    private String   sid         = null;

    public String getServiceName()
    {
        return serviceName;
    }

    /**
     * Instantiate URLa specification for MySQL with InnoDB as default table
     * type.
     */
    public SqlConnectionSpecOracle()
    {
        this.tableType = "CDC";
    }

    public void setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
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

    public String getSid()
    {
        return sid;
    }

    public void setSid(String sid)
    {
        this.sid = sid;
    }

    /**
     * Indicate that MySQL can create database from the URL.
     */
    @Override
    public boolean supportsCreateDB()
    {
        return false;
    }

    /**
     * Generates a MySQL URL with or without the createDB=true option. This
     * option should *only* be used the first time we connect.
     */
    public String createUrl(boolean createDB)
    {
        // jdbc:oracle:thin:@//${replicator.global.db.host}:${replicator.global.db.port}/${replicator.applier.oracle.service}
        // If we have an URL already just use that.
        boolean useService = serviceName != null && serviceName.trim().length() > 0;

        if (url != null)
            return url;

        // Otherwise compute the MySQL DBMS URL.
        StringBuffer sb = new StringBuffer();
        if (jdbcHeader == null)
            if (useService)
                sb.append("jdbc:oracle:thin:@//");
            else
                sb.append("jdbc:oracle:thin:@");

        else
            sb.append(jdbcHeader);
        sb.append(host);
        sb.append(":");
        sb.append(port);
        if (useService)
        {
            sb.append("/");
            sb.append(serviceName);
        }
        else if (sid != null && sid.length() > 0)
        {
            sb.append(":");
            sb.append(sid);
        }
        if (urlOptions != null && urlOptions.length() > 0)
        {
            // Prepend ? if needed to make the URL options syntactically
            // correct, then add the option string.
            if (!urlOptions.startsWith("?"))
                sb.append("?");
            sb.append(urlOptions);
        }
        return sb.toString();
    }
}