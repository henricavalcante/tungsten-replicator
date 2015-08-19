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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.datasource;

/**
 * Implements properties common to all SQL connections including boilerplate
 * accessor code required by Java.
 */
public class SqlConnectionSpecGeneric implements SqlConnectionSpec
{
    // Properties.
    protected String  vendor;
    protected String  user;
    protected String  password;
    protected String  host;
    protected int     port;
    protected String  tableType;
    protected boolean sslEnabled;
    protected String  schema;
    protected String  initScript;

    // Url may be specified or generated.
    protected String  url;

    /** Generic constructor to make this fit bean semantics. */
    public SqlConnectionSpecGeneric()
    {
    }

    /**
     * Returns the DBMS login.
     */
    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    /**
     * Returns the DBMS password.
     */
    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Returns the DBMS host.
     */
    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * Returns the DBMS port.
     */
    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    /**
     * Returns the DBMS table type.
     */
    public String getTableType()
    {
        return tableType;
    }

    public void setTableType(String tableType)
    {
        this.tableType = tableType;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.SqlConnectionSpec#getVendor()
     */
    public String getVendor()
    {
        return vendor;
    }

    public void setVendor(String vendor)
    {
        this.vendor = vendor;
    }

    /**
     * Returns the DBMS schema for catalog tables.
     */
    public String getSchema()
    {
        return schema;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    /**
     * If true, connection must be SSL-enabled.
     */
    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
    }

    /** Sets an optional URL. Subclasses may compute this if it is absent. */
    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getUrl()
    {
        return url;
    }

    /** Sets an optional script to execute at connection time. */
    public String getInitScript()
    {
        return initScript;
    }

    public void setInitScript(String initScript)
    {
        this.initScript = initScript;
    }

    /**
     * Returns true if this URL type supports an option to create DB
     * automatically.
     */
    public boolean supportsCreateDB()
    {
        return false;
    }

    /**
     * Returns a JDBC URL that can be used for connections.
     * 
     * @param createDB Add an option to create database automatically if
     *            supported by this URL type
     */
    public String createUrl(boolean createDB)
    {
        return getUrl();
    }
}