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
 * Denotes a class that can generate URLs for a specific DBMS type.
 */
public interface SqlConnectionSpec
{
    /** Returns the DBMS login. */
    public String getUser();

    /** Returns the password. */
    public String getPassword();

    /** Returns the DBMS schema for catalog tables. */
    public String getSchema();

    /**
     * Returns vendor for some DBMS types which share the same URL beginning
     * (eg. PostgreSQL, Greenplum and Redshift). Can be null if not applicable.
     */
    public String getVendor();

    /** Returns the DBMS table type. This is a MySQL option. */
    public String getTableType();

    /** Returns an optional connect script to run at connect time. */
    public String getInitScript();

    /**
     * Returns true if this URL type supports an option to create DB
     * automatically.
     */
    public boolean supportsCreateDB();

    /**
     * Returns a URL to connect to the DBMS to which this specification applies.
     * 
     * @param createDB If true add option to create schema used by URL on
     *            initial connect. Ignored for DBMS types that do not support
     *            such an option.
     */
    public String createUrl(boolean createDB);
}