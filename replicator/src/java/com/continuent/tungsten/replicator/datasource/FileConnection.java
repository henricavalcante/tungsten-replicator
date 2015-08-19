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
 * Contributor(s): Robert Hodges, Stephane Giron, Linas Virbalas
 */

package com.continuent.tungsten.replicator.datasource;

import java.io.BufferedWriter;

import com.continuent.tungsten.common.csv.CsvSpecification;
import com.continuent.tungsten.common.csv.CsvWriter;

/**
 * Implements a dummy connection for use with data sources that do not have
 * connections.
 */
public class FileConnection implements UniversalConnection
{
    private CsvSpecification csvSpecification;

    /**
     * Creates a new <code>FileConnection</code> object
     * 
     * @param csvSpecification
     */
    public FileConnection(CsvSpecification csvSpecification)
    {
        this.csvSpecification = csvSpecification;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#getCsvWriter(java.io.BufferedWriter)
     */
    public CsvWriter getCsvWriter(BufferedWriter writer)
    {
        return csvSpecification.createCsvWriter(writer);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#commit()
     */
    public void commit() throws Exception
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#rollback()
     */
    public void rollback() throws Exception
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#setAutoCommit(boolean)
     */
    public void setAutoCommit(boolean autoCommit) throws Exception
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#setLogged(boolean)
     */
    public void setLogged(boolean logged)
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#setPrivileged(boolean)
     */
    public void setPrivileged(boolean privileged)
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#close()
     */
    public void close()
    {
        // Do nothing.
    }
}