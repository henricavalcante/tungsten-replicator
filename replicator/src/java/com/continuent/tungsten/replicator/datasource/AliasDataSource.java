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

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements an alias data source, which denotes a data source that is actually
 * an alias for another data source. This is used to allow symmetric definitions
 * of configuration properties without having to define data source multiple
 * times.
 */
public class AliasDataSource extends AbstractDataSource
        implements
            UniversalDataSource
{
    public String dataSource;

    /** Create new instance. */
    public AliasDataSource()
    {
    }

    public String getDataSource()
    {
        return dataSource;
    }

    public void setDataSource(String dataSource)
    {
        this.dataSource = dataSource;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#configure()
     */
    public void configure() throws ReplicatorException, InterruptedException
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#prepare()
     */
    @Override
    public void prepare() throws ReplicatorException, InterruptedException
    {
        // Do nothing.
    }

    /**
     * Release all data source tables.
     */
    @Override
    public void release() throws ReplicatorException, InterruptedException
    {
        // Do nothing.
    }

    /**
     * Ensure all tables are ready for use, creating them if necessary.
     */
    @Override
    public void initialize() throws ReplicatorException, InterruptedException
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    public void reduce() throws ReplicatorException, InterruptedException
    {
        // Do nothing.
    }

    public boolean clear() throws ReplicatorException, InterruptedException
    {
        // Do nothing.
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getCommitSeqno()
     */
    @Override
    public CommitSeqno getCommitSeqno()
    {
        throw new UnsupportedOperationException(
                "Alias data sources do not support catalog operations");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getConnection()
     */
    public UniversalConnection getConnection() throws ReplicatorException
    {
        throw new UnsupportedOperationException(
                "Alias data sources do not support connection operations");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#releaseConnection(com.continuent.tungsten.replicator.datasource.UniversalConnection)
     */
    public void releaseConnection(UniversalConnection conn)
    {
        throw new UnsupportedOperationException(
                "Alias data sources do not support connection operations");
    }
}