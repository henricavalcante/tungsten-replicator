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

import java.util.TimeZone;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.csv.CsvDataFormat;

/**
 * Denotes a generic data source that a replicator may connect at either end of
 * a pipeline. Data source implementations encapsulate the following data:
 * <ul>
 * <li>Replicator catalogs, which consist of a set of "tables" that hold
 * metadata used to control replication. Data sources may implement such tables
 * using relational tables, files, or any other suitable means.</li>
 * <li>Connection manager, which parcels out connections to the data source, be
 * this a JDBC connection, a MongoDB connection, a connection to HDFS, etc.</li>
 * </ul>
 * All data required for operation must be provided through property setters.
 * Data sources do not implement the ReplicatorPlugin lifecycle or access the
 * PluginContext implementation as this introduces dependencies that prevent
 * easy testing and hurt portability between store types.
 * 
 * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin
 * @see com.continuent.tungsten.replicator.plugin.PluginContext
 */
public interface UniversalDataSource extends CatalogEntity
{
    /** Set the data source name. */
    public void setName(String name);

    /** Return the data source name. */
    public String getName();

    /**
     * Set the name of the replicator service that is using this data source.
     */
    public void setServiceName(String serviceName);

    /**
     * Return the name of the replicator service that is using this data source.
     */
    public String getServiceName();

    /**
     * Set the number of channels to use when applying to the data source. This
     * is the basic mechanism to support parallel replication for relational
     * DBMS systems. It should be set according to the role of the pipeline
     * within a data source.
     * <ol>
     * <li>Master - Channels should always be set to 1.</li>
     * <li>Slave or relay - Channels set to number of apply threads</li>
     * <li>Other role - Channels set to 1</li>
     * </ol>
     */
    public void setChannels(int channels);

    /**
     * Return the number of channels to track.
     */
    public int getChannels();

    /**
     * Returns a ready-to-use CommitSeqno instance for operations on commit
     * seqno data.
     */
    public CommitSeqno getCommitSeqno();

    /**
     * Returns a ready-to-use wrapped connection for operations on the data
     * source.
     */
    public UniversalConnection getConnection() throws ReplicatorException;

    /**
     * Releases a wrapped connection.
     */
    public void releaseConnection(UniversalConnection conn);

    /**
     * Returns a configured formatter for writing data to CSV files for this
     * particular data source type.
     * 
     * @param tz A time zone to use in date/time conversions
     */
    public CsvDataFormat getCsvStringFormatter(TimeZone tz)
            throws ReplicatorException;

    /**
     * Ensures all data source catalog data are cleaned up prior to going
     * offline. This should always be called when taking a replication service
     * offline to ensure restart points and channel assignments are properly
     * reduced to allow reconfiguration.
     */
    public void reduce() throws ReplicatorException, InterruptedException;
}