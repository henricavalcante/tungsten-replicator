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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.RawExtractor;

/**
 * This class defines the base class for all THL Loader extractor classes. All
 * classes that will be used with the loader must extend this.
 * 
 * @author <a href="mailto:jeff.mace@continuent.com">Jeff Mace</a>
 * @version 1.0
 */
public abstract class Loader implements RawExtractor
{
    private static final int            DEFAULT_CHUNK_SIZE = 500;

    private static Logger               logger             = Logger.getLogger(Loader.class);

    protected URI                       uri                = null;
    protected Map<String, List<String>> params             = null;
    protected int                       chunkSize          = DEFAULT_CHUNK_SIZE;
    protected boolean                   lockTables         = false;

    /**
     * Parse the URI to extract events from
     * 
     * @param uri
     * @throws Exception
     */
    public void setUri(String uri) throws Exception
    {
        try
        {
            logger.debug("Load from " + uri);

            this.uri = new URI(uri);

            params = new HashMap<String, List<String>>();

            if (this.uri.getQuery() != null)
            {
                if (this.uri.getQuery().length() > 0)
                {
                    for (String param : this.uri.getQuery().split("&"))
                    {
                        String pair[] = param.split("=");
                        String key = URLDecoder.decode(pair[0], "UTF-8");
                        String value = "";
                        if (pair.length > 1)
                        {
                            value = URLDecoder.decode(pair[1], "UTF-8");
                        }
                        List<String> values = params.get(key);
                        if (values == null)
                        {
                            values = new ArrayList<String>();
                            params.put(key, values);
                        }
                        values.add(value);
                    }
                }
            }
        }
        catch (UnsupportedEncodingException uee)
        {
            throw new Exception("Unable to decode a parameter in "
                    + uri.toString());
        }
    }

    /**
     * Set the number of rows to include in each THL event
     * 
     * @param chunkSize
     */
    public void setChunkSize(int chunkSize)
    {
        this.chunkSize = chunkSize;
    }

    /**
     * Get the number of rows to include in each THL event
     */
    public int getChunkSize()
    {
        return this.chunkSize;
    }

    /**
     * Build an event that includes the heartbeat name to indicate that all data
     * has been extracted
     * 
     * @return The DBMSEvent with the heartbeat metadata set
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    protected DBMSEvent getFinishLoadEvent() throws ReplicatorException,
            InterruptedException
    {
        DBMSEmptyEvent heartbeat = new DBMSEmptyEvent(
                this.getCurrentResourceEventId(), new Timestamp(
                        System.currentTimeMillis()));
        heartbeat
                .setMetaDataOption(ReplOptionParams.HEARTBEAT, "LOAD_COMPLETE");
        return heartbeat;
    }

    /**
     * Set if the tables should be locked at runtime
     * 
     * @param lockTables
     */
    public void setLockTables(boolean lockTables)
    {
        this.lockTables = lockTables;
    }

    /**
     * Are locks required on the tables during the load?
     */
    public boolean getLockTables()
    {
        return lockTables;
    }

    /**
     * Take a raw string value and return the proper Java data type for the
     * java.sql.Types type given
     * 
     * @param type
     * @param value
     * @throws Exception
     */
    public Serializable parseStringValue(int type, String value)
            throws Exception
    {
        switch (type)
        {
            case java.sql.Types.BIT :
            case java.sql.Types.BOOLEAN :
            {
                return new Boolean(value);
            }

            case java.sql.Types.CHAR :
            case java.sql.Types.VARCHAR :
            case java.sql.Types.LONGVARCHAR :
            case java.sql.Types.NCHAR :
            case java.sql.Types.NVARCHAR :
            case java.sql.Types.LONGNVARCHAR :
            case java.sql.Types.NCLOB :
            case java.sql.Types.CLOB :
            {
                return value;
            }

            case java.sql.Types.TINYINT :
            case java.sql.Types.SMALLINT :
            case java.sql.Types.INTEGER :
            {
                return new Integer(value);
            }

            case java.sql.Types.BIGINT :
            {
                return new Long(value);
            }

            case java.sql.Types.FLOAT :
            case java.sql.Types.DOUBLE :
            {
                return new Double(value);
            }

            case java.sql.Types.REAL :
            {
                return new Float(value);
            }

            case java.sql.Types.DECIMAL :
            case java.sql.Types.NUMERIC :
            {
                return new java.math.BigDecimal(value);
            }

            case java.sql.Types.TIMESTAMP :
            {
                return java.sql.Timestamp.valueOf(value);
            }

            case java.sql.Types.DATE :
            {
                return java.sql.Date.valueOf(value);
            }

            case java.sql.Types.TIME :
            {
                return java.sql.Time.valueOf(value);
            }

            case java.sql.Types.BINARY :
            case java.sql.Types.VARBINARY :
            case java.sql.Types.LONGVARBINARY :
            case java.sql.Types.BLOB :
            {
                throw new Exception(
                        "THL loader does not yet support binary data");
            }

            case java.sql.Types.NULL :
            case java.sql.Types.OTHER :
            case java.sql.Types.JAVA_OBJECT :
            case java.sql.Types.DISTINCT :
            case java.sql.Types.STRUCT :
            case java.sql.Types.ARRAY :
            case java.sql.Types.REF :
            case java.sql.Types.DATALINK :
            case java.sql.Types.ROWID :
            case java.sql.Types.SQLXML :
            {
                throw new Exception("unsupported data type " + type);
            }

            default :
            {
                throw new Exception("unknown data type " + type);
            }
        }
    }
}
