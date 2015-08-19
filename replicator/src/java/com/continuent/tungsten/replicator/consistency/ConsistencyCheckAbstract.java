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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.consistency;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;

import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;

/**
 * This class defines a ConsistencyCheckAbstract
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public abstract class ConsistencyCheckAbstract implements ConsistencyCheck
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    protected int    id     = -1;
    protected Table  table  = null;
    protected String method = null;

    protected ConsistencyCheckAbstract(int id, Table table, String method)
    {
        this.id = id;
        this.table = table;
        this.method = method;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getCheckId()
     */
    public final int getCheckId()
    {
        return id;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getSchemaName()
     */
    public final String getSchemaName()
    {
        return table.getSchema();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getTableName()
     */
    public final String getTableName()
    {
        return table.getName();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getRowOffset()
     */
    public int getRowOffset()
    {
        return ConsistencyTable.ROW_UNSET;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getRowLimit()
     */
    public int getRowLimit()
    {
        return ConsistencyTable.ROW_UNSET;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getMethod()
     */
    public final String getMethod()
    {
        return method;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#performConsistencyCheck(com.continuent.tungsten.replicator.database.Database)
     */
    public abstract ResultSet performConsistencyCheck(Database conn)
            throws ConsistencyException;

    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("ID: ");
        sb.append(id);
        sb.append("; TABLE: ");
        sb.append(table.getSchema());
        sb.append('.');
        sb.append(table.getName());
        sb.append(", LIMITS: ");
        sb.append(getRowOffset());
        sb.append(", ");
        sb.append(getRowLimit());
        return sb.toString();
    }

    // serialization stuff
    private static final Object          serializer = new Object();
    private static ByteArrayOutputStream bos        = new ByteArrayOutputStream();
    private static ObjectOutputStream    oos;
    static
    {
        try
        {
            oos = new ObjectOutputStream(bos);
        }
        catch (IOException e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    public byte[] serialize() throws ConsistencyException
    {
        byte[] ret = null;
        synchronized (serializer)
        {
            bos.reset();
            try
            {
                oos.reset();
                oos.writeObject(this);
                oos.flush();
            }
            catch (IOException e)
            {
                throw new ConsistencyException(
                        "Failed to serialize ConsistencyCheck object: "
                                + e.getMessage(), e);
            }
            ret = bos.toByteArray();
        }
        return ret;
    }

    public static ConsistencyCheck deserialize(byte[] bytes)
            throws ConsistencyException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try
        {
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object obj = ois.readObject();
            if (obj instanceof ConsistencyCheck)
            {
                return (ConsistencyCheck) ois.readObject();
            }
            throw new ConsistencyException(
                    "This is not a ConsistencyCheck object.");
        }
        catch (Exception e)
        {
            throw new ConsistencyException(
                    "Failed to deserialize ConsistencyCheck object:"
                            + e.getMessage(), e);
        }
    }
}
