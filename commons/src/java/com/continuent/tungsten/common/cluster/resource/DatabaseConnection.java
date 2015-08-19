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
 * Initial developer(s): Edward Archibald
 * Contributor(s): Gilles Rayrat
 */

package com.continuent.tungsten.common.cluster.resource;

import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

import com.continuent.tungsten.common.patterns.order.Sequence;
import com.continuent.tungsten.common.utils.CLUtils;

public class DatabaseConnection
{
    public enum ConnectionType
    {
        DIRECT, CLUSTER, CONNECTOR, BRIDGED
    };

    private ConnectionType type     = ConnectionType.DIRECT;
    private String         name;
    private Connection     connection;
    private Sequence       sequence = null;
    private DataSource     ds       = null;
    private Object         context;
    
    /** Last time the connection was used */
    private Date lastUsed = new Date();

    public DatabaseConnection(ConnectionType type, String name,
            Connection connection, DataSource ds, Object context)
    {
        this.type = type;
        this.name = name;
        this.connection = connection;
        this.ds = ds;
        setContext(context);
    }

    public ConnectionType getType()
    {
        return type;
    }

    public void setType(ConnectionType type)
    {
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Connection getConnection()
    {
        return connection;
    }

    public Object getContext()
    {
        return context;
    }

    public void setContext(Object context)
    {
        this.context = context;

        if (ds != null)
            this.sequence = ds.getSequence();
    }

    public Sequence getSequence()
    {
        return sequence;
    }

    public void setSequence(Sequence sequence)
    {
        this.sequence = sequence;
    }

    public DataSource getDs()
    {
        return ds;
    }
    
    /**
     * Returns the lastUsed value.
     * 
     * @return Returns the lastUsed.
     */
    public Date getLastUsed()
    {
        return lastUsed;
    }

    /**
     * Updates the lastUsed value to the current Date/Time.
     * 
     */
    public void touch()
    {
        this.lastUsed = new Date();
    }

    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean detailed)
    {
        if (type == ConnectionType.DIRECT)
        {
            return String.format("%s(%s) DIRECT TO %s", name, liveness(),
                    ds.toString());
        }
        else if (type == ConnectionType.CLUSTER)
        {
            StringBuilder builder = new StringBuilder();
            builder.append(name).append('(').append(liveness())
                    .append(") VIA CLUSTER TO ").append(connection.toString());
            return builder.toString();
        }
        else if (type == ConnectionType.CONNECTOR)
        {
            return String.format("%s(%s) CONNECTOR TO HOST %s", name,
                    liveness(), getContext());
        }
        else if (type == ConnectionType.BRIDGED)
        {
            return name + " BRIDGED to " + ds != null ? ds.getName() : "null";
        }
        else
        {
            CLUtils.println(String.format(
                    "no connection status logic for type %s", type));
            return "UNKNOWN";
        }
    }

    /**
     * Provides a string representation of whether this connection is closed or
     * not
     * 
     * @return "CLOSED" if the wrapped JDBC connection is closed, "OPEN"
     *         otherwise
     */
    private String liveness()
    {
        boolean isClosed = false;

        try
        {
            if (isClosed())
            {
                isClosed = true;
            }
        }
        catch (SQLException s)
        {
            isClosed = true;
        }

        if (isClosed)
        {
            return "CLOSED";
        }

        return "OPEN";
    }

    public boolean isClosed() throws SQLException
    {
        if (type == ConnectionType.BRIDGED)
        {
            return ((Socket) context).isClosed();
        }
        return connection.isClosed();
    }
}
