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
 * Initial developer(s): Marcus Eriksson
 * Contributor(s): Robert Hodges
 * INITIAL CODE DONATED UNDER TUNGSTEN CODE CONTRIBUTION AGREEMENT
 */

package com.continuent.tungsten.replicator.applier;

import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.RowIdData;

/**
 * Stub applier class that automatically constructs url from Drizzle-specific
 * properties like host, port, and service.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @author Marcus Eriksson
 * @version 1.0
 */
public class DrizzleApplier extends JdbcApplier
{
    private static Logger logger     = Logger.getLogger(DrizzleApplier.class);

    protected String      host       = "localhost";
    protected int         port       = 4427;
    protected String      urlOptions = null;

    /**
     * Host name or IP address.
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * TCP/IP port number, a positive integer.
     */
    public void setPort(String portAsString)
    {
        this.port = Integer.parseInt(portAsString);
    }

    /**
     * JDBC URL options with a leading ?.
     */
    public void setUrlOptions(String urlOptions)
    {
        this.urlOptions = urlOptions;
    }

    protected void applyRowIdData(RowIdData data) throws ReplicatorException
    {
        String query = "SET INSERT_ID = " + data.getRowId();
        try
        {
            try
            {
                statement.execute(query);
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
            }
            statement.clearBatch();

            if (logger.isDebugEnabled())
            {
                logger.debug("Applied event: " + query);
            }
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(query, e);
            throw new ApplierException(e);
        }
    }

}