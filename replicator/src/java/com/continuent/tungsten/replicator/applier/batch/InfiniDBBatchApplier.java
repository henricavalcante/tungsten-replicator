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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.applier.batch;

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.csv.CsvInfo;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class InfiniDBBatchApplier extends SimpleBatchApplier
{

    private static Logger logger = Logger.getLogger(SimpleBatchApplier.class);

    /**
     * Needs to be done in a transaction, otherwise cpimport will be unable to
     * get the lock on the stage table !
     * 
     * @param info
     * @throws ReplicatorException
     */
    protected void clearStageTable(CsvInfo info) throws ReplicatorException
    {
        Statement tmpStatement = null;

        Table table = info.stageTableMetadata;
        if (logger.isDebugEnabled())
        {
            logger.debug("Clearing InfiniDB stage table: "
                    + table.fullyQualifiedName());
        }

        // Generate and submit SQL command.
        String delete = "DELETE FROM " + table.fullyQualifiedName();
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing delete command: " + delete);
        }
        try
        {
            Database conn = (Database) connections.get(0);
            tmpStatement = conn.createStatement();
            conn.setAutoCommit(false);
            int rowsLoaded = tmpStatement.executeUpdate(delete);
            conn.commit();
            conn.setAutoCommit(true);
            if (logger.isDebugEnabled())
            {
                logger.debug("Rows deleted: " + rowsLoaded);
            }
        }
        catch (Exception e)
        {
            ReplicatorException re = new ReplicatorException(
                    "Unable to delete data from stage table: "
                            + table.fullyQualifiedName(),
                    e);
            re.setExtraData(delete);
            throw re;
        }
        finally
        {
            if (tmpStatement != null)
                try
                {
                    tmpStatement.close();
                }
                catch (SQLException e)
                {
                }
        }
    }
}
