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
 * Javascript filter to issue a shard id by combining the db and table
 * so that each table belongs to a different shard.  Assigns #UNKNOWN
 * if the transaction contains more than one table. 
 *
 * replicator.filter.shardbytable=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.shardbytable.script=${replicator.home}/support/filters-javascript/shardbytable.js
 */

/**
 * Called on every filtered event. See replicator's javadoc for more details
 * on accessible classes. Also, JavaScriptFilter's javadoc contains description
 * about how to define a script like this.
 *
 * @param event Filtered com.continuent.tungsten.replicator.event.ReplDBMSEvent
 *
 * @see com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * @see com.continuent.tungsten.replicator.event.ReplDBMSEvent
 * @see com.continuent.tungsten.replicator.dbms.DBMSData
 * @see com.continuent.tungsten.replicator.dbms.StatementData
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData
 * @see com.continuent.tungsten.replicator.dbms.OneRowChange
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType
 * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#printRowChangeData(StringBuilder, RowChangeData, String, boolean, int)
 * @see java.lang.Thread
 * @see org.apache.log4j.Logger
 */

// Shard ID. 
shardId = null;

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    logger.info("shardbytable: Generating shard ID for each schema/table");
}

function filter(event)
{
    // Analyse what this event is holding.
    data = event.getData();

    // See if this is the first fragment of the transaction. 
    if (event.getFragno() == 0)
    {
        shardId = null;
    }

    // One ReplDBMSEvent may contain many DBMSData events.
    shardIds = new Array();
    proposedShardId = null;
    for(i=0;i<data.size();i++)
    {
        // Get com.continuent.tungsten.replicator.dbms.DBMSData
        d = data.get(i);

        // Determine the underlying type of DBMSData event.
        if(d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
        {
            proposedShardId = "#UNKNOWN";
        }
        else if(d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
        {
            // It's a row change event.
            rowChanges = data.get(i).getRowChanges();

            // One RowChangeData may contain many OneRowChange events.
            for(j=0;j<rowChanges.size();j++)
            {
                // Get com.continuent.tungsten.replicator.dbms.OneRowChange
                oneRowChange = rowChanges.get(j);
                schemaName = oneRowChange.getSchemaName();
                tableName  = oneRowChange.getTableName();

                id = schemaName + "_" + tableName;
                if (proposedShardId == null)
                {
                    proposedShardId = id;
                }
                else if (proposedShardId == id)
                {
                    // Do nothing. 
                }
                else 
                {
                    proposedShardId = "#UNKNOWN";
                }
                logger.debug("Proposed shard ID: " + proposedShardId);
            }
        }
    }

    // Set the shard ID.  A null value is converted to #UNKNOWN. 
    if (proposedShardId == null)
        proposedShardId = "#UNKNOWN";

    event.setShardId(proposedShardId);
}
