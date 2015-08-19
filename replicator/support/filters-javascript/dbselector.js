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
 * Allows one to select a specific schema to replicate while ignoring 
 * others on the slave. This way one can configure multiple slaves to
 * replicate different schemas or just ignore the ones don't needed.
 * Works on both statement and row replication.
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.filter.dbselector=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.dbselector.script=../support/filters-javascript/dbselector.js
 * replicator.filter.dbselector.db=dbtoreplicate
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

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    schemaName = filterProperties.getString("db");
    logger.info("dbselector: Selecting only statements from schema \"" + schemaName + "\"");
}

function filter(event)
{
    // All other DBs than specified in replicator.properties will be ignored.
    db = filterProperties.getString("db");
    
    // Analyse what this event is holding.
    data = event.getData();
    
    // One ReplDBMSEvent may contain many DBMSData events.
    for(i=0;i<data.size();i++)
    {
        // Get com.continuent.tungsten.replicator.dbms.DBMSData
        d = data.get(i);
    
        // Determine the underlying type of DBMSData event.
        if(d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
        {
            // It's a SQL statement event.
            if(d.getDefaultSchema().compareTo(db)!=0)
            {
                // Not our DB - ignore the statement!
                data.remove(i);
                logger.debug("dbselector: Removed statement: \"" + d.getQuery() + "\"");
                logger.debug("dbselector: Came from default schema: \"" + d.getDefaultSchema() + "\"");
                // As we removed the array element all other ones came closer to us:
                i--;
            }
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
                
                if(oneRowChange.getSchemaName().compareTo(db)!=0)
                {
                    // Not our DB - ignore the statement!
                    rowChanges.remove(j);

                    schemaName = oneRowChange.getSchemaName();
                    tableName  = oneRowChange.getTableName();

                    logger.debug("dbselector: Removed row operation: table = \"" + schemaName + "."
                             + tableName + "\"");
                    // As we removed the array element all other ones came closer to us:
                    j--;
                }
            }
        }
    }
}
