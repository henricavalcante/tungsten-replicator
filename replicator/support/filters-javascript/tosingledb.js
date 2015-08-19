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
 * Transforms transaction's default schema into a single one defined by the
 *  user. Useful for heterogeneous replication and data warehousing scenarios.
 *
 * NOTE: does *not* transform SQL string inside the event, thus care
 * must be taken if statement replication is used to ensure that there
 * are no explicit schema definitions (eg. "INSERT INTO myschema.t ...").
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.filter.tosingledb=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.tosingledb.script=${replicator.home.dir}/support/filters-javascript/tosingledb.js
 * replicator.filter.tosingledb.db=dbtoreplicateto
 * replicator.filter.tosingledb.skip=tungsten
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
    db = filterProperties.getString("db");
    skip = filterProperties.getString("skip");
    logger.info("tosingledb: Any (except \"" + skip + "\") default schema will be transformed into: \"" + db + "\"");
}

function filter(event)
{
    // All other DBs than specified in replicator.properties will be ignored.
    db = filterProperties.getString("db");
    skip = filterProperties.getString("skip");
    
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
            oldDb = d.getDefaultSchema();
            if(oldDb!=null && oldDb.compareTo(skip)!=0)
            {
                d.setDefaultSchema(db);
                logger.debug("tosingledb: Transformed default schema: \"" +
                    oldDb + "\" -> \"" + d.getDefaultSchema() + "\"");
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
                
                oldDb = oneRowChange.getSchemaName();
                if(oldDb.compareTo(skip)!=0)
                {
                    oneRowChange.setSchemaName(db);
                    logger.debug("tosingledb: Transformed default schema: \"" +
                        oldDb + "\" -> \"" + oneRowChange.getSchemaName() + "\"");
                }
            }
        }
    }
}
