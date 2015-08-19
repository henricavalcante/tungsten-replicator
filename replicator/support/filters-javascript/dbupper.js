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
 * Upper case both schema name and table name in changes.  This is often
 * useful when replicating MySQL to Oracle since MySQL schema and table
 * names are often in lower case, while Oracle's default to upper case.
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.postfilter=dbupper
 * replicator.filter.dbupper=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.dbupper.script=../support/filters-javascript/dbupper.js
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
    // nothing to do here.
}


function filter(event)
{

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
            from = d.getDefaultSchema();
            if (from != null)
            {
            	to   = from.toUpperCase();
            	// It's a SQL statement event.
            	d.setDefaultSchema(to);
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

                fromSchema = oneRowChange.getSchemaName();
                toSchema   = fromSchema.toUpperCase();
                fromTable  = oneRowChange.getTableName();
                toTable    = fromTable.toUpperCase();

                logger.debug("dbupper converting " + fromSchema + "." + fromTable + " to "
                            + toSchema + "." + toTable);

                oneRowChange.setSchemaName(toSchema);
                oneRowChange.setTableName(toTable);
            }
        }
    }
}
