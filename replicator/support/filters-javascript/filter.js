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
 * JavaScript example to be used with JavaScriptFilter. Presents trivial
 * possibilities of this filter.
 */
 
/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    // It is possible to get from replicator.properties both factory properties
    // and custom defined properties of current filter's instance.
    master = properties.getString("replicator.thl.remote_uri");
    custom = filterProperties.getString("sample_custom_property");
    
    // Log custom property's value into replicator's log.
    logger.info(custom);
    
    // We might return some message to be logged into replicator's log. It is
    // optional though.  
    return "filter.js prepared to receive events from " + master;
}

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
function filter(event)
{
    // It is possible to delay this event.
    logger.info("Sleeping...");
    thread.sleep(1000);
    
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
            // Log the query into replicator's log.
            logger.info("StatementData: " + d.getQuery());
        }
        else if(d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
        {
            // It's a row change event.
            logger.info("RowChangeData:");
        
            rowChanges = data.get(i).getRowChanges();
            // One RowChangeData may contain many OneRowChange events.
            for(j=0;j<rowChanges.size();j++)
            {
                // Get com.continuent.tungsten.replicator.dbms.OneRowChange
                oneRowChange = rowChanges.get(j);
        
                // Log action (INSERT, UPDATE, DELETE).
                logger.info(oneRowChange.getAction());
        
                // Log table name which this row event affects.
                logger.info(oneRowChange.getTableName());
        
                // Bellow is an example of how to change an event. It is actual
                // in case you need to write data transforming filter. Just use
                // setter methods of the event objects:
                //
                // oneRowChange.setTableName(oneRowChange.getTableName()+"_");
                // logger.info(oneRowChange.getTableName());
            }
        }
    }
    
    // Handling of return values:
    // a. If return value is null or instanceof ReplDBMSEvent, it is passed
    // through to the higher levels of the Replicator,
    // b. If it's something else - it's just logged into INFO stream,
    // c. If there's no return value, it's ignored.
    return event.getSeqno();
}

/**
 * Called once when JavaScriptFilter corresponding to this script is released.
 */
function release()
{
    // We might return some message to be logged into replicator's log. It is
    // optional though.
    return "filter.js released";
}
