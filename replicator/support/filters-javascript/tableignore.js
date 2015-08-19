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
 * Allows one to ignore specific tables from replicating.
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.filter.tableignore=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.tableignore.script=${replicator.home.dir}/support/filters-javascript/tableignore.js
 * replicator.filter.tableignore.tables=dbnameA.tablename1,dbnameA.tablename2,dbnameB.tablename3
 * 
 * replicator.stage.thl-to-dbms.filters=...,tableignore
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 *
 * NOTE:  This filter has been superceded by the replicate filter.  Check 
 * documentation for more information. 
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    logger.info("tableignore: Initialiazing...");    
    schematables = filterProperties.getString("tables").split("\\,");
    schemas = new Array();
    tables = new Array();
    for (i = 0; i < schematables.length; i++)
    {
        pair = schematables[i].split("\\.");
        schemas[i] = pair[0];
        tables[i] = pair[1];
        logger.info("tableignore: Ignoring statements and row changes from table \"" + tables[i] + "\" in schema \"" + schemas[i] + "\"");
    }
    
    // Show stoppers: if our table name is beyond any of these keywords,
    // it is not really a table name. Must be upper case!
    stoppers = new Array();
    stoppers[0] = "VALUES"; // INSERT | REPLACE ... INTO ... _VALUES_ ... X ...
    stoppers[1] = "WHERE";  // DELETE ... FROM ... _WHERE_ ... X ... 
    stoppers[2] = "SET ";    // UPDATE | LOAD DATA ... _SET_ ... X ...
    // CREATE TABLE t (tablenametoignore int);
    // INSERT INTO t (tablenametoignore) VALUES ('test');
    logger.info("tableignore: Table names encountered after these keywords are treated as values: " + stoppers); 
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
    // Analyze what this event is holding.
    data = event.getData();
    if(data != null)
    {
        // One ReplDBMSEvent may contain many DBMSData events.
        for (i = 0; i < data.size(); i++)
        {
            // Get com.continuent.tungsten.replicator.dbms.DBMSData
            d = data.get(i);
        
            // Determine the underlying type of DBMSData event.
            if(d != null && d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
            {
                // It's a SQL statement event.
                for (t = 0; t < tables.length; t++) 
                {
                    if (d.getDefaultSchema() == null ||
                        d.getDefaultSchema().compareToIgnoreCase(schemas[t]) == 0) 
                    {
                        // Our DB - check the table.
                        table = tables[t];
                        sql = d.getQuery();
                        // Does this SQL even mention this table?
                        tblNamePos = sql.indexOf(table);
                        if (tblNamePos != -1) 
                        {
                            // Ensure that table name is not in the VALUES, WHERE, etc. clauses.
                            isTableName = true;
                            for (s = 0; s < stoppers.length; s++)
                            {
                                if (sql.toUpperCase().lastIndexOf(stoppers[s], tblNamePos) == -1)
                                {
                                    // No stoppers found before table name.
                                }
                                else
                                {
                                    isTableName = false;
                                    logger.debug("tableignore: Leaving statement unfiltered, because table name encountered after stopper '" + stoppers[s] + "': " + sql);
                                    break;
                                }
                            }
                            if(isTableName)
                            {
                                data.remove(i);
                                logger.debug("tableignore: Removed statement, because of table '" + table + "': \"" + d.getQuery() + "\"");
                                logger.debug("tableignore: Came from default schema: \"" + d.getDefaultSchema() + "\"");
                                // As we removed the array element all other ones came closer to us:
                                i--;
                                // Check the next element in this event.
                                break;
                            }
                        }
                    }
                }
            }
            else if(d != null && d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
            {
                rowChanges = d.getRowChanges();
                
                // One RowChangeData may contain many OneRowChange events.
                for(j = 0; j < rowChanges.size(); j++)
                {
                    // Get com.continuent.tungsten.replicator.dbms.OneRowChange
                    oneRowChange = rowChanges.get(j);
                    
                    for (t = 0; t < tables.length; t++) 
                    {
                        if (oneRowChange.getSchemaName() == null ||
                            oneRowChange.getSchemaName().compareToIgnoreCase(schemas[t]) == 0) 
                        {
                            // Our DB - check the table.
                            table = tables[t];
                            
                            if(oneRowChange.getTableName().compareToIgnoreCase(table) == 0)
                            {
                                rowChanges.remove(j);
                                logger.debug(
                                        "tableignore: Removed row change, because of table '" +
                                        oneRowChange.getSchemaName() + "." + table + "'");
                                // As we removed the array element all other ones came closer to us:
                                j--;
                                // Check the next element in this event.
                                break;
                            }
                        }
                    }
                }
            }
        }
        // Remove event completely, if everything's filtered out.
        if (data.isEmpty())
        {
            return null;
        }
    }
}
