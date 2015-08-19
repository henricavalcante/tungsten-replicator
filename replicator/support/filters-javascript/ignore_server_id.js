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
 * Allows one to prevent specific server_ids from replicating.  This must
 * be applied to the last stage of a slave.
 *
 * Instructions for deployment.  
 *
 * 1.) Copy this file to directory tungsten-replicator/support/filters-javascript.  
 *
 * 2.) Locate the properties file for your replication service.  It has a name 
 * like tungsten-replicator/conf/static-home_demo_test.properties.  
 * 
 * 3.) Edit the FILTERS section of the properties file and add the following
 * lines.  Edit the server IDs you want to omit to use your own values. 

 * replicator.filter.ignoreserver=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.ignoreserver.script=${replicator.home.dir}/support/filters-javascript/ignore_server_id.js
 * replicator.filter.ignoreserver.server_ids=3,7
 * 
 * 4.) Edit the pipelines section of the properties file and change the 
 * following line from: 
 * 
 * replicator.stage.q-to-dbms.filters=mysqlsessions,bidiSlave
 *
 * to: 
 * replicator.stage.q-to-dbms.filters=mysqlsessions,bidiSlave,ignoreserver
 * 
 * 5.) Save the file and restart the replicator.  Server IDs that do not match 
 * will not be applied. 
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
var server_ids = null;

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    logger.info("ignore_server_id: Initializing...");    
    server_id_list = filterProperties.getString("server_ids")
    if (server_id_list == null)
        logger.info("No server IDs found to skip");
    else
    {
        server_ids = server_id_list.split("\\,");
        for (i = 0; i < server_ids.length; i++)
        {
            logger.info("ignore_server_id: Ignoring statements from server_id \"" + server_ids[i] + "\"");
        }
    }
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
 * @see java.lang.Thread
 * @see org.apache.log4j.Logger
 */
function filter(event)
{
    // Find the server ID event metadata.
    data = event.getDBMSEvent();
    if (data != null)
    {
        server_id = data.getMetadataOptionValue("mysql_server_id");
        if (server_id != null)
        {
            for (t = 0; t < server_ids.length; t++) 
            {
                if (server_id == server_ids[t]) 
                {
                    logger.debug("ignore_server_id: Removed transation, because of server_id " + server_ids[t]);
                    return null;
                }
            }
        }
    }
}
