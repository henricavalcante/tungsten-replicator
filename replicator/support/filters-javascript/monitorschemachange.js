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
 * Monitors schema changes and optionally forces a commit when such 
 * schema changes occur.  This filter is useful when loading into Hadoop 
 * in order to flag table changes.  It should run in the final q-to-dbms 
 * stage for best results. 
 *
 * Example of how to define in replicator.properties:
 *
 * replicator.filter.monitorschemachange=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.monitorschemachange.script=${replicator.home.dir}/support/filters-javascript/monitorschemachange.js
 * replicator.filter.monitorschemachange.commit=true
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s): Linas Virbalas
 */
 
/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    // Check to see if we should commit when schema changes occur. 
    doCommit = filterProperties.getBoolean("commit");
    if (doCommit)
    { 
      logger.info("monitorschemachange: will commit on schema change");
    }  

    // Check to see if we should generate a notification on a schema change. 
    doNotify = filterProperties.getBoolean("notify");
    if (doNotify)
    { 
      logger.info("monitorschemachange: will notify on schema change");

      // We need a notification directory as well.  Try to create it. 
      notifyDirname = filterProperties.getString("notifyDir");
      notifyDir = new java.io.File(notifyDirname);
      if (! notifyDir.exists())
      {
        notifyDir.mkdirs();
      }
    }  
}

function filter(event)
{
    // Analyse what this event is holding.
    dbmsEvent = event.getDBMSEvent();
    data = event.getData();

    // If there is no schema change marked, we don't have to do anything. 
    if (dbmsEvent.getMetadataOptionValue("schema_change") == null &&
        dbmsEvent.getMetadataOptionValue("truncate") == null)
    {
      //logger.info("Nothing to do: seqno=" + event.getSeqno());
      return;
    }

    // Log all schema changes that need attention. 
    for(i = 0; i < data.size(); i++)
    {
        // Get com.continuent.tungsten.replicator.dbms.DBMSData
        d = data.get(i);

        // We only care about statements in this filter. 
        if(d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
        {
            // If it's a SQL statement event and has the "##operation" tag it's
            // a schema change and should be logged. 
            operation = d.getOption("##operation");
            if (operation != null)
            {
              schema = d.getOption("##schema");
              table = d.getOption("##table");
              if (table == null)
              {
                table = "-none-";
              }
              sql = d.getQuery();
              logger.info("SCHEMA CHANGE: sourceId=" + event.getSourceId() 
                + " seqno=" + event.getSeqno() 
                + " commitTimestamp=" + event.getExtractedTstamp()
                + " schema=" + schema + " table=" + table 
                + " operation=" + operation + " sql=[" + sql + "]");

              // If notifications are enabled, generate a file for this
              // schema change.  
              if (doNotify)
              {
                notifyName = schema + "." + table + "." + event.getSeqno();
                notifyFile = new java.io.File(notifyDir, notifyName);
                writer = new java.io.PrintWriter(new java.io.BufferedWriter(new java.io.FileWriter(notifyFile)));
                writer.println("# Schema change notification");
                writer.println("sourceId=" + event.getSourceId());
                writer.println("seqno=" + event.getSeqno());
                writer.println("commitTimestamp=" + event.getExtractedTstamp());
                writer.println("schema=" + schema);
                writer.println("table=" + table);
                writer.println("operation=" + operation);
                writer.println("sql=" + sql);
                writer.flush();
                writer.close();
              }
            }
        }
    }

    // If we should commit, mark the event now.  This will force commit on
    // current and any downstream stages. 
    if (doCommit)
    {
      logger.info("Forcing commit due to table change: seqno=" 
        + event.getSeqno());
      dbmsEvent.setMetaDataOption("force_commit","");
    }
}
