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
 * Javascript filter to insert breadcrumb locations in transactions. 
 * Breadcrumbs are a technique to insert restart locations in a log generated
 * by a DBMS server that does not have global transaction IDs (GTIDs) enabled.
 * Breadcrumbs work as follows: 
 * 
 * 1.) A table is created and populated with one more more rows on the master
 * server.  Here is an example of the table definition and inserting a row 
 * using MySQL syntax. 
 * 
 * CREATE TABLE `tungsten_svc1`.`breadcrumbs` (
 *  `id` int(11) NOT NULL PRIMARY KEY,
 *  `counter` int(11) DEFAULT NULL,
 *  `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) ENGINE=InnoDB;
 * INSERT INTO tungsten_svc1.breadcrumbs(id, counter) values(@@server_id, 1);
 *
 * 2.) Now update the table regularly.  In MySQL the following scheduled 
 * event will do the job. 
 * 
 * CREATE EVENT breadcrumbs_refresh
 *   ON SCHEDULE EVERY 5 SECOND
 *   DO
 *      UPDATE tungsten_svc1.breadcrumbs SET counter=counter+1;
 * SET GLOBAL event_scheduler = ON;
 *
 * This filter will extra the value of the counter each time it sees
 * an update.  It will then mark each transaction with a particular server ID 
 * with the counter value plus an offset that increments with each succeeding
 * record.  For convenience we assume row replication is enabled.  
 * 
 * If you need to failover to another server that has different logs, you 
 * can figure out the restart point by looking in the THL for the breadcrumb
 * metadata on the last transaction.  Use this to search the binlogs on the 
 * new server for the correct restart point. 
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */

/**
 * Prepare the script by looking for the server ID that we should use.  
 */
function prepare()
{
  server_id = filterProperties.getString("server_id");
  if (server_id == null)
  {
    logger.info("breadcrumbs.js:  No server ID.  All transactions will be marked.");
  }
  else
  {
    logger.info("breadcrumbs.jsignore_server_id: Only transactions from " 
      + server_id + " will be marked");
  }
 
  // Set initial bread_crumb values. 
  breadcrumb_counter = -1; 
  breadcrumb_offset = -1;
}

/**
 * Called on every event.  If the event is not filtered we add the
 * bread_crumb value.  We always add the same breadcrumb value for all 
 * transaction fragments.  
 */
function filter(event)
{
  // Get the data. 
  data = event.getData();
  if (data != null)
  {
    // First search for updates to the breadcrumbs table.  If we find 
    // these we update our position information.  One ReplDBMSEvent may 
    // contain many DBMSData events.  We only care about row update to 
    // the breadcrumbs table. 
    for (i = 0; i < data.size(); i++)
    {
      // Get com.continuent.tungsten.replicator.dbms.DBMSData
      d = data.get(i);
  
      // Determine the underlying type of DBMSData event.
      if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
      {
        // Ignore statements for now.  
      }
      else if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
      {
        // It's a row change event.
        rowChanges = d.getRowChanges();
        logger.info("Found row change data...");

        for(j = 0; j < rowChanges.size(); j++)
        {
          // Get the next row change set. 
          oneRowChange = rowChanges.get(j);
          schema = oneRowChange.getSchemaName();
          table = oneRowChange.getTableName();

          // See if it's on the breadcrumbs table. 
          if (table.compareToIgnoreCase("breadcrumbs") == 0)
          {
            columnValues = oneRowChange.getColumnValues();
            for (row = 0; row < columnValues.size(); row++)
            {
              values = columnValues.get(row);
              server_id_value = values.get(0);
              if (server_id == null || server_id == server_id_value.getValue())
              {
                counter_value = values.get(1);
                breadcrumb_counter = counter_value.getValue();
                breadcrumb_offset = 0;
                logger.info("Updated breadcrumb data: seqno=" 
                  + event.getSeqno() + " server_id=" + server_id
                  + " breadcrumb_counter=" + breadcrumb_counter
                  + " breadcrumb_offset=" + breadcrumb_offset);
              }
              else
              {
                logger.info("Failed to update breadcrumb data: seqno=" 
                  + event.getSeqno() + " server_id=" + server_id
                  + " server_id in table=" + server_id_value.getValue());
              }
            }
          }
          else
          {
             logger.info("Table does not match: table=" + table);
          }
        }
      }
    }

    // Now apply whatever we have to the current transaction as metadata. 
    topLevelEvent = event.getDBMSEvent();
    if (topLevelEvent != null)
    {
      xact_server_id = topLevelEvent.getMetadataOptionValue("mysql_server_id");
      if (server_id == xact_server_id)
      {
        topLevelEvent.setMetaDataOption("breadcrumb_counter", breadcrumb_counter);
        topLevelEvent.setMetaDataOption("breadcrumb_offset", breadcrumb_offset);
        logger.info("Set breadcrumb data: seqno=" 
          + event.getSeqno() + " server_id=" + server_id
          + " breadcrumb_counter=" + breadcrumb_counter
          + " breadcrumb_offset=" + breadcrumb_offset);
      }
      else
      {
        logger.info("Did not set breadcrumb data: seqno=" 
          + event.getSeqno() + " server_id=" + server_id
          + " xact_server_id=" + xact_server_id);
      }
    }

    // If we are at the end of the transaction, update the breadcrumb offset
    // value. 
    if (event.getLastFrag())
    {
      breadcrumb_offset = breadcrumb_offset + 1;
    }
  }
}
