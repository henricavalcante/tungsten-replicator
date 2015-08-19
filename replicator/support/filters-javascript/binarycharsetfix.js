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
 * Java script example showing how to mutate an invalid character set and 
 * and collation on a statement into one that can be processed on the slave. 
 *
 * Instructions for deployment.  
 *
 * 1.) Copy this file to directory tungsten-replicator/support/filters-javascript
 *
 * 2.) Locate the properties file for your replication service.  It has a name 
 * like tungsten-replicator/conf/static-mysvc.properties.  
 * 
 * 3.) Edit the FILTERS section of the properties file and add the following
 * lines.  Edit the server IDs you want to omit to use your own values. 

 * replicator.filter.binarycharsetfix=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.binarycharsetfix.script=${replicator.home.dir}/support/filters-javascript/binarycharsetfix.js
 * 
 * 4.) Edit the pipelines section of the properties file and change the 
 * following line from: 
 * 
 * replicator.stage.binary-to-q.filters=
 * 
 * to: 
 *
 * replicator.stage.binary-to-q.filters=binarycharsetfix
 *
 * 5.) Save the file and restart the replicator.  
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    logger.info("binarycharsetfix initialized");    
}

/**
 * Filter charset in current event and change 63 (binary) to 47 (Latin1 binary)
 * Do this for statements only.  We alter the stated options for charset. 
 */
function filter(event)
{
    // Find the DBMS data. 
    data = event.getData();

    // One ReplDBMSEvent may contain many DBMSData events.
    for(i = 0; i < data.size(); i++)
    {
          // Get com.continuent.tungsten.replicator.dbms.DBMSData
          d = data.get(i);

          // Look for statement data. 
          if(d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
          {   
              // Now look for charset equal to 63. 
              charset_id = d.getOption("character_set_client");
              if (charset_id == 63)
              {
                  // If this does not happen too often a comment is nice. 
                  logger.info("Patching up charset: seqno=" + event.getSeqno() 
                     + " fragno=" + event.getFragno() + " statement number=" + i);

                  // Drop old values. Options are stored as name value pairs, 
                  // so we have to cycle through the option list backwards
                  // to remove them. 
                  options = d.getOptions();
                  for (o = options.size() - 1; o >= 0; o--)
                  {
                      option = options.get(o);
                      name = option.getOptionName();
                      if (name == "character_set_client" || name == "collation_connection")
                      {
                          if (options.remove(option))
                          {
                              logger.debug("Removed option: " + name);
                          }
                      }
                  }

                  // Add replacement values.  
                  d.addOption("character_set_client", 47);
                  d.addOption("collation_connection", 47);
              }
          }
    }
}
