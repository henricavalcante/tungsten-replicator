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
 * This filter is for heterogeneous MySQL->Oracle topology. To be enabled
 * on the Oracle slave.
 *
 * Allows one to truncate the incoming BLOB to the defined length.
 * Targeted case: MySQL.TEXT (which is stored as BLOB) of variable length is
 * received and to be applied to the Oracle.VARCHAR2 of maximum 4000 length.
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.filter.truncatetext=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.truncatetext.script=${replicator.home.dir}/support/filters-javascript/truncatetext.js
 * replicator.filter.truncatetext.length=4000
 *
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
	logger.info("truncate: Initializing...");	
	
    TypesBLOB = 2004;
    TypesNULL = 0;
    
	truncateTo = filterProperties.getString("length");
	
	logger.info("truncate: Truncate the BLOB/TEXT columns to " + truncateTo + " characters");
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
  // Analise what this event is holding.
  data = event.getData();
  if(data != null)
  {
    // One ReplDBMSEvent may contain many DBMSData events.
    for (i = 0; i < data.size(); i++)
    {
      // Get com.continuent.tungsten.replicator.dbms.DBMSData
      d = data.get(i);
  
      // Determine the underlying type of DBMSData event.
      if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
	  {
	    // We can't do anything about these events.
      }
      else if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
      {
        filterRowChangeData(event, d);
      }
    }
    
    // Remove event completely, if everything's filtered out.
    if (data.isEmpty())
    {
        return null;
    }
  }
}

function filterRowChangeData(event, d)
{
  rowChanges = d.getRowChanges();
  
  // One RowChangeData may contain many OneRowChange events.
  for(j = 0; j < rowChanges.size(); j++)
  {
    // Get com.continuent.tungsten.replicator.dbms.OneRowChange
    oneRowChange = rowChanges.get(j);

    // Iterate through its columns, rows to reach cell values.
    columns = oneRowChange.getColumnSpec();    
    columnValues = oneRowChange.getColumnValues();
    for (c = 0; c < columns.size(); c++)
    {
      columnSpec = columns.get(c);
      type = columnSpec.getType();
 
      // If Oracle's column is *not* BLOB (rather eg. VARCHAR),
      // while the incoming event is of type BLOB or NULL, then
      // it might be MySQL.TEXT->Oracle.VARCHARx case.
      if (!columnSpec.isBlob() && (type == TypesBLOB || TypesNULL))
      {
        for (row = 0; row < columnValues.size(); row++)
        {
          values = columnValues.get(row);
          value = values.get(c);
          // Is incoming value a BLOB?
          if (value.getValue() instanceof com.continuent.tungsten.replicator.extractor.mysql.SerialBlob)
          {
            // Convert it to byte array.
            blob = value.getValue(); // No need for a type cast to SerialBlob as we're in JavaScript.
            if (blob != null)
            {
              valueBytes = blob.getBytes(1, blob.length()); // byte[]
              //logger.debug("truncate: " + java.lang.Object(blob).getClass().getName());
              if (blob.length() > truncateTo)
              {
                logger.debug("truncatetext: value of length " + blob.length() +
                             " detected @ seqno=" + event.getSeqno() +
                             " ROW=" + row + " COL=" + c +
                             "; truncating to " + truncateTo);
                logger.debug("truncatetext: original value: " + byteArrayToString(valueBytes));
                // Truncate the value.
                blob.truncate(truncateTo);
                logger.debug("truncatetext: new value: " + byteArrayToString(blob.getBytes(1, blob.length())));
              }
            }
          }
        }
      }      
    }
  }
}

function byteArrayToString(byteArray)
{
  str = "";
  for (i = 0; i < byteArray.length; i++ )
  {
    str += String.fromCharCode(byteArray[i]);
  }
  return str;
}
