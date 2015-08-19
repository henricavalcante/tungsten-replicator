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
 * Filter removes specified columns from the THL row change events. Columns are defined in JSON file.
 *
 * Example of how to define one in static-<service>.properties:
 *
 * replicator.filter.dropColumn=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.dropColumn.script=${replicator.home.dir}/support/filters-javascript/dropcolumn.js
 * replicator.filter.dropColumn.definitionsFile=~/dropcolumn.json
 *
 * See support/filters-javascript/dropcolumn.json for definition file example.
 *
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */

any = new java.lang.String("*");

/**
 * Reads text file into string.
 */
function readFile(path)
{
  var file = new java.io.BufferedReader(new java.io.FileReader(new java.io.File(path)));

  var sb = new java.lang.StringBuffer();
  while((line = file.readLine()) != null)
  {
    sb.append(line);
    sb.append(java.lang.System.getProperty("line.separator"));
  }

  return sb.toString();
}

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
  definitionsFile = filterProperties.getString("definitionsFile");
  logger.info("dropcolumn.js using: " + definitionsFile);
  var json = readFile(definitionsFile);
  definitions = eval("(" + json + ")");
  
  logger.info("Columns to drop:");
  for (var i in definitions)
  {
    var drop = definitions[i];
    logger.info("In " + drop["schema"] + "." + drop["table"] + ": ");
    var cols = drop["columns"];
    for (var c in cols)
    {
      logger.info("  " + cols[c]);
    }
  }
}

function filter(event)
{
  data = event.getData();
  var dataToDrop = new Array();
  if(data != null)
  {
    for (i = 0; i < data.size(); i++)
    {
      d = data.get(i);
      if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
      {
        filterRowChangeData(d);
        
        // No more OneRowChange instances? Queue removal of this DBMSData.
        if (d.getRowChanges().isEmpty() && dataToDrop.indexOf(i) < 0)
          dataToDrop[dataToDrop.length] = i;
      }
    }
    
    // Drop DBMSData objects if needed.
    if (dataToDrop.length > 0)
    {
      // Looping from the end to avoid shifting element positions.
      for (i = dataToDrop.length - 1; i >= 0; i--)
      {
        data.remove(dataToDrop[i]);
      }
    }
  }
  
  // No more data in this event? Remove event, but don't drop when dealing
  // with fragmented events (this could drop the commit part).
  if (event.getFragno() == 0 && event.getLastFrag() && data.isEmpty())
  {
      return null;
  }
}

/**
 * Checks whether particular column should be dropped.
 */
function isDrop(schema, table, col)
{
  for (var def in definitions)
  {
    var drop = definitions[def];
    if(any.compareTo(drop["schema"]) == 0 || schema.compareTo(drop["schema"]) == 0)
    {
      if(any.compareTo(drop["table"]) == 0 || table.compareTo(drop["table"]) == 0)
      {
        var cols = drop["columns"];
        for (var cl in cols)
        {
          if(col == null)
          {
            throw new com.continuent.tungsten.replicator.ReplicatorException(
              "dropcolumn.js: column name in " + schema + "." + table +
              " is undefined - is colnames filter enabled and is it before the dropcolumn filter?"
              );
          }
          if(col.compareTo(cols[cl]) == 0)
          {
            return true;
          }
        }
      }
    }
  }
  return false;
}

function filterRowChangeData(d)
{
  rowChanges = d.getRowChanges();
  var rowToDrop = new Array();
  for(var j = 0; j < rowChanges.size(); j++)
  {
    oneRowChange = rowChanges.get(j);
    var schema = oneRowChange.getSchemaName();
    var table = oneRowChange.getTableName();
    var columns = oneRowChange.getColumnSpec();

    // Drop column values.    
    var columnValues = oneRowChange.getColumnValues();
    var specToDrop = new Array();
    for (var r = 0; r < columnValues.size(); r++)
    {
      for (c = columns.size() - 1 ; c >= 0 ; c--)
      {
        columnSpec = columns.get(c);
        colName = columnSpec.getName();
        if (isDrop(schema,table,colName))
        {
          columnValues.get(r).remove(c);
          if (specToDrop.indexOf(c) < 0)
            specToDrop[specToDrop.length] = c;
        }
      }
    }
    if (specToDrop.length > 0)
    {
      for (var i = 0 ; i < specToDrop.length ; i++)
      {
        columns.remove(specToDrop[i]);
      }
      
      // Queue drop of the row change if we removed all columns (Issue 985).
      if (columns.size() == 0 && rowToDrop.indexOf(j) < 0)
        rowToDrop[rowToDrop.length] = j;
    }
    
    // Drop key values.
    var keyValues = oneRowChange.getKeyValues();
    var keys = oneRowChange.getKeySpec();
    var specToDrop = new Array();
    for (var r = 0; r < keyValues.size(); r++)
    {
      for (c = keys.size() - 1 ; c >= 0; c--)
      {
        keySpec = keys.get(c);
        colName = keySpec.getName();
        if (isDrop(schema,table,colName))
        {
          keyValues.get(r).remove(c);
          if (specToDrop.indexOf(c) < 0)
            specToDrop[specToDrop.length] = c;
        }
      }
    }
    if (specToDrop.length > 0)
    {
      for (var i = 0 ; i < specToDrop.length ; i++)
      {
        keys.remove(specToDrop[i]);
      }
    }
  }
  
  // Drop the row changes if needed.
  if (rowToDrop.length > 0)
  {
    // Looping from the end to avoid shifting element positions.
    for (r = rowToDrop.length - 1; r >= 0; r--)
    {
      rowChanges.remove(rowToDrop[r]);
    }
  }
}
