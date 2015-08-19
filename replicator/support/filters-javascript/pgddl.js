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
 * Reformats MySQL's DDL statements to support PostgreSQL slave. Currently reformats only:
 * 1. " integer autoincrement " -> " serial "
 * 2. " tinyint" -> " smallint"
 * 3. "create database " -> "create schema "
 * 4. "drop database " -> "drop schema "
 *
 * NOTE: Case sensitive!
 * NOTE: Extend as needed per case by case basis.
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    transformers = new Array();
	transformers[0] = new Array(2);
	transformers[0][0] = " integer auto_increment ";
	transformers[0][1] = " serial ";
	transformers[1] = new Array(2);
	transformers[1][0] = " tinyint";
	transformers[1][1] = " smallint";
	transformers[2] = new Array(3);
	transformers[2][0] = "create database ";
	transformers[2][1] = "create schema ";
	transformers[3] = new Array(3);
	transformers[3][0] = "drop database ";
	transformers[3][1] = "drop schema ";
	
	for (t = 0; t < transformers.length; t++)
		logger.debug("pgddl: " + transformers[t][0] + " -> " + transformers[t][1]);
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
			for (t = 0; t < transformers.length; t++)
			{
				sql = d.getQuery();
				if(sql.indexOf(transformers[t][0]) != -1)
				{
					newSql = sql.replace(transformers[t][0], transformers[t][1]);
					logger.debug("pgddl: " + sql + " -> " + newSql);
					d.setQuery(newSql);
				}
			}
        }
        else if(d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
        {
			// DDL does not come as a row change.
        }
    }
}
