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
 * This filter turns MySQL 0 date and timestamp values into NULL values.  
 * It is necessary for MySQL to Oracle replication, since Oracle gets sick
 * on 0 date values that MySQL accepts. 
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare() {
	logger.info("fixdates: Initializing...");
	// JDBC type for a date.
	TypesDATE = 91;
	TypesTIMESTAMP = 93;
}

/**
 * Called on every filtered event. See replicator's javadoc for more details on
 * accessible classes. Also, JavaScriptFilter's javadoc contains description
 * about how to define a script like this.
 * 
 * @param event
 *            Filtered com.continuent.tungsten.replicator.event.ReplDBMSEvent
 * 
 * @see com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * @see com.continuent.tungsten.replicator.event.ReplDBMSEvent
 * @see com.continuent.tungsten.replicator.dbms.DBMSData
 * @see com.continuent.tungsten.replicator.dbms.StatementData
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData
 * @see com.continuent.tungsten.replicator.dbms.OneRowChange
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType
 * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#printRowChangeData(StringBuilder,
 *      RowChangeData, String, boolean, int)
 * @see java.lang.Thread
 * @see org.apache.log4j.Logger
 */
function filter(event) {
	// Get the data.
	data = event.getData();
	if (data != null) {
		// One ReplDBMSEvent may contain many DBMSData events.
		for (i = 0; i < data.size(); i++) {
			// Get com.continuent.tungsten.replicator.dbms.DBMSData
			d = data.get(i);

			// Determine the underlying type of DBMSData event.
			if (d != null
					&& d instanceof com.continuent.tungsten.replicator.dbms.StatementData) {
				// We can't do anything about statements.
			} else if (d != null
					&& d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData) {
				fixZeroDates(event, d);
			}
		}

		// Remove event completely, if everything's filtered out.
		if (data.isEmpty()) {
			return null;
		}
	}
}

// Fix zero date and timestamp for a transaction fragment.
function fixZeroDates(event, d) {
	rowChanges = d.getRowChanges();

	// One RowChangeData may contain many OneRowChange events.
	for (j = 0; j < rowChanges.size(); j++) {
		// Get com.continuent.tungsten.replicator.dbms.OneRowChange
		oneRowChange = rowChanges.get(j);

		// Iterate through its columns, rows to reach cell values.
		columns = oneRowChange.getColumnSpec();
		columnValues = oneRowChange.getColumnValues();
		for (c = 0; c < columns.size(); c++) {
			columnSpec = columns.get(c);
			type = columnSpec.getType();
			logger.debug("Type=" + type);

			// See if we have a date in this column.
			if (type == TypesDATE || type == TypesTIMESTAMP) {
				logger.debug("Found a date column");
				// If so, maybe some column values need to be fixed. Iterate
				// through the rows.
				for (row = 0; row < columnValues.size(); row++) {
					values = columnValues.get(row);
					value = values.get(c);

					// Is incoming value a "0 date"?
					if (value.getValue() == 0) {
						// Make it a null.
						value.setValueNull()
						logger.debug("Fixed up a date by making it null");
					} else {
						logger.debug("Date did not need to be fixed...");
					}
				}
			}
		}

		// Iterate through its keys if any
		keys = oneRowChange.getKeySpec();
		keyValues = oneRowChange.getKeyValues();
		for (c = 0; c < keys.size(); c++) {
			keySpec = keys.get(c);
			type = keySpec.getType();
			logger.debug("Type=" + type);

			if (type == TypesDATE || type == TypesTIMESTAMP) {
				logger.debug("Found a date key");
				for (row = 0; row < keyValues.size(); row++) {
					values = keyValues.get(row);
					if (c >= values.size()) {
						continue;
					}
					value = values.get(c);

					if (value.getValue() == 0) {
						value.setValueNull()
						logger.debug("Fixed up a date by making it null");
					} else {
						logger.debug("Date did not need to be fixed...");
					}
				}
			}
		}
	}
}
