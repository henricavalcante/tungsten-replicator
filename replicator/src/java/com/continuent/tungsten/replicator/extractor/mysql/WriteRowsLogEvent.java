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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class WriteRowsLogEvent extends RowsLogEvent
{

    public WriteRowsLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent,
            boolean useBytesForString, String currentPosition)
            throws ReplicatorException
    {
        super(buffer, eventLength, descriptionEvent,
                buffer[MysqlBinlog.EVENT_TYPE_OFFSET], useBytesForString);

        this.startPosition = currentPosition;
        if (logger.isDebugEnabled())
            logger.debug("Extracting event at position  : " + startPosition
                    + " -> " + getNextEventPosition());

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.mysql.RowsLogEvent#processExtractedEvent(com.continuent.tungsten.replicator.dbms.RowChangeData,
     *      com.continuent.tungsten.replicator.extractor.mysql.TableMapLogEvent)
     */
    @Override
    public void processExtractedEvent(RowChangeData rowChanges,
            TableMapLogEvent map) throws ReplicatorException
    {
        if (map == null)
        {
            throw new MySQLExtractException("Write row event for unknown table");
        }
        OneRowChange oneRowChange = new OneRowChange();
        oneRowChange.setSchemaName(map.getDatabaseName());
        oneRowChange.setTableName(map.getTableName());
        oneRowChange.setTableId(map.getTableId());
        oneRowChange.setAction(RowChangeData.ActionType.INSERT);

        int rowIndex = 0; /* index of the row in value arrays */

        int size = bufferSize;

        for (int bufferIndex = 0; bufferIndex < size;)
        {
            int length;

            /* Extract data */
            try
            {
                length = processExtractedEventRow(oneRowChange, rowIndex,
                        usedColumns, bufferIndex, packedRowsBuffer, map, false);
            }
            catch (ReplicatorException e)
            {
                logger.error(
                        "Failure while processing extracted write row event", e);
                throw (e);
            }
            rowIndex++;

            if (length == 0)
                break;
            bufferIndex += length;
        }
        rowChanges.appendOneRowChange(oneRowChange);

        // Store options, if any
        rowChanges.addOption("foreign_key_checks", getForeignKeyChecksFlag());
        rowChanges.addOption("unique_checks", getUniqueChecksFlag());

    }

}
