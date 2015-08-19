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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import java.io.IOException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class AppendBlockLogEvent extends LogEvent
        implements
            LoadDataInfileEvent
{
    int             fileID;
    byte[]          fileData;
    private boolean nextEventCanBeAppended = false;

    public AppendBlockLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, String currentPosition)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent, MysqlBinlog.APPEND_BLOCK_EVENT);

        this.startPosition = currentPosition;
        if (logger.isDebugEnabled())
            logger.debug("Extracting event at position  : " + startPosition
                    + " -> " + getNextEventPosition());

        int commonHeaderLength, postHeaderLength;

        int fixedPartIndex;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength
                    + " common header length: " + commonHeaderLength
                    + " post header length: " + postHeaderLength);

        if (descriptionEvent.useChecksum())
        {
            // Removing the checksum from the size of the event
            eventLength -= 4;
        }

        /* Read the fixed data part */
        fixedPartIndex = commonHeaderLength;

        try
        {
            /* 4 Bytes for file ID */
            fileID = LittleEndianConversion.convert4BytesToInt(buffer,
                    fixedPartIndex);
            fixedPartIndex += 4;

            /*
             * the remaining bytes represent the first bytes of the files to be
             * loaded
             */
            int dataLength = eventLength - fixedPartIndex;
            fileData = new byte[dataLength];
            System.arraycopy(buffer, fixedPartIndex, fileData, 0, dataLength);

            doChecksum(buffer, eventLength, descriptionEvent);
        }
        catch (IOException e)
        {
            logger.error("AppendBlockLogEvent parsing failed", e);
        }
    }

    public int getFileID()
    {
        return fileID;
    }

    public byte[] getData()
    {
        return fileData;
    }

    @Override
    public void setNextEventCanBeAppended(boolean b)
    {
        this.nextEventCanBeAppended = b;
    }

    @Override
    public boolean canNextEventBeAppended()
    {
        return nextEventCanBeAppended;
    }
}
