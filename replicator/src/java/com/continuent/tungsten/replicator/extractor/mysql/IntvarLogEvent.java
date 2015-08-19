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

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class IntvarLogEvent extends LogEvent
{
    /**
     * Fixed data part: Empty
     * <p>
     * Variable data part:
     * <ul>
     * <li>1 byte. A value indicating the variable type: LAST_INSERT_ID_EVENT =
     * 1 or INSERT_ID_EVENT = 2.</li>
     * <li>8 bytes. An unsigned integer indicating the value to be used for the
     * LAST_INSERT_ID() invocation or AUTO_INCREMENT column.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */

    static Logger logger = Logger.getLogger(IntvarLogEvent.class);

    private long  value;

    public IntvarLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, String currentPosition)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent, MysqlBinlog.INTVAR_EVENT);

        this.startPosition = currentPosition;
        if (logger.isDebugEnabled())
            logger.debug("Extracting event at position  : " + startPosition
                    + " -> " + getNextEventPosition());

        int commonHeaderLength, postHeaderLength;
        int offset;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength
                    + " common header length: " + commonHeaderLength
                    + " post header length: " + postHeaderLength);

        offset = commonHeaderLength + postHeaderLength
                + MysqlBinlog.I_TYPE_OFFSET;

        if (descriptionEvent.useChecksum())
        {
            // Removing the checksum from the size of the event
            eventLength -= 4;
        }

        /*
         * Check that the event length is greater than the calculated offset
         */
        if (eventLength < offset)
        {
            throw new MySQLExtractException("INTVAR event length is too short");
        }

        try
        {
            type = LittleEndianConversion.convert1ByteToInt(buffer, offset);
            offset += MysqlBinlog.I_VAL_OFFSET;
            value = LittleEndianConversion.convert8BytesToLong(buffer, offset);

        }
        catch (Exception e)
        {
            throw new MySQLExtractException("Intvar extracting failed: " + e);
        }

        doChecksum(buffer, eventLength, descriptionEvent);
    }

    public int getType()
    {
        return type;
    }

    public void setType(int type)
    {
        this.type = type;
    }

    public long getValue()
    {
        return value;
    }

    public void setValue(long value)
    {
        this.value = value;
    }
}
