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
public class RandLogEvent extends LogEvent
{
    /**
     * Fixed data part: Empty
     * <p>
     * Variable data part:
     * <ul>
     * <li>8 bytes. The value for the first seed.</li>
     * <li>8 bytes. The value for the second seed.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */

    static Logger  logger = Logger.getLogger(RandLogEvent.class);

    private String query;
    private long   seed1;
    private long   seed2;

    public String getQuery()
    {
        return query;
    }

    public RandLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, String currentPosition)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent, MysqlBinlog.RAND_EVENT);

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
                + MysqlBinlog.RAND_SEED1_OFFSET;

        /*
         * Check that the event length is greater than the calculated offset
         */
        if (eventLength < offset)
        {
            throw new MySQLExtractException("rand event length is too short");
        }

        if (descriptionEvent.useChecksum())
        {
            // Removing the checksum from the size of the event
            eventLength -= 4;
        }

        try
        {
            seed1 = LittleEndianConversion.convert8BytesToLong(buffer, offset);
            offset = offset + MysqlBinlog.RAND_SEED2_OFFSET;
            seed2 = LittleEndianConversion.convert8BytesToLong(buffer, offset);

            query = new String("SET SESSION rand_seed1 = " + seed1
                    + " , rand_seed2 = " + seed2);
        }
        catch (Exception e)
        {
            throw new MySQLExtractException("Unable to read rand event", e);
        }

        doChecksum(buffer, eventLength, descriptionEvent);
    }
}
