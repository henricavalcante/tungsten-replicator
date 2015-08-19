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

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class MariaDBGTIDEvent extends QueryLogEvent
{

    /**
     * seq_no : 8 byte unsigned integer increasing id within one server_id.
     * Starts at 1, holes in the sequence may occur <BR>
     * domain_id : 4 byte unsigned integer Replication domain id, identifying
     * independent replication streams<BR>
     * flags : 1 byte bitfield<BR>
     * Bit 0 set indicates stand-alone event (no terminating COMMIT)<BR>
     * Bit 1 set indicates group commit, and that commit id exists<BR>
     * Reserved (no group commit) / commit id (group commit) (see flags bit 1) 6
     * bytes / 8 bytes Reserved bytes, set to 0. Maybe be used for future
     * expansion (no group commit). OR commit id, same for all GTIDs in the same
     * group commit (see flags bit 1).
     */

    private int     domainId;
    private long    seqno;
    private boolean standalone;
    private boolean groupCommit;

    public MariaDBGTIDEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, String currentPosition)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent, MysqlBinlog.GTID_EVENT);

        this.startPosition = currentPosition;
        if (logger.isDebugEnabled())
            logger.debug("Extracting event at position  : " + startPosition
                    + " -> " + getNextEventPosition());

        int commonHeaderLength, postHeaderLength;
        int offset;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.maria10PostHeaderLength[type
                - MysqlBinlog.ENUM_MARIA_START_EVENT];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength
                    + " common header length: " + commonHeaderLength
                    + " post header length: " + postHeaderLength);

        offset = commonHeaderLength;

        /*
         * Check that the event length is greater than the calculated offset
         */
        if (eventLength < offset)
        {
            throw new MySQLExtractException(
                    "MariaDB GTID event length is too short");
        }

        if (descriptionEvent.useChecksum())
        {
            // Removing the checksum from the size of the event
            eventLength -= 4;
        }

        try
        {
            seqno = LittleEndianConversion.convert8BytesToLong(buffer, offset);
            offset += 8;
            domainId = LittleEndianConversion
                    .convert4BytesToInt(buffer, offset);
            offset += 4;

            // Extract flags
            standalone = (buffer[offset] & 0x01) == 1;
            groupCommit = (buffer[offset] & 0x02) == 2;

            // Next bytes are reserved for future usage, so no need to read them
            // for now.

        }
        catch (Exception e)
        {
            throw new MySQLExtractException("Unable to read GTID event", e);
        }

        doChecksum(buffer, eventLength, descriptionEvent);

    }

    /**
     * Returns the standalone value.
     * 
     * @return true if it is a standalone statement.
     */
    public boolean isStandalone()
    {
        return standalone;
    }

    /**
     * Returns the GTID domainId value.
     * 
     * @return Returns the domainId.
     */
    public int getGTIDDomainId()
    {
        return domainId;
    }

    /**
     * Returns the GTID seqno value.
     * 
     * @return Returns the seqno.
     */
    public long getGTIDSeqno()
    {
        return seqno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return domainId + " " + serverId + " " + seqno + "(standalone:"
                + standalone + " / groupCommit:" + groupCommit + ")";
    }

}
