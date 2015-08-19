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

import java.io.IOException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.GeneralConversion;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class FormatDescriptionLogEvent extends StartLogEvent
{
    protected int   binlogVersion;
    public short    commonHeaderLength;
    public short[]  postHeaderLength;
    public short[]  maria10PostHeaderLength;

    private int     eventTypesCount;
    private int     checksumAlgo;

    // MariaDB 10 support
    private boolean isMaria10 = false;

    public FormatDescriptionLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, String currentPosition)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent);

        this.startPosition = currentPosition;
        if (logger.isDebugEnabled())
            logger.debug("Extracting event at position  : " + startPosition
                    + " -> " + getNextEventPosition());

        int noCrcEventLength = eventLength - 4;

        if (logger.isDebugEnabled())
            logger.debug("FormatDescriptionLogEvent - length should be "
                    + eventLength);

        commonHeaderLength = buffer[MysqlBinlog.LOG_EVENT_MINIMAL_HEADER_LEN
                + MysqlBinlog.ST_COMMON_HEADER_LEN_OFFSET];

        if (commonHeaderLength < MysqlBinlog.OLD_HEADER_LEN)
        {
            throw new MySQLExtractException(
                    "Format Description event header length is too short");
        }

        eventTypesCount = eventLength
                - (MysqlBinlog.LOG_EVENT_MINIMAL_HEADER_LEN
                        + MysqlBinlog.ST_COMMON_HEADER_LEN_OFFSET + 1);

        if (logger.isDebugEnabled())
            logger.debug("commonHeaderLength= " + commonHeaderLength
                    + " eventTypesCount= " + eventTypesCount);

        // Clear the IN_USE flag before computing the CRC
        // No need to save the value as we don't use it anyway.
        buffer[MysqlBinlog.FLAGS_OFFSET] = (byte) (buffer[MysqlBinlog.FLAGS_OFFSET] & ~MysqlBinlog.LOG_EVENT_BINLOG_IN_USE_F);

        if (logger.isDebugEnabled())
            logger.debug("Checksumming : "
                    + hexdump(buffer, 0, noCrcEventLength));

        long evChecksum = 0L;
        try
        {
            evChecksum = LittleEndianConversion.convert4BytesToLong(buffer,
                    noCrcEventLength);
            if (logger.isDebugEnabled())
                logger.debug("Binlog event checksum is : "
                        + hexdump(buffer, noCrcEventLength) + " / "
                        + evChecksum);
        }
        catch (IOException e)
        {
        }

        // This event is checksummed if calculated checksum == checksum bytes as
        // found in the binlog
        long calculatedChecksum = MysqlBinlog.getCrc32(buffer, 0,
                noCrcEventLength);
        if (logger.isDebugEnabled())
            logger.debug("Calculated checksum = " + calculatedChecksum);
        boolean isChecksummed = evChecksum == calculatedChecksum;
        if (isChecksummed)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("This FD event is checksummed");
                // Check whether checksum algorithm is set
                logger.debug("@Pos : "
                        + (MysqlBinlog.LOG_EVENT_MINIMAL_HEADER_LEN
                                + MysqlBinlog.FORMAT_DESCRIPTION_HEADER_LEN_5_6 + 1)
                        + " Algo is :"
                        + hexdump(
                                buffer,
                                MysqlBinlog.LOG_EVENT_MINIMAL_HEADER_LEN
                                        + MysqlBinlog.FORMAT_DESCRIPTION_HEADER_LEN_5_6
                                        + 1, 1));
            }

            int chksumAlg = GeneralConversion
                    .unsignedByteToInt(buffer[MysqlBinlog.LOG_EVENT_MINIMAL_HEADER_LEN
                            + MysqlBinlog.FORMAT_DESCRIPTION_HEADER_LEN_5_6 + 1]);
            if (logger.isDebugEnabled())
                logger.debug("Found algo =" + chksumAlg);
            if (chksumAlg > 0 && chksumAlg < 0xFF)
            {
                this.checksumAlgo = chksumAlg;
                logger.debug("This binlog is checksummed.");
            }
        }
        else
        {
            this.checksumAlgo = 0; // NONE
            if (logger.isDebugEnabled())
                logger.debug("This FD event is not checksummed -> this master is not checksum enabled !");
        }
    }

    public FormatDescriptionLogEvent(int binlogVersion, int checksumAlgo)
    {
        if (logger.isDebugEnabled())
            logger.debug("Using checksum algo :" + checksumAlgo);
        this.checksumAlgo = checksumAlgo;
        this.binlogVersion = binlogVersion;
        postHeaderLength = new short[MysqlBinlog.ENUM_END_EVENT_FROM_56];

        maria10PostHeaderLength = new short[MysqlBinlog.ENUM_MARIA_END_EVENT
                - MysqlBinlog.ENUM_MARIA_START_EVENT + 1];

        /* identify binlog format */
        switch (binlogVersion)
        {
            case 1 : // 3.23
                commonHeaderLength = MysqlBinlog.OLD_HEADER_LEN;
                eventTypesCount = MysqlBinlog.FORMAT_DESCRIPTION_EVENT - 1;

                postHeaderLength[MysqlBinlog.START_EVENT_V3 - 1] = MysqlBinlog.START_V3_HEADER_LEN;
                postHeaderLength[MysqlBinlog.QUERY_EVENT - 1] = MysqlBinlog.QUERY_HEADER_MINIMAL_LEN;
                postHeaderLength[MysqlBinlog.STOP_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.ROTATE_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.INTVAR_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.LOAD_EVENT - 1] = MysqlBinlog.LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.SLAVE_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.CREATE_FILE_EVENT - 1] = MysqlBinlog.CREATE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.APPEND_BLOCK_EVENT - 1] = MysqlBinlog.APPEND_BLOCK_HEADER_LEN;
                postHeaderLength[MysqlBinlog.EXEC_LOAD_EVENT - 1] = MysqlBinlog.EXEC_LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.DELETE_FILE_EVENT - 1] = MysqlBinlog.DELETE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.NEW_LOAD_EVENT - 1] = postHeaderLength[MysqlBinlog.LOAD_EVENT - 1];
                postHeaderLength[MysqlBinlog.RAND_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.USER_VAR_EVENT - 1] = 0;
                break;
            case 3 : // 4.0.2
                commonHeaderLength = MysqlBinlog.OLD_HEADER_LEN;
                eventTypesCount = MysqlBinlog.FORMAT_DESCRIPTION_EVENT - 1;

                postHeaderLength[MysqlBinlog.START_EVENT_V3 - 1] = MysqlBinlog.START_V3_HEADER_LEN;
                postHeaderLength[MysqlBinlog.QUERY_EVENT - 1] = MysqlBinlog.QUERY_HEADER_MINIMAL_LEN;
                postHeaderLength[MysqlBinlog.STOP_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.ROTATE_EVENT - 1] = MysqlBinlog.ROTATE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.INTVAR_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.LOAD_EVENT - 1] = MysqlBinlog.LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.SLAVE_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.CREATE_FILE_EVENT - 1] = MysqlBinlog.CREATE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.APPEND_BLOCK_EVENT - 1] = MysqlBinlog.APPEND_BLOCK_HEADER_LEN;
                postHeaderLength[MysqlBinlog.EXEC_LOAD_EVENT - 1] = MysqlBinlog.EXEC_LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.DELETE_FILE_EVENT - 1] = MysqlBinlog.DELETE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.NEW_LOAD_EVENT - 1] = postHeaderLength[MysqlBinlog.LOAD_EVENT - 1];
                postHeaderLength[MysqlBinlog.RAND_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.USER_VAR_EVENT - 1] = 0;

                break;
            case 4 : // 5.0
                commonHeaderLength = MysqlBinlog.LOG_EVENT_HEADER_LEN;
                eventTypesCount = MysqlBinlog.LOG_NEW_5_6_EVENT_TYPES;

                postHeaderLength[MysqlBinlog.START_EVENT_V3 - 1] = MysqlBinlog.START_V3_HEADER_LEN;
                postHeaderLength[MysqlBinlog.QUERY_EVENT - 1] = MysqlBinlog.QUERY_HEADER_LEN;
                postHeaderLength[MysqlBinlog.ROTATE_EVENT - 1] = MysqlBinlog.ROTATE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.LOAD_EVENT - 1] = MysqlBinlog.LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.CREATE_FILE_EVENT - 1] = MysqlBinlog.CREATE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.APPEND_BLOCK_EVENT - 1] = MysqlBinlog.APPEND_BLOCK_HEADER_LEN;
                postHeaderLength[MysqlBinlog.EXEC_LOAD_EVENT - 1] = MysqlBinlog.EXEC_LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.DELETE_FILE_EVENT - 1] = MysqlBinlog.DELETE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.NEW_LOAD_EVENT - 1] = postHeaderLength[MysqlBinlog.LOAD_EVENT - 1];
                postHeaderLength[MysqlBinlog.FORMAT_DESCRIPTION_EVENT - 1] = MysqlBinlog.FORMAT_DESCRIPTION_HEADER_LEN;
                postHeaderLength[MysqlBinlog.TABLE_MAP_EVENT - 1] = MysqlBinlog.TABLE_MAP_HEADER_LEN;
                postHeaderLength[MysqlBinlog.WRITE_ROWS_EVENT - 1] = MysqlBinlog.ROWS_HEADER_LEN;
                postHeaderLength[MysqlBinlog.UPDATE_ROWS_EVENT - 1] = MysqlBinlog.ROWS_HEADER_LEN;
                postHeaderLength[MysqlBinlog.DELETE_ROWS_EVENT - 1] = MysqlBinlog.ROWS_HEADER_LEN;
                postHeaderLength[MysqlBinlog.EXECUTE_LOAD_QUERY_EVENT - 1] = MysqlBinlog.EXECUTE_LOAD_QUERY_HEADER_LEN;
                postHeaderLength[MysqlBinlog.APPEND_BLOCK_EVENT - 1] = MysqlBinlog.APPEND_BLOCK_HEADER_LEN;
                postHeaderLength[MysqlBinlog.DELETE_FILE_EVENT - 1] = MysqlBinlog.DELETE_FILE_HEADER_LEN;

                postHeaderLength[MysqlBinlog.NEW_WRITE_ROWS_EVENT - 1] = MysqlBinlog.ROWS_HEADER_LEN + 2;
                postHeaderLength[MysqlBinlog.NEW_UPDATE_ROWS_EVENT - 1] = MysqlBinlog.ROWS_HEADER_LEN + 2;
                postHeaderLength[MysqlBinlog.NEW_DELETE_ROWS_EVENT - 1] = MysqlBinlog.ROWS_HEADER_LEN + 2;

                maria10PostHeaderLength[MysqlBinlog.ANNOTATE_ROWS_EVENT
                        - MysqlBinlog.ENUM_MARIA_START_EVENT] = MysqlBinlog.ANNOTATE_ROWS_HEADER_LEN;
                maria10PostHeaderLength[MysqlBinlog.GTID_EVENT
                        - MysqlBinlog.ENUM_MARIA_START_EVENT] = MysqlBinlog.GTID_HEADER_LEN;
                maria10PostHeaderLength[MysqlBinlog.GTID_LIST_EVENT
                        - MysqlBinlog.ENUM_MARIA_START_EVENT] = MysqlBinlog.GTID_LIST_HEADER_LEN;
                maria10PostHeaderLength[MysqlBinlog.BINLOG_CHECKPOINT_EVENT
                        - MysqlBinlog.ENUM_MARIA_START_EVENT] = MysqlBinlog.BINLOG_CHECKPOINT_HEADER_LEN;

                break;
        }
    }

    public FormatDescriptionLogEvent(int binlogVersion, int checksumAlgo,
            boolean isMaria10)
    {
        this(binlogVersion, checksumAlgo);
        this.isMaria10 = isMaria10;
    }

    public int getChecksumAlgo()
    {
        return checksumAlgo;
    }

    public boolean useChecksum()
    {
        if (logger.isDebugEnabled())
            logger.debug("Checking if checksum in use :" + checksumAlgo);
        return checksumAlgo > 0 && checksumAlgo < 0xff;
    }

    /**
     * Returns the isMaria10 value.
     * 
     * @return Returns the isMaria10.
     */
    public boolean isMaria10()
    {
        return isMaria10;
    }

}
