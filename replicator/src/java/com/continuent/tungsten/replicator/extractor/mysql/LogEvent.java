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

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.BigEndianConversion;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */

public abstract class LogEvent
{
    protected static Logger logger              = Logger.getLogger(LogEvent.class);

    protected long          execTime;
    protected int           type;
    protected Timestamp     when;
    protected int           serverId;

    protected int           logPos;
    protected int           flags;

    protected boolean       threadSpecificEvent = false;

    protected String        startPosition       = "";

    public LogEvent()
    {
        type = MysqlBinlog.START_EVENT_V3;
    }

    public LogEvent(byte[] buffer, FormatDescriptionLogEvent descriptionEvent,
            int eventType) throws ReplicatorException
    {
        type = eventType;

        try
        {
            when = new Timestamp(
                    1000 * LittleEndianConversion
                            .convert4BytesToLong(buffer, 0));
            serverId = (int) LittleEndianConversion.convert4BytesToLong(buffer,
                    MysqlBinlog.SERVER_ID_OFFSET);
            if (descriptionEvent.binlogVersion == 1)
            {
                logPos = 0;
                flags = 0;
                return;
            }

            // Get initial log position.
            logPos = (int) LittleEndianConversion.convert4BytesToLong(buffer,
                    MysqlBinlog.LOG_POS_OFFSET);

            if ((descriptionEvent.binlogVersion == 3)
                    && (buffer[MysqlBinlog.EVENT_TYPE_OFFSET] < MysqlBinlog.FORMAT_DESCRIPTION_EVENT)
                    && (logPos > 0))
            {
                logPos += LittleEndianConversion.convert4BytesToLong(buffer,
                        MysqlBinlog.EVENT_LEN_OFFSET);
            }
            if (logger.isDebugEnabled())
                logger.debug("log_pos: " + logPos);

            flags = LittleEndianConversion.convert2BytesToInt(buffer,
                    MysqlBinlog.FLAGS_OFFSET);

            // See if we have a thread-specific event.
            if (logger.isDebugEnabled())
                logger.debug("Event is thread-specific = "
                        + threadSpecificEvent);

            // See if we have an event that is just a header.
            if ((buffer[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.FORMAT_DESCRIPTION_EVENT)
                    || (buffer[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.ROTATE_EVENT))
            {
                // If so, return.
                return;
            }
        }
        catch (IOException e)
        {
            throw new MySQLExtractException("log event create failed", e);
        }
    }

    public long getExecTime()
    {
        return execTime;
    }

    public Timestamp getWhen()
    {
        return when;
    }

    /**
     * Returns the position for the next event.
     * 
     * @return Returns the logPos.
     */
    public int getNextEventPosition()
    {
        return logPos;
    }

    private static LogEvent readLogEvent(boolean parseStatements,
            String currentPosition, byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent,
            boolean useBytesForString) throws ReplicatorException
    {
        LogEvent event = null;

        int eventType;
        try
        {
            eventType = LittleEndianConversion.convert1ByteToInt(buffer,
                    MysqlBinlog.EVENT_TYPE_OFFSET);

            switch (eventType)
            {
                case MysqlBinlog.QUERY_EVENT :
                    event = new QueryLogEvent(buffer, eventLength,
                            descriptionEvent, parseStatements,
                            useBytesForString, currentPosition);
                    break;
                case MysqlBinlog.LOAD_EVENT :
                    logger.warn("Skipping unsupported LOAD_EVENT");
                    break;
                case MysqlBinlog.NEW_LOAD_EVENT :
                    logger.warn("Skipping unsupported NEW_LOAD_EVENT");
                    break;
                case MysqlBinlog.ROTATE_EVENT :
                    event = new RotateLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.SLAVE_EVENT : /*
                                                * can never happen (unused
                                                * event)
                                                */
                    logger.warn("Skipping unsupported SLAVE_EVENT");
                    break;
                case MysqlBinlog.CREATE_FILE_EVENT :
                    logger.warn("Skipping unsupported CREATE_FILE_EVENT");
                    break;
                case MysqlBinlog.APPEND_BLOCK_EVENT :
                    if (logger.isDebugEnabled())
                        logger.debug("reading APPEND_BLOCK_EVENT");
                    event = new AppendBlockLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.DELETE_FILE_EVENT :
                    if (logger.isDebugEnabled())
                        logger.debug("reading DELETE_FILE_EVENT");
                    event = new DeleteFileLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.EXEC_LOAD_EVENT :
                    logger.warn("Skipping unsupported EXEC_LOAD_EVENT");
                    break;
                case MysqlBinlog.START_EVENT_V3 :
                    /* this is sent only by MySQL <=4.x */
                    logger.warn("Skipping unsupported START_EVENT_V3");
                    break;
                case MysqlBinlog.STOP_EVENT :
                    event = new StopLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.INTVAR_EVENT :
                    if (logger.isDebugEnabled())
                        logger.debug("extracting INTVAR_EVENT");
                    event = new IntvarLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.XID_EVENT :
                    event = new XidLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.RAND_EVENT :
                    event = new RandLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.USER_VAR_EVENT :
                    event = new UserVarLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.FORMAT_DESCRIPTION_EVENT :
                    event = new FormatDescriptionLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.PRE_GA_WRITE_ROWS_EVENT :
                    logger.warn("Skipping unsupported PRE_GA_WRITE_ROWS_EVENT");
                    break;
                case MysqlBinlog.PRE_GA_UPDATE_ROWS_EVENT :
                    logger.warn("Skipping unsupported PRE_GA_UPDATE_ROWS_EVENT");
                    break;
                case MysqlBinlog.PRE_GA_DELETE_ROWS_EVENT :
                    logger.warn("Skipping unsupported PRE_GA_DELETE_ROWS_EVENT");
                    break;
                case MysqlBinlog.WRITE_ROWS_EVENT :
                case MysqlBinlog.NEW_WRITE_ROWS_EVENT :
                    if (logger.isDebugEnabled())
                        logger.debug("reading WRITE_ROWS_EVENT");
                    event = new WriteRowsLogEvent(buffer, eventLength,
                            descriptionEvent, useBytesForString,
                            currentPosition);
                    break;
                case MysqlBinlog.UPDATE_ROWS_EVENT :
                case MysqlBinlog.NEW_UPDATE_ROWS_EVENT :
                    if (logger.isDebugEnabled())
                        logger.debug("reading UPDATE_ROWS_EVENT");
                    event = new UpdateRowsLogEvent(buffer, eventLength,
                            descriptionEvent, useBytesForString,
                            currentPosition);
                    break;
                case MysqlBinlog.DELETE_ROWS_EVENT :
                case MysqlBinlog.NEW_DELETE_ROWS_EVENT :
                    if (logger.isDebugEnabled())
                        logger.debug("reading DELETE_ROWS_EVENT");
                    event = new DeleteRowsLogEvent(buffer, eventLength,
                            descriptionEvent, useBytesForString,
                            currentPosition);
                    break;
                case MysqlBinlog.TABLE_MAP_EVENT :
                    if (logger.isDebugEnabled())
                        logger.debug("reading TABLE_MAP_EVENT");
                    event = new TableMapLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.BEGIN_LOAD_QUERY_EVENT :
                    if (logger.isDebugEnabled())
                        logger.debug("reading BEGIN_LOAD_QUERY_EVENT");
                    event = new BeginLoadQueryLogEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                case MysqlBinlog.EXECUTE_LOAD_QUERY_EVENT :
                    if (logger.isDebugEnabled())
                        logger.debug("reading EXECUTE_LOAD_QUERY_EVENT");
                    event = new ExecuteLoadQueryLogEvent(buffer, eventLength,
                            descriptionEvent, parseStatements, currentPosition);
                    break;
                case MysqlBinlog.INCIDENT_EVENT :
                    logger.warn("Skipping unsupported INCIDENT_EVENT");
                    break;
                case MysqlBinlog.GTID_EVENT :
                    event = new MariaDBGTIDEvent(buffer, eventLength,
                            descriptionEvent, currentPosition);
                    break;
                default :
                    logger.warn("Skipping unrecognized binlog event type "
                            + eventType);
            }

        }
        catch (IOException e1)
        {
        }

        return event;
    }

    public static LogEvent readLogEvent(ReplicatorRuntime runtime,
            BinlogReader position, FormatDescriptionLogEvent descriptionEvent,
            boolean parseStatements, boolean useBytesForString,
            boolean prefetchSchemaNameLDI) throws ReplicatorException,
            InterruptedException
    {
        int eventLength = 0;
        byte[] header = new byte[descriptionEvent.commonHeaderLength];

        try
        {
            String currentPosition = position.toString();

            // read the header part
            // timeout is set to 2 minutes.
            readDataFromBinlog(runtime, position, header, 0, header.length, 120);

            // Extract event length
            eventLength = (int) LittleEndianConversion.convert4BytesToLong(
                    header, MysqlBinlog.EVENT_LEN_OFFSET);

            eventLength -= header.length;

            byte[] fullEvent = new byte[header.length + eventLength];

            // read the event data part
            // timeout is set to 2 minutes
            readDataFromBinlog(runtime, position, fullEvent, header.length,
                    eventLength, 120);

            System.arraycopy(header, 0, fullEvent, 0, header.length);

            LogEvent event = readLogEvent(parseStatements, currentPosition,
                    fullEvent, fullEvent.length, descriptionEvent,
                    useBytesForString);

            // If schema name has to be prefetched, check if it is a BEGIN LOAD
            // EVENT
            if (prefetchSchemaNameLDI
                    && event instanceof BeginLoadQueryLogEvent)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Got Begin Load Query Event - Looking for corresponding Execute Event");

                BeginLoadQueryLogEvent beginLoadEvent = (BeginLoadQueryLogEvent) event;
                // Spawn a new data input stream
                BinlogReader tempPosition = position.clone();
                tempPosition.setEventID(position.getEventID() + 1);
                tempPosition.open();

                if (logger.isDebugEnabled())
                    logger.debug("Reading from " + tempPosition);
                boolean found = false;
                byte[] tmpHeader = new byte[descriptionEvent.commonHeaderLength];

                String tempPos;
                while (!found)
                {
                    tempPos = tempPosition.toString();
                    eventLength = extractEventHeader(runtime, tempPosition,
                            tmpHeader);

                    if (tmpHeader[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.EXECUTE_LOAD_QUERY_EVENT)
                    {
                        fullEvent = extractFullEvent(runtime, eventLength,
                                tempPosition, tmpHeader);

                        LogEvent tempEvent = readLogEvent(parseStatements,
                                tempPos, fullEvent, fullEvent.length,
                                descriptionEvent, useBytesForString);

                        if (tempEvent instanceof ExecuteLoadQueryLogEvent)
                        {
                            ExecuteLoadQueryLogEvent execLoadQueryEvent = (ExecuteLoadQueryLogEvent) tempEvent;
                            if (execLoadQueryEvent.getFileID() == beginLoadEvent
                                    .getFileID())
                            {
                                if (logger.isDebugEnabled())
                                    logger.debug("Found corresponding Execute Load Query Event - Schema is "
                                            + execLoadQueryEvent.getDefaultDb());
                                beginLoadEvent.setSchemaName(execLoadQueryEvent
                                        .getDefaultDb());
                                found = true;
                            }
                        }

                    }
                    else if (tmpHeader[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.ROTATE_EVENT)
                    {
                        fullEvent = extractFullEvent(runtime, eventLength,
                                tempPosition, tmpHeader);

                        LogEvent tempEvent = readLogEvent(parseStatements,
                                tempPos, fullEvent, fullEvent.length,
                                descriptionEvent, useBytesForString);

                        if (tempEvent instanceof RotateLogEvent)
                        { // It's real so we need to rotate the log.
                            tempPosition.close();
                            tempPosition
                                    .setFileName(((RotateLogEvent) tempEvent)
                                            .getNewBinlogFilename());
                            tempPosition.open();
                        }
                        else
                            throw new ExtractorException(
                                    "Failed to extract RotateLogEvent"
                                            + tempPosition);
                    }
                    else
                    {
                        long skip = 0;
                        while (skip != eventLength)
                        {
                            skip += tempPosition.skip(eventLength - skip);
                        }
                    }
                }
                // Release the file handler
                tempPosition.close();
            }

            if (event instanceof BeginLoadQueryLogEvent
                    || event instanceof AppendBlockLogEvent)
            {
                LoadDataInfileEvent currentEvent = (LoadDataInfileEvent) event;
                currentEvent
                        .setNextEventCanBeAppended(checkNextEventIsPartOfSameLDI(
                                runtime, position, descriptionEvent,
                                parseStatements, useBytesForString,
                                currentEvent.getFileID()));
            }

            return event;
        }
        catch (EOFException e)
        {
            throw new MySQLExtractException("EOFException while reading "
                    + eventLength + " bytes from binlog ", e);
        }
        catch (IOException e)
        {
            throw new MySQLExtractException("binlog read error", e);
        }
    }

    /**
     * Check whether next binlog event is part of the same Load Data Infile
     * command (identified by file ID).
     * 
     * @param runtime Replicator runtime
     * @param position Position of current event
     * @param descriptionEvent Description event to be used to extract the next
     *            event
     * @param parseStatements
     * @param useBytesForString
     * @param fileId The file ID of current event
     * @return true if next event is part of the same load data infile command,
     *         false otherwise
     * @throws ReplicatorException
     * @throws InterruptedException
     * @throws IOException
     * @throws ExtractorException
     */
    private static boolean checkNextEventIsPartOfSameLDI(
            ReplicatorRuntime runtime, BinlogReader position,
            FormatDescriptionLogEvent descriptionEvent,
            boolean parseStatements, boolean useBytesForString, int fileID)
            throws ReplicatorException, InterruptedException, IOException,
            ExtractorException
    {
        byte[] fullEvent;
        int eventLength;

        // Spawn a new data input stream
        BinlogReader tempPosition = position.clone();
        tempPosition.setEventID(position.getEventID() + 1);
        tempPosition.open();

        if (logger.isDebugEnabled())
            logger.debug("Reading from " + tempPosition);

        boolean found = false;
        byte[] tmpHeader = new byte[descriptionEvent.commonHeaderLength];

        try
        {
            String tempPos;
            while (!found)
            {
                tempPos = tempPosition.toString();
                eventLength = extractEventHeader(runtime, tempPosition,
                        tmpHeader);

                if (tmpHeader[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.EXECUTE_LOAD_QUERY_EVENT
                        || tmpHeader[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.DELETE_FILE_EVENT
                        || tmpHeader[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.APPEND_BLOCK_EVENT)
                {
                    // Next event is of the correct type... does it match
                    // the file ID?
                    found = true;
                    fullEvent = extractFullEvent(runtime, eventLength,
                            tempPosition, tmpHeader);

                    LogEvent tempEvent = readLogEvent(parseStatements, tempPos,
                            fullEvent, fullEvent.length, descriptionEvent,
                            useBytesForString);

                    if (tempEvent instanceof LoadDataInfileEvent)
                    {
                        LoadDataInfileEvent nextEvent = (LoadDataInfileEvent) tempEvent;

                        if (nextEvent.getFileID() == fileID)
                        {
                            return true;
                        }
                    }

                }
                else if (tmpHeader[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.ROTATE_EVENT)
                {
                    fullEvent = extractFullEvent(runtime, eventLength,
                            tempPosition, tmpHeader);

                    LogEvent tempEvent = readLogEvent(parseStatements, tempPos,
                            fullEvent, fullEvent.length, descriptionEvent,
                            useBytesForString);

                    if (tempEvent instanceof RotateLogEvent)
                    { // It's real so we need to rotate the log.
                        tempPosition.close();
                        tempPosition.setFileName(((RotateLogEvent) tempEvent)
                                .getNewBinlogFilename());
                        tempPosition.open();
                    }
                    else
                        throw new ExtractorException(
                                "Failed to extract RotateLogEvent"
                                        + tempPosition);
                }
                else if (tmpHeader[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.FORMAT_DESCRIPTION_EVENT)
                {
                    long skip = 0;
                    while (skip != eventLength)
                    {
                        skip += tempPosition.skip(eventLength - skip);
                    }
                }
                else
                    return false;
            }
        }
        finally
        {
            // Release the file handler
            tempPosition.close();
        }
        return false;
    }

    /**
     * Read a full event from the binlog into a byte array
     * 
     * @param runtime Replicator runtime
     * @param eventLength Length of the event to be read
     * @param tempPosition Current position in the binlog. It matches the
     *            beginning of the event to be read
     * @param tmpHeader Header of the event to be read
     * @return The byte array read from the binlog
     * @throws IOException
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    private static byte[] extractFullEvent(ReplicatorRuntime runtime,
            int eventLength, BinlogReader tempPosition, byte[] tmpHeader)
            throws IOException, ReplicatorException, InterruptedException
    {
        byte[] fullEvent;
        fullEvent = new byte[tmpHeader.length + eventLength];
        readDataFromBinlog(runtime, tempPosition, fullEvent, tmpHeader.length,
                eventLength, 120);

        System.arraycopy(tmpHeader, 0, fullEvent, 0, tmpHeader.length);
        return fullEvent;
    }

    /**
     * Extract the event header from the binlog file at the given position.
     * 
     * @param runtime Replicator runtime
     * @param position Position from the binlog where to read from
     * @param header the extracted header
     * @return the full length, as read from the header
     * @throws IOException
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    private static int extractEventHeader(ReplicatorRuntime runtime,
            BinlogReader position, byte[] header) throws IOException,
            ReplicatorException, InterruptedException
    {
        int eventLength;
        readDataFromBinlog(runtime, position, header, 0, header.length, 60);

        // Extract event length
        eventLength = (int) LittleEndianConversion.convert4BytesToLong(header,
                MysqlBinlog.EVENT_LEN_OFFSET) - header.length;
        return eventLength;
    }

    /**
     * readDataFromBinlog waits for data to be fully written in the binlog file
     * and then reads it.
     * 
     * @param runtime replicator runtime
     * @param dis Input stream from which data will be read
     * @param data Array of byte that will contain read data
     * @param offset Position in the previous array where data should be written
     * @param length Data length to be read
     * @param timeout Maximum time to wait for data to be available
     * @throws IOException if an error occurs while reading from the stream
     * @throws ReplicatorException if the timeout is reached
     */
    private static void readDataFromBinlog(ReplicatorRuntime runtime,
            BinlogReader binlog, byte[] data, int offset, int length,
            int timeout) throws IOException, ReplicatorException,
            InterruptedException
    {
        boolean alreadyLogged = false;
        int spentTime = 0;
        int timeoutInMs = timeout * 1000;

        long available;
        while ((available = binlog.available()) < (long) length)
        {
            if (!alreadyLogged)
            {
                if (logger.isDebugEnabled())
                {
                    // This conditions appears commonly on slow file systems,
                    // hence should be a debug message.
                    logger.debug("Trying to read more bytes (" + length
                            + ") than available in the file (" + available
                            + " in " + binlog.getFileName()
                            + ")... waiting for data to be available");
                }
                alreadyLogged = true;
            }

            try
            {
                if (spentTime < timeoutInMs)
                {
                    Thread.sleep(1);
                    spentTime++;
                }
                else
                    throw new MySQLExtractException(
                            "Timeout while waiting for data : spent more than "
                                    + timeout + " seconds while waiting for "
                                    + length + " bytes to be available");
            }
            catch (InterruptedException e)
            {
            }
        }
        binlog.read(data, offset, length);
    }

    public int getType()
    {
        return type;
    }

    protected static String hexdump(byte[] buffer, int offset)
    {
        StringBuffer dump = new StringBuffer();
        if ((buffer.length - offset) > 0)
        {
            dump.append(String.format("%02x", buffer[offset]));
            for (int i = offset + 1; i < buffer.length; i++)
            {
                dump.append("_");
                dump.append(String.format("%02x", buffer[i]));
            }
        }
        return dump.toString();
    }

    protected String hexdump(byte[] buffer, int offset, int length)
    {
        StringBuffer dump = new StringBuffer();

        if (buffer.length >= offset + length)
        {
            dump.append(String.format("%02x", buffer[offset]));
            for (int i = offset + 1; i < offset + length; i++)
            {
                dump.append("_");
                dump.append(String.format("%02x", buffer[i]));
            }
        }
        return dump.toString();
    }

    protected void doChecksum(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent)
            throws ExtractorException
    {
        if (descriptionEvent.useChecksum())
        {
            long checksum = MysqlBinlog.getChecksum(
                    descriptionEvent.getChecksumAlgo(), buffer, 0, eventLength);
            if (checksum > -1)
                try
                {
                    long binlogChecksum = LittleEndianConversion
                            .convert4BytesToLong(buffer, eventLength);
                    if (checksum != binlogChecksum)
                    {
                        throw new ExtractorException(
                                "Corrupted event in binlog (checksums do not match) at position "
                                        + startPosition
                                        + ". \nCalculated checksum : "
                                        + checksum + " - Binlog checksum : "
                                        + binlogChecksum
                                        + "\nNext event position :"
                                        + getNextEventPosition());

                    }
                }
                catch (IOException ignore)
                {
                    logger.warn("Failed to compute checksum", ignore);
                }
        }
    }

    protected BigDecimal extractDecimal(byte[] buffer, int precision, int scale)
    {
        //
        // Decimal representation in binlog seems to be as follows:
        // 1 byte - 'precision'
        // 1 byte - 'scale'
        // remaining n bytes - integer such that value = n / (10^scale)
        // Integer is represented as follows:
        // 1st bit - sign such that set == +, unset == -
        // every 4 bytes represent 9 digits in big-endian order, so that if
        // you print the values of these quads as big-endian integers one after
        // another, you get the whole number string representation in decimal.
        // What remains is to put a sign and a decimal dot.
        // 13 0a 80 00 00 05 1b 38 b0 60 00 means:
        // 0x13 - precision = 19
        // 0x0a - scale = 10
        // 0x80 - positive
        // 0x00000005 0x1b38b060 0x00
        // 5 456700000 0
        // 54567000000 / 10^{10} = 5.4567
        //
        // int_size below shows how long is integer part
        //
        // offset = offset + 2; // offset of the number part
        //
        int intg = precision - scale;
        int intg0 = intg / MysqlBinlog.DIG_PER_INT32;
        int frac0 = scale / MysqlBinlog.DIG_PER_INT32;
        int intg0x = intg - intg0 * MysqlBinlog.DIG_PER_INT32;
        int frac0x = scale - frac0 * MysqlBinlog.DIG_PER_INT32;

        int offset = 0;

        int sign = (buffer[offset] & 0x80) == 0x80 ? 1 : -1;

        // how many bytes are used to represent given amount of digits?
        int integerSize = intg0 * MysqlBinlog.SIZE_OF_INT32
                + MysqlBinlog.dig2bytes[intg0x];
        int decimalSize = frac0 * MysqlBinlog.SIZE_OF_INT32
                + MysqlBinlog.dig2bytes[frac0x];

        if (logger.isDebugEnabled())
            logger.debug("Integer size in bytes = " + integerSize
                    + " - Fraction size in bytes = " + decimalSize);
        int bin_size = integerSize + decimalSize; // total bytes
        byte[] d_copy = new byte[bin_size];

        if (bin_size > buffer.length)
        {
            throw new ArrayIndexOutOfBoundsException("Calculated bin_size: "
                    + bin_size + ", available bytes: " + buffer.length);
        }

        // Invert first bit
        d_copy[0] = buffer[0];
        d_copy[0] ^= 0x80;
        if (sign == -1)
        {
            // Invert every byte
            d_copy[0] ^= 0xFF;
        }

        for (int i = 1; i < bin_size; i++)
        {
            d_copy[i] = buffer[i];
            if (sign == -1)
            {
                // Invert every byte
                d_copy[i] ^= 0xFF;
            }
        }

        // Integer part
        offset = MysqlBinlog.dig2bytes[intg0x];

        BigDecimal intPart = new BigDecimal(0);

        if (offset > 0)
            intPart = BigDecimal.valueOf(BigEndianConversion
                    .convertNBytesToInt(d_copy, 0, offset));

        while (offset < integerSize)
        {
            intPart = intPart.movePointRight(MysqlBinlog.DIG_PER_DEC1).add(
                    BigDecimal.valueOf(BigEndianConversion.convert4BytesToInt(
                            d_copy, offset)));
            offset += 4;
        }

        // Decimal part
        BigDecimal fracPart = new BigDecimal(0);
        int shift = 0;
        for (int i = 0; i < frac0; i++)
        {
            shift += MysqlBinlog.DIG_PER_DEC1;
            fracPart = fracPart.add(BigDecimal.valueOf(
                    BigEndianConversion.convert4BytesToInt(d_copy, offset))
                    .movePointLeft(shift));
            offset += 4;
        }

        if (MysqlBinlog.dig2bytes[frac0x] > 0)
        {
            fracPart = fracPart.add(BigDecimal.valueOf(
                    BigEndianConversion.convertNBytesToInt(d_copy, offset,
                            MysqlBinlog.dig2bytes[frac0x])).movePointLeft(
                    shift + frac0x));
        }

        return BigDecimal.valueOf(sign).multiply(intPart.add(fracPart));

    }

    /**
     * Returns the number of bytes that is used to store a decimal whose
     * precision and scale are given
     * 
     * @param precision of the decimal
     * @param scale of the decimal
     * @return number of bytes used to store the decimal(precision, scale)
     */
    protected int getDecimalBinarySize(int precision, int scale)
    {
        int intg = precision - scale;
        int intg0 = intg / MysqlBinlog.DIG_PER_DEC1;
        int frac0 = scale / MysqlBinlog.DIG_PER_DEC1;
        int intg0x = intg - intg0 * MysqlBinlog.DIG_PER_DEC1;
        int frac0x = scale - frac0 * MysqlBinlog.DIG_PER_DEC1;

        assert (scale >= 0 && precision > 0 && scale <= precision);

        return intg0 * (4) + MysqlBinlog.dig2bytes[intg0x] + frac0 * (4)
                + MysqlBinlog.dig2bytes[frac0x];
    }

    public static String hexdump(byte[] buffer)
    {
        return hexdump(buffer, 0);
    }

}
