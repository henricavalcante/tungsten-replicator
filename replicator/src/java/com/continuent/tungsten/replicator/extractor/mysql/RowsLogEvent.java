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
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.DatabaseHelper;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.BigEndianConversion;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.GeneralConversion;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public abstract class RowsLogEvent extends LogEvent
{
    /**
     * Fixed data part:
     * <ul>
     * <li>6 bytes. The table ID.</li>
     * <li>2 bytes. Reserved for future use.</li>
     * </ul>
     * <p>
     * Variable data part:
     * <ul>
     * <li>Packed integer. The number of columns in the table.</li>
     * <li>Variable-sized. Bit-field indicating whether each column is used, one
     * bit per column. For this field, the amount of storage required for N
     * columns is INT((N+7)/8) bytes.</li>
     * <li>Variable-sized (for UPDATE_ROWS_LOG_EVENT only). Bit-field indicating
     * whether each column is used in the UPDATE_ROWS_LOG_EVENT after-image; one
     * bit per column. For this field, the amount of storage required for N
     * columns is INT((N+7)/8) bytes.</li>
     * <li>Variable-sized. A sequence of zero or more rows. The end is
     * determined by the size of the event. Each row has the following format:
     * <ul>
     * <li>Variable-sized. Bit-field indicating whether each field in the row is
     * NULL. Only columns that are "used" according to the second field in the
     * variable data part are listed here. If the second field in the variable
     * data part has N one-bits, the amount of storage required for this field
     * is INT((N+7)/8) bytes.</li>
     * <li>Variable-sized. The row-image, containing values of all table fields.
     * This only lists table fields that are used (according to the second field
     * of the variable data part) and non-NULL (according to the previous
     * field). In other words, the number of values listed here is equal to the
     * number of zero bits in the previous field (not counting padding bits in
     * the last byte). The format of each value is described in the
     * log_event_print_value() function in log_event.cc.</li>
     * <li>(for UPDATE_ROWS_EVENT only) the previous two fields are repeated,
     * representing a second table row.</li>
     * </ul>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    static Logger                       logger                                 = Logger.getLogger(RowsLogEvent.class);

    private long                        tableId;

    protected long                      columnsNumber;

    // BITMAP
    protected BitSet                    usedColumns;

    // BITMAP for row after image
    protected BitSet                    usedColumnsForUpdate;

    /* Rows in packed format */
    protected byte[]                    packedRowsBuffer;

    /* One-after the end of the allocated space */
    protected int                       bufferSize;

    protected boolean                   useBytesForString;

    protected FormatDescriptionLogEvent descriptionEvent                       = null;

    private boolean                     flagForeignKeyChecks                   = true;
    private boolean                     flagUniqueChecks                       = true;

    /**
     * MariaDB 10 TIME, TIMESTAMP and DATETIME support
     */
    // DATETIME is stored on a different number of bytes depending on the
    // subsecond precision.
    // DATETIME_BYTES_PER_SUB_SECOND_DECIMALS[i] is the number of bytes written
    // to the binlog for a DATETIME value with i decimal digits (DATETIME(i) in
    // MariaDB).
    private static final int[]          DATETIME_BYTES_PER_SUB_SECOND_DECIMAL  = new int[]{
            5, 6, 6, 7, 7, 7, 8                                                };

    // TIME is stored on a different number of bytes depending on the
    // subsecond precision.
    // TIME_BYTES_PER_SUB_SECOND_DECIMALS[i] is the number of bytes written
    // to the binlog for a TIME value with i decimal digits (TIME(i) in
    // MariaDB).
    private static final int[]          TIME_BYTES_PER_SUB_SECOND_DECIMAL      = {
            3, 4, 4, 5, 5, 5, 6                                                };

    // This is the maximum value for TIME datatype in microseconds. This value
    // is added to actual time value when written to the binlog. It needs to get
    // substracted when value is extracted from binlog.
    private static final long           MAX_TIME                               = (838L
                                                                                       * 3600L
                                                                                       + 59L
                                                                                       * 60L
                                                                                       + 59L + 1L) * 1000000L;

    // TIMESTAMP is written as previously, but extra bytes are stored for second
    // parts.
    // TIMESTAMP_BYTES_PER_SUB_SECOND_DECIMAL[i] is the number of extra bytes
    // written to the binlog for i decimal digits (TIMESTAMP(i) in MariaDB).
    private static final int[]          TIMESTAMP_BYTES_PER_SUB_SECOND_DECIMAL = {
            0, 1, 1, 2, 2, 3, 3                                                };

    // Multiplier used to read the number as microseconds.
    // For example, TIME(i) has i decimal digits. These decimals are converted
    // into microseconds by multiplying by SECOND_TO_MICROSECOND_MULTIPLIER[i]
    private static final int[]          SECOND_TO_MICROSECOND_MULTIPLIER       = new int[]{
            1000000, 100000, 10000, 1000, 100, 10, 1                           };

    public RowsLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, int eventType,
            boolean useBytesForString) throws ReplicatorException
    {
        super(buffer, descriptionEvent, eventType);
        this.descriptionEvent = descriptionEvent;

        if (logger.isDebugEnabled())
            logger.debug("Dumping rows event " + hexdump(buffer));

        this.useBytesForString = useBytesForString;

        int commonHeaderLength, postHeaderLength;

        int fixedPartIndex;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength
                    + " common header length: " + commonHeaderLength
                    + " post header length: " + postHeaderLength);

        try
        {
            /* Read the fixed data part */
            fixedPartIndex = commonHeaderLength;

            fixedPartIndex += MysqlBinlog.RW_MAPID_OFFSET;
            if (postHeaderLength == 6)
            {
                /*
                 * Master is of an intermediate source tree before 5.1.4. Id is
                 * 4 bytes
                 */
                tableId = LittleEndianConversion.convert4BytesToLong(buffer,
                        fixedPartIndex);
                fixedPartIndex += 4;
            }
            else
            {
                // assert (postHeaderLength ==
                // MysqlBinlog.TABLE_MAP_HEADER_LEN);
                /* 6 bytes. The table ID. */
                tableId = LittleEndianConversion.convert6BytesToLong(buffer,
                        fixedPartIndex);
                fixedPartIndex += MysqlBinlog.TM_FLAGS_OFFSET;
            }

            /*
             * Next 2 bytes are reserved for future use : no need to process
             * them for now.
             */
            readSessionVariables(buffer, fixedPartIndex);

            /* Read the variable data part of the event */
            int variableStartIndex = commonHeaderLength + postHeaderLength;

            int index = variableStartIndex;

            if (logger.isDebugEnabled())
                logger.debug("Reading number of columns from position " + index);

            long ret[] = MysqlBinlog.decodePackedInteger(buffer, index);
            columnsNumber = ret[0];
            index = (int) ret[1];

            if (logger.isDebugEnabled())
                logger.debug("Number of columns in the table = "
                        + columnsNumber);

            /*
             * Amount of storage required by bit-field indicating whether each
             * column is used for columnsNumber columns
             */
            int usedColumnsLength = (int) ((columnsNumber + 7) / 8);
            usedColumns = new BitSet(usedColumnsLength);

            if (logger.isDebugEnabled())
                logger.debug("Reading used columns bit-field from position "
                        + index);

            MysqlBinlog.setBitField(usedColumns, buffer, index,
                    (int) columnsNumber);

            index += usedColumnsLength;
            if (logger.isDebugEnabled())
                logger.debug("Bit-field of used columns "
                        + usedColumns.toString());

            if (eventType == MysqlBinlog.UPDATE_ROWS_EVENT
                    || eventType == MysqlBinlog.NEW_UPDATE_ROWS_EVENT)
            {
                usedColumnsForUpdate = new BitSet(usedColumnsLength);
                if (logger.isDebugEnabled())
                    logger.debug("Reading used columns bit-field for update from position "
                            + index);
                MysqlBinlog.setBitField(usedColumnsForUpdate, buffer, index,
                        (int) columnsNumber);
                index += usedColumnsLength;
                if (logger.isDebugEnabled())
                    logger.debug("Bit-field of used columns for update "
                            + usedColumnsForUpdate.toString());
            }

            int dataIndex = index;

            if (descriptionEvent.useChecksum())
            {
                // Removing the checksum from the size of the event
                eventLength -= 4;
            }

            int dataSize = eventLength - dataIndex;
            if (logger.isDebugEnabled())
                logger.debug("tableId: " + tableId
                        + " Number of columns in table: " + columnsNumber
                        + " Data size: " + dataSize);

            packedRowsBuffer = new byte[dataSize];
            bufferSize = dataSize;
            System.arraycopy(buffer, dataIndex, packedRowsBuffer, 0, bufferSize);

            doChecksum(buffer, eventLength, descriptionEvent);

        }
        catch (IOException e)
        {
            logger.error("Rows log event parsing failed : ", e);
        }
        return;
    }

    public abstract void processExtractedEvent(RowChangeData rowChanges,
            TableMapLogEvent map) throws ReplicatorException;

    public int getEventSize()
    {
        return packedRowsBuffer.length;
    }

    protected int extractValue(ColumnSpec spec, ColumnVal value, byte[] row,
            int rowPos, int type, int meta, TableMapLogEvent map)
            throws IOException, ReplicatorException
    {
        int length = 0;
        // Calculate length for MYSQL_TYPE_STRING
        if (type == MysqlBinlog.MYSQL_TYPE_STRING)
        {
            if (meta >= 256)
            {
                int byte0 = meta >> 8;
                int byte1 = meta & 0xFF;

                if ((byte0 & 0x30) != 0x30)
                {
                    /* a long CHAR() field: see #37426 */
                    length = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
                    type = byte0 | 0x30;
                }
                else
                {
                    switch (byte0)
                    {
                        case MysqlBinlog.MYSQL_TYPE_SET :
                        case MysqlBinlog.MYSQL_TYPE_ENUM :
                        case MysqlBinlog.MYSQL_TYPE_STRING :
                            type = byte0;
                            length = byte1;
                            break;

                        default :
                        {
                            logger.error("Don't know how to handle column type");
                            return 0;
                        }
                    }
                }
            }
            else
            {
                length = meta;
            }
        }

        if (logger.isDebugEnabled())
            logger.debug("Handling type " + type + " - meta = " + meta);
        switch (type)
        {
            case MysqlBinlog.MYSQL_TYPE_LONG :
            {
                int si = (int) LittleEndianConversion
                        .convertSignedNBytesToLong(row, rowPos, 4);

                if (si < MysqlBinlog.INT_MIN || si > MysqlBinlog.INT_MAX)
                {
                    logger.error("int out of range: " + si + "(range: "
                            + MysqlBinlog.INT_MIN + " - " + MysqlBinlog.INT_MAX
                            + " )");
                }
                value.setValue(new Integer(si));
                if (spec != null)
                {
                    spec.setType(java.sql.Types.INTEGER);
                    spec.setLength(4);
                }
                return 4;
            }

            case MysqlBinlog.MYSQL_TYPE_TINY :
            {
                short si = BigEndianConversion.convert1ByteToShort(row, rowPos);
                if (si < MysqlBinlog.TINYINT_MIN
                        || si > MysqlBinlog.TINYINT_MAX)
                {
                    logger.error("tinyint out of range: " + si + "(range: "
                            + MysqlBinlog.TINYINT_MIN + " - "
                            + MysqlBinlog.TINYINT_MAX + " )");
                }

                value.setValue(new Integer(si));
                if (spec != null)
                {
                    spec.setType(java.sql.Types.INTEGER);
                    spec.setLength(1);
                }
                return 1;
            }

            case MysqlBinlog.MYSQL_TYPE_SHORT :
            {
                short si = (short) LittleEndianConversion
                        .convertSignedNBytesToLong(row, rowPos, 2);
                if (si < MysqlBinlog.SMALLINT_MIN
                        || si > MysqlBinlog.SMALLINT_MAX)
                {
                    logger.error("smallint out of range: " + si + "(range: "
                            + MysqlBinlog.SMALLINT_MIN + " - "
                            + MysqlBinlog.SMALLINT_MAX + " )");
                }
                value.setValue(new Integer(si));
                if (spec != null)
                {
                    spec.setType(java.sql.Types.INTEGER);
                    spec.setLength(2);
                }
                return 2;
            }

            case MysqlBinlog.MYSQL_TYPE_INT24 :
            {
                int si = (int) LittleEndianConversion
                        .convertSignedNBytesToLong(row, rowPos, 3);
                if (si < MysqlBinlog.MEDIUMINT_MIN
                        || si > MysqlBinlog.MEDIUMINT_MAX)
                {
                    logger.error("mediumint out of range: " + si + "(range: "
                            + MysqlBinlog.MEDIUMINT_MIN + " - "
                            + MysqlBinlog.MEDIUMINT_MAX + " )");
                }
                value.setValue(new Integer(si));
                if (spec != null)
                {
                    spec.setType(java.sql.Types.INTEGER);
                    spec.setLength(3);
                }
                return 3;
            }

            case MysqlBinlog.MYSQL_TYPE_LONGLONG :
            {
                long si = LittleEndianConversion.convertSignedNBytesToLong(row,
                        rowPos, 8);
                if (si < 0)
                {
                    long ui = LittleEndianConversion.convert8BytesToLong(row,
                            rowPos);
                    value.setValue(new Long(ui));
                    if (spec != null)
                    {
                        spec.setType(java.sql.Types.INTEGER);
                        spec.setLength(8);
                    }
                }
                else
                {
                    value.setValue(new Long(si));
                    if (spec != null)
                    {
                        spec.setType(java.sql.Types.INTEGER);
                        spec.setLength(8);
                    }
                }
                return 8;
            }

            case MysqlBinlog.MYSQL_TYPE_NEWDECIMAL :
            {
                int precision = meta >> 8;
                int decimals = meta & 0xFF;
                int bin_size = getDecimalBinarySize(precision, decimals);
                byte[] dec = new byte[bin_size];
                for (int i = 0; i < bin_size; i++)
                    dec[i] = row[rowPos + i];
                BigDecimal myDouble = extractDecimal(dec, precision, decimals);
                value.setValue(myDouble);
                if (spec != null)
                    spec.setType(java.sql.Types.DECIMAL);
                return bin_size;
            }

            case MysqlBinlog.MYSQL_TYPE_FLOAT :
            {
                float fl = MysqlBinlog.float4ToFloat(row, rowPos);
                value.setValue(new Float(fl));
                if (spec != null)
                    spec.setType(java.sql.Types.FLOAT);
                return 4;
            }

            case MysqlBinlog.MYSQL_TYPE_DOUBLE :
            {
                double dbl = MysqlBinlog.double8ToDouble(row, rowPos);
                value.setValue(new Double(dbl));
                if (spec != null)
                    spec.setType(java.sql.Types.DOUBLE);

                return 8;
            }

            case MysqlBinlog.MYSQL_TYPE_BIT :
            {
                /* Meta-data: bit_len, bytes_in_rec, 2 bytes */
                int nbits = ((meta >> 8) * 8) + (meta & 0xFF);
                length = (nbits + 7) / 8;

                /*
                 * This code has come from observations of patterns in the MySQL
                 * binlog. It is not directly from reading any public domain
                 * C-source code. The test cases included a variety of bit(x)
                 * columns from 1 bit up to 28 bits. This length appears to be
                 * correctly calculated and the bit values themselves are in a
                 * simple, non byte swapped byte array.
                 */

                int retval = (int) MysqlBinlog.ulNoSwapToInt(row, rowPos,
                        length);
                value.setValue(new Integer(retval));
                if (spec != null)
                    spec.setType(java.sql.Types.BIT);
                return length;
            }

            case MysqlBinlog.MYSQL_TYPE_TIMESTAMP :
            {
                int offset = 0;

                Timestamp ts;
                long i32;
                int nanos = 0;

                if (spec != null)
                    spec.setType(java.sql.Types.TIMESTAMP);

                if (meta > 0)
                {
                    // MariaDB 10 TIMESTAMP datatype support
                    offset = TIMESTAMP_BYTES_PER_SUB_SECOND_DECIMAL[meta];
                    i32 = BigEndianConversion.convert4BytesToInt(row, rowPos);
                    long microsec = BigEndianConversion.convertNBytesToInt(row,
                            rowPos + 4, offset)
                            * SECOND_TO_MICROSECOND_MULTIPLIER[meta];
                    nanos = 1000 * (int) microsec;
                    if (nanos < 0 || nanos > 999999999)
                    {
                        logger.warn("Extracted a wrong number of nanoseconds : "
                                + nanos
                                + " - in ms, value was "
                                + microsec
                                + "("
                                + BigEndianConversion.convertNBytesToInt(row,
                                        rowPos + 4, offset)
                                + " * "
                                + SECOND_TO_MICROSECOND_MULTIPLIER[meta]
                                + " )"
                                + "- as hexa : "
                                + hexdump(row, rowPos + 4, offset));
                    }

                }
                else
                {
                    // MySQL TIMESTAMP standard datatype support
                    i32 = LittleEndianConversion.convertNBytesToLong_2(row,
                            rowPos, 4);
                }
                if (i32 == 0)
                {
                    value.setValue(Integer.valueOf(0));
                }
                else
                {
                    ts = new java.sql.Timestamp(i32 * 1000);
                    ts.setNanos(nanos);
                    value.setValue(ts);
                }
                return 4 + offset;
            }
            case MysqlBinlog.MYSQL_TYPE_TIMESTAMP2 :
            {
                // MYSQL 5.6 TIMESTAMP datatype support
                int secPartsLength = 0;
                long i32 = BigEndianConversion.convertNBytesToLong(row, rowPos,
                        4);

                if (logger.isDebugEnabled())
                {
                    logger.debug("Extracting timestamp "
                            + hexdump(row, rowPos, 4));
                    logger.debug("Meta value is " + meta);
                    logger.debug("Value as integer is " + i32);
                }
                if (i32 == 0)
                {
                    value.setValue(Integer.valueOf(0));
                    secPartsLength = getSecondPartsLength(meta);
                }
                else
                {
                    // convert sec based timestamp to millisecond precision
                    Timestamp tsVal = new java.sql.Timestamp(i32 * 1000);
                    if (logger.isDebugEnabled())
                        logger.debug("Setting value to " + tsVal);

                    value.setValue(tsVal);
                    secPartsLength = getSecondPartsLength(meta);
                    rowPos += 4;
                    tsVal.setNanos(extractNanoseconds(row, rowPos, meta,
                            secPartsLength));
                }

                if (spec != null)
                    spec.setType(java.sql.Types.TIMESTAMP);
                return 4 + secPartsLength;
            }
            case MysqlBinlog.MYSQL_TYPE_DATETIME :
            {
                java.sql.Timestamp ts = null;
                int year, month, day, hour, min, sec, nanos = 0;

                long i64 = 0;
                int offset;
                if (meta == 0)
                {
                    // MYSQL standard DATETIME datatype support
                    offset = 8;
                    i64 = LittleEndianConversion.convert8BytesToLong(row,
                            rowPos); /* YYYYMMDDhhmmss */

                    // Let's check for zero date
                    if (i64 == 0)
                    {
                        value.setValue(Integer.valueOf(0));
                        if (spec != null)
                            spec.setType(java.sql.Types.TIMESTAMP);
                        return offset;
                    }
                    // calculate year, month...sec components of timestamp
                    long d = i64 / 1000000;
                    year = (int) (d / 10000);
                    month = (int) (d % 10000) / 100;
                    day = (int) (d % 10000) % 100;

                    long t = i64 % 1000000;
                    hour = (int) (t / 10000);
                    min = (int) (t % 10000) / 100;
                    sec = (int) (t % 10000) % 100;
                    offset = 8;
                }
                else
                {
                    // MariaDB 10 DATETIME datatype support
                    offset = DATETIME_BYTES_PER_SUB_SECOND_DECIMAL[meta];

                    if (logger.isDebugEnabled())
                        logger.debug("Handling MariaDB 10 datetime datatype");
                    if (meta < 0)
                    {
                        meta = 0;
                    }
                    i64 = BigEndianConversion.convertNBytesToLong(row, rowPos,
                            DATETIME_BYTES_PER_SUB_SECOND_DECIMAL[meta])
                            * SECOND_TO_MICROSECOND_MULTIPLIER[meta];

                    // Let's check for zero date
                    if (i64 == 0)
                    {
                        value.setValue(Integer.valueOf(0));
                        if (spec != null)
                            spec.setType(java.sql.Types.TIMESTAMP);
                        return offset;
                    }

                    nanos = (int) (i64 % 1000000L) * 1000;
                    i64 /= 1000000L;
                    sec = (int) (i64 % 60L);
                    i64 /= 60L;
                    min = (int) (i64 % 60L);
                    i64 /= 60L;
                    hour = (int) (i64 % 24L);
                    i64 /= 24L;
                    day = (int) (i64 % 32L);
                    i64 /= 32L;
                    month = (int) (i64 % 13L);
                    i64 /= 13L;
                    year = (int) i64;
                    offset = DATETIME_BYTES_PER_SUB_SECOND_DECIMAL[meta];
                }

                // Force the use of GMT as calendar for DATETIME datatype
                Calendar cal = Calendar
                        .getInstance(TimeZone.getTimeZone("GMT"));

                // Month value is 0-based. e.g., 0 for January.
                cal.set(year, month - 1, day, hour, min, sec);

                ts = new Timestamp(cal.getTimeInMillis());

                ts.setNanos(nanos);

                value.setValue(ts);
                if (spec != null)
                    spec.setType(java.sql.Types.DATE);
                return offset;
            }
            case MysqlBinlog.MYSQL_TYPE_DATETIME2 :
            {
                // MYSQL 5.6 DATETIME datatype support
                /**
                 * 1 bit sign (used when on disk)<br>
                 * 17 bits year*13+month (year 0-9999, month 0-12)<br>
                 * 5 bits day (0-31)<br>
                 * 5 bits hour (0-23)<br>
                 * 6 bits minute (0-59)<br>
                 * 6 bits second (0-59)<br>
                 * 24 bits microseconds (0-999999)<br>
                 * Total: 64 bits = 8 bytes SYYYYYYY.YYYYYYYY.YYdddddh
                 * .hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
                 */
                long i64 = BigEndianConversion.convertNBytesToLong(row, rowPos,
                        5) - 0x8000000000L;
                int secPartsLength = 0;

                // Let's check for zero date
                if (i64 == 0)
                {
                    value.setValue(Integer.valueOf(0));
                    secPartsLength = getSecondPartsLength(meta);
                    rowPos += 5;
                    if (logger.isDebugEnabled())
                        logger.debug("Got nanos = "
                                + extractNanoseconds(row, rowPos, meta,
                                        secPartsLength));

                    if (spec != null)
                        spec.setType(java.sql.Types.TIMESTAMP);
                    return 5 + secPartsLength;
                }

                long currentValue = (i64 >> 22);
                int year = (int) (currentValue / 13);
                int month = (int) (currentValue % 13);

                long previousValue = currentValue;
                currentValue = i64 >> 17;
                int day = (int) (currentValue - (previousValue << 5));

                previousValue = currentValue;
                currentValue = (i64 >> 12);

                int hour = (int) (currentValue - (previousValue << 5));

                previousValue = currentValue;
                currentValue = (i64 >> 6);

                int minute = (int) (currentValue - (previousValue << 6));

                previousValue = currentValue;
                currentValue = i64;

                int seconds = (int) (currentValue - (previousValue << 6));
                if (logger.isDebugEnabled())
                    logger.debug("Time " + hour + ":" + minute + ":" + seconds);

                // construct timestamp from time components
                java.sql.Timestamp ts = null;

                // Calendar cal = Calendar.getInstance();
                // Force the use of GMT as calendar
                Calendar cal = Calendar
                        .getInstance(TimeZone.getTimeZone("GMT"));

                // Month value is 0-based. e.g., 0 for January.
                cal.set(year, month - 1, day, hour, minute, seconds);

                ts = new Timestamp(cal.getTimeInMillis());

                value.setValue(ts);
                if (spec != null)
                    spec.setType(java.sql.Types.DATE);

                secPartsLength = getSecondPartsLength(meta);
                rowPos += 5;
                ts.setNanos(extractNanoseconds(row, rowPos, meta,
                        secPartsLength));

                return 5 + secPartsLength;
            }
            case MysqlBinlog.MYSQL_TYPE_TIME :
            {
                Time time;
                Timestamp tsVal;
                int offset;

                if (meta == 0)
                {
                    // MYSQL standard TIME datatype support
                    offset = 3;
                    long i32 = LittleEndianConversion.convert3BytesToInt(row,
                            rowPos);
                    time = java.sql.Time.valueOf(i32 / 10000 + ":"
                            + (i32 % 10000) / 100 + ":" + i32 % 100);
                    value.setValue(time);
                }
                else
                {
                    // MariaDB 10 TIME datatype support
                    if (meta < 0)
                    {
                        meta = 0;
                        logger.warn("Negative metadata detected");
                    }

                    offset = TIME_BYTES_PER_SUB_SECOND_DECIMAL[meta];

                    long i64 = BigEndianConversion.convertNBytesToLong(row,
                            rowPos, offset)
                            * SECOND_TO_MICROSECOND_MULTIPLIER[meta];

                    i64 -= MAX_TIME;

                    if (logger.isDebugEnabled())
                        logger.debug("Extracted value is " + i64);

                    // Convert microseconds to nanoseconds
                    int nanos = (int) (i64 % 1000000L) * 1000;

                    i64 /= 1000000L;
                    int sec = (int) (i64 % 60L);
                    i64 /= 60L;
                    int min = (int) (i64 % 60L);
                    i64 /= 60L;
                    int hour = (int) (i64 % 24L);
                    time = java.sql.Time.valueOf(hour + ":" + min + ":" + sec);
                    tsVal = new java.sql.Timestamp(time.getTime());
                    tsVal.setNanos(nanos);
                    value.setValue(tsVal);
                }

                if (spec != null)
                    spec.setType(java.sql.Types.TIME);
                return offset;
            }

            case MysqlBinlog.MYSQL_TYPE_TIME2 :
            {
                // MYSQL 5.6 TIME datatype support
                /**
                 * 1 bit sign (Used for sign, when on disk)<br>
                 * 1 bit unused (Reserved for wider hour range, e.g. for
                 * intervals)<br>
                 * 10 bit hour (0-836)<br>
                 * 6 bit minute (0-59)<br>
                 * 6 bit second (0-59)<br>
                 * 24 bits microseconds (0-999999)<br>
                 * Total: 48 bits = 6 bytes
                 * Suhhhhhh.hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
                 */
                if (logger.isDebugEnabled())
                    logger.debug("Extracting TIME2 from position " + rowPos
                            + " : " + hexdump(row, rowPos, 3));
                long i32 = (BigEndianConversion.convert3BytesToInt(row, rowPos) - 0x800000L) & 0xBFFFFFL;

                long currentValue = (i32 >> 12);
                int hours = (int) currentValue;

                long previousValue = currentValue;
                currentValue = i32 >> 6;
                int minutes = (int) (currentValue - (previousValue << 6));

                previousValue = currentValue;
                currentValue = i32;
                int seconds = (int) (currentValue - (previousValue << 6));

                Time time = java.sql.Time.valueOf(hours + ":" + minutes + ":"
                        + seconds);

                Timestamp tsVal = new java.sql.Timestamp(time.getTime());
                value.setValue(tsVal);

                int secPartsLength = getSecondPartsLength(meta);
                rowPos += 3;
                int nanoseconds = extractNanoseconds(row, rowPos, meta,
                        secPartsLength);
                tsVal.setNanos(nanoseconds);

                if (spec != null)
                    spec.setType(java.sql.Types.TIME);
                return 3 + secPartsLength;
            }

            case MysqlBinlog.MYSQL_TYPE_DATE :
            {
                int i32 = 0;
                i32 = LittleEndianConversion.convert3BytesToInt(row, rowPos);
                java.sql.Date date = null;

                // Let's check if the date is 0000-00-00
                if (i32 == 0)
                {
                    value.setValue(Integer.valueOf(0));
                    if (spec != null)
                        spec.setType(java.sql.Types.DATE);
                    return 3;
                }

                Calendar cal = Calendar.getInstance();
                cal.clear();
                // Month value is 0-based. e.g., 0 for January.
                cal.set(i32 / (16 * 32), (i32 / 32 % 16) - 1, i32 % 32);

                date = new Date(cal.getTimeInMillis());

                value.setValue(date);
                if (spec != null)
                    spec.setType(java.sql.Types.DATE);
                return 3;
            }

            case MysqlBinlog.MYSQL_TYPE_YEAR :
            {
                int i32 = LittleEndianConversion.convert1ByteToInt(row, rowPos);
                // raw value is offset by 1900. e.g. "1" is 1901.
                value.setValue(1900 + i32);
                // It might seem more correct to create a java.sql.Types.DATE
                // value for this date, but it is much simpler to pass the value
                // as an integer. The MySQL JDBC specification states that one
                // can pass a java int between 1901 and 2055. Creating a DATE
                // value causes truncation errors with certain SQL_MODES
                // (e.g."STRICT_TRANS_TABLES").
                if (spec != null)
                    spec.setType(java.sql.Types.INTEGER);
                return 1;
            }

            case MysqlBinlog.MYSQL_TYPE_ENUM :
                switch (length)
                {
                    case 1 :
                    {
                        int i32 = LittleEndianConversion.convert1ByteToInt(row,
                                rowPos);
                        value.setValue(new Integer(i32));
                        if (spec != null)
                            spec.setType(java.sql.Types.OTHER);
                        return 1;
                    }
                    case 2 :
                    {
                        int i32 = LittleEndianConversion.convert2BytesToInt(
                                row, rowPos);
                        value.setValue(new Integer(i32));
                        if (spec != null)
                            spec.setType(java.sql.Types.INTEGER);
                        return 2;
                    }
                    default :
                        return 0;
                }

            case MysqlBinlog.MYSQL_TYPE_SET :
                long val = LittleEndianConversion.convertNBytesToLong_2(row,
                        rowPos, length);
                value.setValue(new Long(val));
                if (spec != null)
                    spec.setType(java.sql.Types.INTEGER);
                return length;

            case MysqlBinlog.MYSQL_TYPE_BLOB :
                /*
                 * BLOB or TEXT datatype
                 */
                if (spec != null)
                    spec.setType(java.sql.Types.BLOB);
                int blob_size = 0;
                switch (meta)
                {
                    case 1 :
                        length = GeneralConversion
                                .unsignedByteToInt(row[rowPos]);
                        blob_size = 1;
                        break;
                    case 2 :
                        length = LittleEndianConversion.convert2BytesToInt(row,
                                rowPos);
                        blob_size = 2;
                        break;
                    case 3 :
                        length = LittleEndianConversion.convert3BytesToInt(row,
                                rowPos);
                        blob_size = 3;
                        break;
                    case 4 :
                        length = (int) LittleEndianConversion
                                .convert4BytesToLong(row, rowPos);
                        blob_size = 4;
                        break;
                    default :
                        logger.error("Unknown BLOB packlen= " + length);
                        return 0;
                }
                try
                {
                    SerialBlob blob = DatabaseHelper.getSafeBlob(row, rowPos
                            + blob_size, length);
                    value.setValue(blob);
                }
                catch (SQLException e)
                {
                    throw new MySQLExtractException(
                            "Failure while extracting blob", e);
                }
                if (spec != null)
                {
                    spec.setType(java.sql.Types.BLOB);
                }
                return length + blob_size;

            case MysqlBinlog.MYSQL_TYPE_VARCHAR :
            case MysqlBinlog.MYSQL_TYPE_VAR_STRING :
                /*
                 * Except for the data length calculation, MYSQL_TYPE_VARCHAR,
                 * MYSQL_TYPE_VAR_STRING and MYSQL_TYPE_STRING are handled the
                 * same way
                 */
                length = meta;
                if (length < 256)
                {
                    length = LittleEndianConversion.convert1ByteToInt(row,
                            rowPos);
                    rowPos++;
                    if (useBytesForString)
                        value.setValue(processStringAsBytes(row, rowPos, length));
                    else
                        value.setValue(processString(row, rowPos, length));
                    length += 1;
                }
                else
                {
                    length = LittleEndianConversion.convert2BytesToInt(row,
                            rowPos);
                    rowPos += 2;
                    if (useBytesForString)
                        value.setValue(processStringAsBytes(row, rowPos, length));
                    else
                        value.setValue(processString(row, rowPos, length));
                    length += 2;
                }

                if (spec != null)
                    spec.setType(java.sql.Types.VARCHAR);
                return length;

            case MysqlBinlog.MYSQL_TYPE_STRING :
                if (length < 256)
                {
                    length = LittleEndianConversion.convert1ByteToInt(row,
                            rowPos);
                    rowPos++;
                    if (useBytesForString)
                        value.setValue(processStringAsBytes(row, rowPos, length));
                    else
                        value.setValue(processString(row, rowPos, length));
                    length += 1;
                }
                else
                {
                    length = LittleEndianConversion.convert2BytesToInt(row,
                            rowPos);
                    rowPos += 2;
                    if (useBytesForString)
                        value.setValue(processStringAsBytes(row, rowPos, length));
                    else
                        value.setValue(processString(row, rowPos, length));
                    length += 2;
                }
                if (spec != null)
                    spec.setType(java.sql.Types.VARCHAR);
                return length;

            default :
            {
                throw new MySQLExtractException("unknown data type " + type);
            }
        }
    }

    private int extractNanoseconds(byte[] row, int rowPos, int meta,
            int secPartsLength)
    {
        if (meta > 0)
        {
            // Extract second parts
            int readValue = BigEndianConversion.convertNBytesToInt(row, rowPos,
                    secPartsLength);

            int i = readValue * 1000;
            switch (meta)
            {
                case 1 :
                case 2 :
                    i *= 10000;
                    break;
                case 3 :
                case 4 :
                    i *= 100;
                    break;
                case 5 :
                case 6 :
                    break;
                default :
                    break;
            }

            return i;
        }
        return 0;
    }

    private int getSecondPartsLength(int meta)
    {
        return (meta + 1) / 2;
    }

    // JIRA TREP-237. Need to expose the table ID.
    protected long getTableId()
    {
        return tableId;
    }

    private byte[] processStringAsBytes(byte[] buffer, int pos, int length)
            throws ReplicatorException
    {
        byte[] output = new byte[length];
        System.arraycopy(buffer, pos, output, 0, length);
        return output;
    }

    protected String processString(byte[] buffer, int pos, int length)
            throws ReplicatorException
    {
        return new String(buffer, pos, length);
    }

    protected int processExtractedEventRow(OneRowChange oneRowChange,
            int rowIndex, BitSet cols, int rowPos, byte[] row,
            TableMapLogEvent map, boolean isKeySpec) throws ReplicatorException
    {
        int startIndex = rowPos;
        if (logger.isDebugEnabled())
        {
            logger.debug("processExtractedEventRow " + hexdump(row)
                    + " from position " + startIndex);
            logger.debug(oneRowChange.getAction().toString() + " for table "
                    + oneRowChange.getSchemaName() + "."
                    + oneRowChange.getTableName());
        }
        int usedColumnsCount = 0;
        for (int i = 0; i < columnsNumber; i++)
        {
            if (cols.get(i))
                usedColumnsCount++;
        }
        BitSet nulls = new BitSet(usedColumnsCount);
        MysqlBinlog.setBitField(nulls, row, startIndex, usedColumnsCount);

        ArrayList<ArrayList<OneRowChange.ColumnVal>> rows = (isKeySpec)
                ? oneRowChange.getKeyValues()
                : oneRowChange.getColumnValues();

        /*
         * add new row for column values
         */
        if (rows.size() == rowIndex)
        {
            rows.add(new ArrayList<ColumnVal>());
        }
        ArrayList<OneRowChange.ColumnVal> columns = rows.get(rowIndex);

        if (columns == null)
        {
            throw new ExtractorException(
                    "Row data corrupted : column value list empty for row "
                            + oneRowChange.toString());
        }
        rowPos += (usedColumnsCount + 7) / 8;

        OneRowChange.ColumnSpec spec = null;
        int nullIndex = 0;

        int colCount = 0;
        for (int i = 0; i < map.getColumnsCount(); i++)
        {

            if (logger.isDebugEnabled())
                logger.debug("Extracting column " + (i + 1) + " out of "
                        + map.getColumnsCount());

            if (cols.get(i) == false)
                continue;

            boolean isNull = nulls.get(nullIndex);
            nullIndex++;

            OneRowChange.ColumnVal value = oneRowChange.new ColumnVal();
            if (isKeySpec)
            {
                if (rowIndex == 0)
                {
                    spec = oneRowChange.new ColumnSpec();
                    spec.setIndex(i + 1);
                    oneRowChange.getKeySpec().add(spec);
                }
                else
                {
                    // Check if column was null until now
                    ColumnSpec keySpec = oneRowChange.getKeySpec()
                            .get(colCount);
                    if (keySpec != null
                            && keySpec.getType() == java.sql.Types.NULL
                            && !isNull)
                    {
                        spec = keySpec;
                    }
                    else
                        spec = null;
                }
                oneRowChange.getKeyValues().get(rowIndex).add(value);
            }
            else
            {
                if (rowIndex == 0)
                {
                    spec = oneRowChange.new ColumnSpec();
                    spec.setIndex(i + 1);
                    oneRowChange.getColumnSpec().add(spec);
                }
                else
                {
                    // Check if column was null until now
                    ColumnSpec columnSpec = oneRowChange.getColumnSpec().get(
                            colCount);

                    if (columnSpec != null
                            && columnSpec.getType() == java.sql.Types.NULL
                            && !isNull)
                    {
                        spec = columnSpec;
                    }
                    else
                        spec = null;
                }
                oneRowChange.getColumnValues().get(rowIndex).add(value);
            }
            if (isNull)
            {
                value.setValueNull();
            }
            else
            {
                int size = 0;
                try
                {
                    size = extractValue(
                            spec,
                            value,
                            row,
                            rowPos,
                            LittleEndianConversion.convert1ByteToInt(
                                    map.getColumnsTypes(), i),
                            map.getMetadata()[i], map);
                }
                catch (IOException e)
                {
                    throw new ExtractorException(
                            "Row column value parsing failure", e);
                }
                if (size == 0)
                {
                    return 0;
                }
                rowPos += size;
            }
            colCount++;
        }

        return rowPos - startIndex;
    }

    private void readSessionVariables(byte[] buffer, int pos)
            throws IOException
    {
        String sessionVariables;
        int flags;

        final int OPTION_NO_FOREIGN_KEY_CHECKS = 1 << 1;
        final int OPTION_RELAXED_UNIQUE_CHECKS = 1 << 2;

        flags = LittleEndianConversion.convert2BytesToInt(buffer, pos);

        flagForeignKeyChecks = (flags & OPTION_NO_FOREIGN_KEY_CHECKS) != OPTION_NO_FOREIGN_KEY_CHECKS;
        flagUniqueChecks = (flags & OPTION_RELAXED_UNIQUE_CHECKS) != OPTION_RELAXED_UNIQUE_CHECKS;

        sessionVariables = "set @@session.foreign_key_checks="
                + (flagForeignKeyChecks ? 1 : 0) + ", @@session.unique_checks="
                + (flagUniqueChecks ? 1 : 0);

        if (logger.isDebugEnabled())
        {
            logger.debug(sessionVariables);
        }
    }

    /**
     * Returns the flagForeignKeyChecks value.
     * 
     * @return Returns the flagForeignKeyChecks.
     */
    public String getForeignKeyChecksFlag()
    {
        return (flagForeignKeyChecks ? "1" : "0");
    }

    /**
     * Returns the flagUniqueChecks value.
     * 
     * @return Returns the flagUniqueChecks.
     */
    public String getUniqueChecksFlag()
    {
        return (flagUniqueChecks ? "1" : "0");
    }
}
