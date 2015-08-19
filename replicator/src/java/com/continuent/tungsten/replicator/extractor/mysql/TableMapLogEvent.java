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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.BigEndianConversion;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class TableMapLogEvent extends LogEvent
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
     * <li>1 byte. The length of the database name.</li>
     * <li>Variable-sized. The database name (null-terminated).</li>
     * <li>1 byte. The length of the table name.</li>
     * <li>Variable-sized. The table name (null-terminated).</li>
     * <li>Packed integer. The number of columns in the table.</li>
     * <li>Variable-sized. An array of column types, one byte per column.</li>
     * <li>Packed integer. The length of the metadata block.</li>
     * <li>Variable-sized. The metadata block; see log_event.h for contents and
     * format.</li>
     * <li>Variable-sized. Bit-field indicating whether each column can be NULL,
     * one bit per column. For this field, the amount of storage required for N
     * columns is INT((N+7)/8) bytes.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    static Logger                logger            = Logger.getLogger(TableMapLogEvent.class);

    private long                 tableId;
    private int                  databaseNameLength;
    private int                  tableNameLength;
    private long                 columnsCount;

    private String               databaseName;
    private String               tableName;

    private byte[]               columnsTypes;

    // Not used for now...
    // private String nullBits;

    private int[]                metadata;
    private int                  metadataSize;

    // MariaDB 10 support
    private Table                table;

    private static final Pattern TIMESTAMP_PATTERN = Pattern
                                                           .compile(
                                                                   "timestamp(\\(([0-6])\\))?",
                                                                   Pattern.CASE_INSENSITIVE);

    private static final Pattern DATETIME_PATTERN  = Pattern
                                                           .compile(
                                                                   "datetime(\\(([0-6])\\))?",
                                                                   Pattern.CASE_INSENSITIVE);

    private static final Pattern TIME_PATTERN      = Pattern
                                                           .compile(
                                                                   "time(\\(([0-6])\\))?",
                                                                   Pattern.CASE_INSENSITIVE);

    public long getTableId()
    {
        return tableId;
    }

    public long getColumnsCount()
    {
        return columnsCount;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public byte[] getColumnsTypes()
    {
        return columnsTypes;
    }

    public int[] getMetadata()
    {
        return metadata;
    }

    private void buildMetadata(byte[] fieldMetadata, int pos, int length)
            throws IOException
    {
        int index = pos;
        for (int i = 0; i < columnsCount; i++)
        {
            int columnType = LittleEndianConversion.convert1ByteToInt(
                    columnsTypes, i);
            switch (columnType)
            {
                case MysqlBinlog.MYSQL_TYPE_TINY_BLOB :
                case MysqlBinlog.MYSQL_TYPE_BLOB :
                case MysqlBinlog.MYSQL_TYPE_MEDIUM_BLOB :
                case MysqlBinlog.MYSQL_TYPE_LONG_BLOB :
                case MysqlBinlog.MYSQL_TYPE_DOUBLE :
                case MysqlBinlog.MYSQL_TYPE_FLOAT :
                    /* These types store a single byte */
                    metadata[i] = fieldMetadata[index];
                    index++;
                    break;

                case MysqlBinlog.MYSQL_TYPE_SET :
                case MysqlBinlog.MYSQL_TYPE_ENUM :
                    /*
                     * log_event.h : MYSQL_TYPE_SET & MYSQL_TYPE_ENUM : This
                     * enumeration value is only used internally and cannot
                     * exist in a binlog.
                     */
                case MysqlBinlog.MYSQL_TYPE_STRING :
                {
                    /*
                     * log_event.h : The first byte is always
                     * MYSQL_TYPE_VAR_STRING (i.e., 253). The second byte is the
                     * field size, i.e., the number of bytes in the
                     * representation of size of the string: 3 or 4.
                     */
                    int x = BigEndianConversion.convert2BytesToInt(
                            fieldMetadata, index);
                    metadata[i] = x;
                    index += 2;
                    break;
                }
                case MysqlBinlog.MYSQL_TYPE_BIT :
                {
                    int x = LittleEndianConversion.convert2BytesToInt(
                            fieldMetadata, index);
                    metadata[i] = x;
                    metadata[i] = x;
                    index += 2;
                    break;
                }
                case MysqlBinlog.MYSQL_TYPE_VARCHAR :
                {
                    /*
                     * These types store two bytes.
                     */
                    metadata[i] = LittleEndianConversion.convert2BytesToInt(
                            fieldMetadata, index);
                    index = index + 2;
                    break;
                }
                case MysqlBinlog.MYSQL_TYPE_NEWDECIMAL :
                {
                    int x = BigEndianConversion.convert2bytesToShort(
                            fieldMetadata, index);
                    metadata[i] = x;
                    index += 2;
                    break;
                }
                case MysqlBinlog.MYSQL_TYPE_TIME2 :
                case MysqlBinlog.MYSQL_TYPE_TIMESTAMP2 :
                case MysqlBinlog.MYSQL_TYPE_DATETIME2 :
                    metadata[i] = fieldMetadata[index];
                    index++;
                    break;

                default :
                    metadata[i] = 0;
                    break;
            }
            if (logger.isDebugEnabled())
                logger.debug("column: " + i + " type: " + columnType
                        + " length: " + metadata[i]);
        }
    }

    public TableMapLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, String currentPosition)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent, MysqlBinlog.TABLE_MAP_EVENT);
        this.startPosition = currentPosition;
        int commonHeaderLength, postHeaderLength;

        int postHeaderIndex;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength
                    + " common header length: " + commonHeaderLength
                    + " post_header_len: " + postHeaderLength);

        try
        {
            /* Read the post-header */
            postHeaderIndex = commonHeaderLength;

            postHeaderIndex += MysqlBinlog.TM_MAPID_OFFSET;
            if (postHeaderLength == 6)
            {
                /*
                 * Master is of an intermediate source tree before 5.1.4. Id is
                 * 4 bytes
                 */
                tableId = LittleEndianConversion.convert4BytesToLong(buffer,
                        postHeaderIndex);
                postHeaderIndex += 4;
            }
            else
            {
                assert (postHeaderLength == MysqlBinlog.TABLE_MAP_HEADER_LEN);
                tableId = LittleEndianConversion.convert6BytesToLong(buffer,
                        postHeaderIndex);
                postHeaderIndex += MysqlBinlog.TM_FLAGS_OFFSET;
            }

            /*
             * Next 2 bytes are reserved for future use : no need to process
             * them for now.
             */

            /* Read the variable data part of the event */
            int variableStartIndex = commonHeaderLength + postHeaderLength;

            /* Extract the length of the various parts from the buffer */
            int index = variableStartIndex + 0;
            databaseNameLength = LittleEndianConversion.convert1ByteToInt(
                    buffer, index);
            index++;
            databaseName = new String(buffer, index, databaseNameLength);

            /* Length of database name + terminating null */
            index += databaseNameLength + 1;

            // int ptr_tbllen = index + databaseNameLength + 2;
            tableNameLength = LittleEndianConversion.convert1ByteToInt(buffer,
                    index);
            index++;
            tableName = new String(buffer, index, tableNameLength);

            /* Length of table name + terminating null */
            index += tableNameLength + 1;

            long ret[] = MysqlBinlog.decodePackedInteger(buffer, index);
            columnsCount = ret[0];
            index = (int) ret[1];

            if (logger.isDebugEnabled())
                logger.debug("Db Name : " + databaseName + " ("
                        + databaseNameLength + ")" + " Tablename : "
                        + tableName + " (" + tableNameLength + ")"
                        + " Columns count: " + columnsCount);

            columnsTypes = new byte[(int) columnsCount];
            System.arraycopy(buffer, index, columnsTypes, 0, (int) columnsCount);

            index += columnsCount;

            if (logger.isDebugEnabled())
                logger.debug("Bytes read: " + index);

            /* initialize field metadata according to table column count */
            metadataSize = (int) columnsCount * 2;
            metadata = new int[metadataSize];
            for (int i = 0; i < columnsCount * 2; i++)
                metadata[i] = 0;

            if (index < eventLength)
            {
                ret = MysqlBinlog.decodePackedInteger(buffer, index);
                int metadata_size = (int) ret[0];
                index = (int) ret[1];
                assert (metadata_size <= (columnsCount * 2));

                buildMetadata(buffer, index, metadata_size);
                index += metadata_size;

                // For now, the following is not used
                // int nullBytesCount = ((int) columnsCount + 7) / 8;
                // nullBits = new String(buffer, index,
                // nullBytesCount);
            }
        }
        catch (IOException e)
        {
            logger.error("Table Map event parsing failed ", e);
        }
        return;
    }

    /**
     * Add to this event the table metadata fetched from database. This is used
     * for MariaDB 10 support. Metadata is then used to check whether
     * datetime/time/timestamp special handling is required (subsecond
     * precision). If so, metadata[datetime_column] is set to the number of
     * digits of subseconds, whereas it is 0 by default.
     * 
     * @param table Table metadata
     */
    public void setTable(Table table)
    {
        // Table metadata
        this.table = table;

        int columnType;
        for (Column column : table.getAllColumns())
        {
            columnType = -1;
            int position = column.getPosition() - 1;
            try
            {
                columnType = LittleEndianConversion.convert1ByteToInt(
                        columnsTypes, position);
            }
            catch (IOException e)
            {
            }

            Matcher matcher;

            switch (columnType)
            {
                case MysqlBinlog.MYSQL_TYPE_DATETIME :
                    if (logger.isDebugEnabled())
                        logger.debug("Handling DATETIME column " + position
                                + " / " + column.getTypeDescription());

                    matcher = DATETIME_PATTERN.matcher((column
                            .getTypeDescription()));
                    if (matcher.matches())
                    {
                        if (matcher.group(1) == null)
                        {
                            // Handling type DATETIME with no extra precision
                            metadata[position] = 0;
                        }
                        else
                        {
                            // Handling type DATETIME(i) with 0 <= i <= 6
                            Integer value = Integer.valueOf(matcher.group(2));
                            metadata[position] = (value == 0 ? -1 : value);
                            // Note that we use metadata field which is not used
                            // inside the binlog to describe the number of
                            // digits for second parts
                        }
                    }
                    break;
                case MysqlBinlog.MYSQL_TYPE_TIME :
                    if (logger.isDebugEnabled())
                        logger.debug("Handling DATETIME column " + position
                                + " / " + column.getTypeDescription());

                    matcher = TIME_PATTERN
                            .matcher((column.getTypeDescription()));
                    if (matcher.matches())
                    {
                        if (matcher.group(1) == null)
                        {
                            // Handling type TIME with no extra precision
                            metadata[position] = 0;
                        }
                        else
                        {
                            // Handling type TIME(i) with 0 <= i <= 6
                            Integer value = Integer.valueOf(matcher.group(2));
                            metadata[position] = (value == 0 ? -1 : value);
                            // Note that we use metadata field which is not used
                            // inside the binlog to describe the number of
                            // digits for second parts
                        }
                    }
                    break;
                case MysqlBinlog.MYSQL_TYPE_TIMESTAMP :
                    if (logger.isDebugEnabled())
                        logger.debug("Handling TIMESTAMP column " + position
                                + " / " + column.getTypeDescription());

                    matcher = TIMESTAMP_PATTERN.matcher((column
                            .getTypeDescription()));
                    if (matcher.matches())
                    {
                        if (matcher.group(1) == null)
                        {
                            // Handling type TIMESTAMP with no extra precision
                            metadata[position] = 0;
                        }
                        else
                        {
                            // Handling type TIMESTAMP(i) with 0 <= i <= 6
                            Integer value = Integer.valueOf(matcher.group(2));
                            metadata[position] = (value == 0 ? -1 : value);
                            // Note that we use metadata field which is not used
                            // inside the binlog to describe the number of
                            // digits for second parts
                        }
                    }
                    break;
                default :
                    break;
            }
        }
    }

    /**
     * Returns the table value.
     * 
     * @return Returns the table.
     */
    public Table getTable()
    {
        return table;
    }

}
