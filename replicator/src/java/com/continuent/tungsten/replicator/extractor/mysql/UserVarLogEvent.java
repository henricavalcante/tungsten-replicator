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
public class UserVarLogEvent extends LogEvent
{
    /**
     * Fixed data part: Empty
     * <p>
     * Variable data part:
     * <ul>
     * <li>4 bytes. the size of the user variable name.</li>
     * <li>The user variable name.</li>
     * <li>1 byte. Non-zero if the variable value is the SQL NULL value, 0
     * otherwise. If this byte is 0, the following parts exist in the event.</li>
     * <li>1 byte. The user variable type. The value corresponds to elements of
     * enum Item_result defined in include/mysql_com.h.</li>
     * <li>4 bytes. The number of the character set for the user variable
     * (needed for a string variable). The character set number is really a
     * collation number that indicates a character set/collation pair.</li>
     * <li>4 bytes. The size of the user variable value (corresponds to member
     * val_len of class Item_string).</li>
     * <li>Variable-sized. For a string variable, this is the string. For a
     * float or integer variable, this is its value in 8 bytes.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    static Logger            logger         = Logger.getLogger(UserVarLogEvent.class);

    private String           query;
    private int              variableNameLength;
    private int              variableNameIndex;
    private boolean          variableIsNull;
    private int              variableType;
    private int              variableValueLength;
    private int              variableValueIndex;

    // Variable type markers from in binlog internal spec description of
    // USER_VAR_EVENT.
    private static final int STRING_RESULT  = 0;
    private static final int REAL_RESULT    = 1;
    private static final int INT_RESULT     = 2;
    private static final int ROW_RESULT     = 3;
    private static final int DECIMAL_RESULT = 4;

    public String getQuery()
    {
        return query;
    }

    public UserVarLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, String currentPosition)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent, MysqlBinlog.USER_VAR_EVENT);

        this.startPosition = currentPosition;
        if (logger.isDebugEnabled())
            logger.debug("Extracting event at position  : " + startPosition
                    + " -> " + getNextEventPosition());

        int commonHeaderLength, postHeaderLength;
        int offset;

        String value = null;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength
                    + " common header length: " + commonHeaderLength
                    + " post header length: " + postHeaderLength);

        offset = commonHeaderLength + postHeaderLength;

        if (eventLength < offset)
        {
            throw new MySQLExtractException(
                    "user var event length is too short");
        }

        if (descriptionEvent.useChecksum())
        {
            // Removing the checksum from the size of the event
            eventLength -= 4;
        }

        try
        {
            variableNameLength = (int) LittleEndianConversion
                    .convert4BytesToLong(buffer, offset);
            variableNameIndex = offset = offset + MysqlBinlog.UV_NAME_LEN_SIZE;
            String variableName = new String(buffer, variableNameIndex,
                    variableNameLength);
            offset = offset + variableNameLength;
            variableIsNull = (LittleEndianConversion.convert1ByteToInt(buffer,
                    offset) != 0);
            if (variableIsNull)
            {
                variableType = STRING_RESULT;
                variableValueLength = 0;
                value = "NULL";
            }
            else
            {
                offset = offset + MysqlBinlog.UV_VAL_IS_NULL;
                variableType = LittleEndianConversion.convert1ByteToInt(buffer,
                        offset);
                offset = offset + MysqlBinlog.UV_VAL_TYPE_SIZE;
                offset = offset + MysqlBinlog.UV_CHARSET_NUMBER_SIZE;
                variableValueLength = (int) LittleEndianConversion
                        .convert4BytesToLong(buffer, offset);
                variableValueIndex = offset + MysqlBinlog.UV_VAL_LEN_SIZE;

                if (variableValueLength > (buffer.length - variableValueIndex))
                {
                    throw new ArrayIndexOutOfBoundsException(
                            "Found variable value len: " + variableValueLength
                                    + ", available bytes: "
                                    + (buffer.length - variableValueIndex));
                }

                switch (variableType)
                {
                    case STRING_RESULT :
                        // TODO: use charset info
                        value = "'"
                                + new String(buffer, variableValueIndex,
                                        variableValueLength).replaceAll("'",
                                        "''") + "'";
                        break;
                    case REAL_RESULT :
                        if (variableValueLength != 8)
                        {
                            throw new MySQLExtractException(
                                    "Unsupported user variable real value length: "
                                            + variableValueLength
                                            + " (expected 8)");
                        }
                        if (logger.isDebugEnabled())
                            logger.debug("Real value dump: "
                                    + hexdump(buffer, variableValueIndex));

                        // Real representation is 8 byte little-endian IEEE-754
                        long l = LittleEndianConversion.convert8BytesToLong_2(
                                buffer, variableValueIndex);
                        value = String.valueOf(Double.longBitsToDouble(l));
                        if (logger.isDebugEnabled())
                            logger.debug("Real value : long="
                                    + Long.toHexString(l) + ", double=" + value);
                        break;
                    case INT_RESULT :
                        if (variableValueLength == 8)
                        {
                            value = String.valueOf(LittleEndianConversion
                                    .convert8BytesToLong(buffer,
                                            variableValueIndex));
                        }
                        else if (variableValueLength == 4)
                        {
                            value = String.valueOf(LittleEndianConversion
                                    .convert4BytesToLong(buffer,
                                            variableValueIndex));
                        }
                        else
                        {
                            throw new MySQLExtractException(
                                    "Unsupported user variable integer value length: "
                                            + variableValueLength);
                        }
                        break;
                    case ROW_RESULT :
                        // this seems to be banned in MySQL altogether
                        throw new MySQLExtractException(
                                "ROW_RESULT user variable type is unsupported");
                    case DECIMAL_RESULT :
                        byte precision = buffer[variableValueIndex];
                        byte scale = buffer[variableValueIndex + 1];

                        if (logger.isDebugEnabled())
                            logger.debug("Decimal value dump: "
                                    + hexdump(buffer, variableValueIndex + 2)
                                    + " precision = " + precision + " scale = "
                                    + scale);
                        int bin_size = getDecimalBinarySize(precision, scale);
                        byte[] dec = new byte[bin_size];
                        for (int i = 0; i < bin_size; i++)
                            dec[i] = buffer[variableValueIndex + 2 + i];

                        value = extractDecimal(dec, precision, scale)
                                .toString();

                        break;
                    default :
                        throw new MySQLExtractException(
                                "Unsupported variable type: " + variableType);
                }
            }

            query = new String("SET @" + variableName + " := " + value);
            if (logger.isDebugEnabled())
                logger.debug("USER_VAR_EVENT: " + query);

            doChecksum(buffer, eventLength, descriptionEvent);
        }
        catch (Exception e)
        {
            throw new MySQLExtractException("Unable to read user var event", e);
        }
    }
}
