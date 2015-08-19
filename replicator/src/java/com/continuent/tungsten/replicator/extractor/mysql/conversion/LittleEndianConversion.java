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

package com.continuent.tungsten.replicator.extractor.mysql.conversion;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class LittleEndianConversion extends GeneralConversion
{

    /*
     * bytes to int conversion methods
     */
    public static int convert1ByteToInt(byte[] buffer, int offset)
            throws IOException
    {
        return convertNBytesToInt(buffer, offset, 1);
    }

    public static int convert2BytesToInt(byte[] buffer, int offset)
            throws IOException
    {
        return convertNBytesToInt(buffer, offset, 2);
    }

    public static int convert3BytesToInt(byte[] buffer, int offset)
            throws IOException
    {
        return convertNBytesToInt(buffer, offset, 3);
    }

    public static int convert4BytesToInt(byte[] buffer, int offset)
            throws IOException
    {
        return convertNBytesToInt(buffer, offset, 4);
    }

    private static int convertNBytesToInt(byte[] buffer, int offset, int nBytes)
            throws IOException
    {
        int value = 0;
        int shift = 0;
        ByteArrayInputStream byte_in = new ByteArrayInputStream(buffer);
        DataInputStream data_in = new DataInputStream(byte_in);

        /* skip the offset */
        data_in.skip(offset);

        for (int i = 0; i < nBytes; i++)
        {
            value += data_in.readUnsignedByte() << shift;
            shift += 8;
        }
        return value;
    }

    public static long convertSignedNBytesToLong(byte[] buffer, int offset, int length)
    {
        long ret = 0;
        int shift = 0;
        for (int i = offset; i < (offset + length); i++, shift += 8)
        {
            if (i == (offset + length - 1))
            {
                // Don't clear the sign from the last extracted value
                ret = ret | ((long) buffer[i] << shift);
            }
            else
            {
                ret = ret | ((long) unsignedByteToInt(buffer[i]) << shift);
            }
        }
        return ret;
    }

    /*
     * bytes to long conversion methods
     */
    public static long convert4BytesToLong(byte[] buffer, int offset)
            throws IOException
    {
        return convertNBytesToLong(buffer, offset, 4);
    }

    public static long convert6BytesToLong(byte[] buffer, int offset)
            throws IOException
    {
        // ByteArrayInputStream byte_in = new ByteArrayInputStream(buf);
        // DataInputStream data_in = new DataInputStream(byte_in);
        // /* skip the offset */
        // data_in.skip(off);
        //
        // long value;
        // value = data_in.readUnsignedByte();
        // value += (data_in.readUnsignedByte() << 8);
        // value += (data_in.readUnsignedByte() << 16);
        // value += (data_in.readUnsignedByte() << 24);
        // value += (data_in.readUnsignedByte() << 32);
        // value += (data_in.readUnsignedByte() << 40);
        // return value;
        return convertNBytesToLong(buffer, offset, 6);
    }

    public static long convert8BytesToLong(byte[] buffer, int offset)
            throws IOException
    {
        ByteArrayInputStream byte_in = new ByteArrayInputStream(buffer);
        DataInputStream data_in = new DataInputStream(byte_in);
        /* skip the offset */
        data_in.skip(offset);
        //
        long value;
        value = data_in.readUnsignedByte();
        value += (long) data_in.readUnsignedByte() << 8;
        value += (long) data_in.readUnsignedByte() << 16;
        value += (long) data_in.readUnsignedByte() << 24;
        value += (long) data_in.readUnsignedByte() << 32;
        value += (long) data_in.readUnsignedByte() << 40;
        value += (long) data_in.readUnsignedByte() << 48;
        value += (long) data_in.readUnsignedByte() << 56;
        return value;
        // return convertNBytesToLong(buffer, offset, 8);
    }

    private static long convertNBytesToLong(byte[] buffer, int offset,
            int nBytes) throws IOException
    {
        long value = 0;
        int shift = 0;
        ByteArrayInputStream byte_in = new ByteArrayInputStream(buffer);
        DataInputStream data_in = new DataInputStream(byte_in);

        /* skip the offset */
        data_in.skip(offset);

        for (int i = 0; i < nBytes; i++)
        {
            value += (long) data_in.readUnsignedByte() << shift;
            shift += 8;
        }
        return value;
    }

    public static long convert8BytesToLong_2(byte[] buffer, int offset)
    {
        long ret = (long) unsignedByteToInt(buffer[offset]);
        ret = ret | ((long) unsignedByteToInt(buffer[offset + 1])) << 8;
        ret = ret | ((long) unsignedByteToInt(buffer[offset + 2])) << 16;
        ret = ret | ((long) unsignedByteToInt(buffer[offset + 3])) << 24;
        ret = ret | ((long) unsignedByteToInt(buffer[offset + 4])) << 32;
        ret = ret | ((long) unsignedByteToInt(buffer[offset + 5])) << 40;
        ret = ret | ((long) unsignedByteToInt(buffer[offset + 6])) << 48;
        ret = ret | ((long) unsignedByteToInt(buffer[offset + 7])) << 56;
        return ret;
    }

    /* Reads little-endian unsigned integer from no more than 4 bytes */
    public static long convertNBytesToLong_2(byte[] buf, int off, int len)
    {
        long ret = 0;
        int shift = 0;
        for (int i = off; i < (off + len); i++, shift += 8)
        {
            ret = ret | (long) unsignedByteToInt(buf[i]) << shift;
        }
        return ret;
    }
}
