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
 * Initial developer(s): Csaba Simon
 * Contributor(s): Gilles Rayrat
 */

package com.continuent.tungsten.common.mysql;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.util.GregorianCalendar;

import com.continuent.tungsten.common.mysql.MySQLPacket;
import com.continuent.tungsten.common.mysql.Utils;

import junit.framework.TestCase;

/**
 * Tests MySQLPacket by inserting various data into packets and verifying output
 * values.
 * 
 * @author <a href="mailto:csaba.simon@continuent.com">Csaba Simon</a>
 * @author <a href="gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class MySQLPacketTestCase extends TestCase
{
    /**
     * Test put and get methods.
     */
    public void testGetAndPut()
    {
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        byte oneByte = 127;
        byte[] moreBytes = {0, -7, 7, -12, 12};
        int oneInt = 65278;
        int oneLongInt = 16777215;
        int oneLong = 2147483647;
        long oneLongLong = 2104061312563890L;
        String oneString = "abcdef";

        array.putByte(oneByte);
        array.putBytes(moreBytes);
        array.putInt16(oneInt);
        array.putLenBytes(moreBytes);
        array.putInt32(oneLong);
        array.putInt24(oneLongInt);
        array.putLong(oneLongLong);
        array.putString(oneString);

        array.reset();

        assertEquals(oneByte, array.getByte());
        compareBytes(moreBytes, array.getBytes(moreBytes.length));
        assertEquals(oneInt, array.getUnsignedShort());
        compareBytes(moreBytes, array.getLenEncodedBytes());
        assertEquals(oneLong, array.getInt32());
        assertEquals(oneLongInt, array.getUnsignedInt24());
        assertEquals(oneLongLong, array.getLong());
        assertEquals(oneString, array.getString());
    }

    /**
     * Test the getByte() method.
     */
    public void testGetByte()
    {
        byte[] bytes = {3, 0, 0, 1, 0, (byte) -1, (byte) -127};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        assertEquals(0, array.getByte());
        assertEquals(-1, array.getByte());
        assertEquals(-127, array.getByte());
    }

    /**
     * Test the getUnsignedByte() method.
     */
    public void testGetUnsignedByte()
    {
        byte[] bytes = {3, 0, 0, 1, (byte) -1, (byte) -127, (byte) -128};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        assertEquals(255, array.getUnsignedByte());
        assertEquals(129, array.getUnsignedByte());
        assertEquals(128, array.getUnsignedByte());
    }

    /**
     * Test the getBytes() method.
     */
    public void testGetBytes()
    {
        byte[] expectedResult = {1, 2, 3};
        byte[] bytes = {3, 0, 0, 1, 1, 2, 3};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        compareBytes(expectedResult, array.getBytes(3));
    }

    /**
     * Test the getUnsignedInt16() method.
     */
    public void testGetUnsignedInt16()
    {
        byte[] bytes = {8, 0, 0, 1, -1, -1, -127, -1, 0, -128, -1, 127};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        assertEquals(65535, array.getUnsignedShort());
        assertEquals(65409, array.getUnsignedShort());
        assertEquals(32768, array.getUnsignedShort());
        assertEquals(32767, array.getUnsignedShort());
    }

    /**
     * Test the getShort() method.
     */
    public void testGetShort()
    {
        byte[] bytes = {6, 0, 0, 1, -1, -1, -127, -1, 0, -128, -1, 127};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        assertEquals(-1, array.getShort());
        assertEquals(-127, array.getShort());
        assertEquals(-32768, array.getShort());
        assertEquals(32767, array.getShort());
    }

    /**
     * Test getLenEncodedBytes() method.
     */
    public void testGetLenEncodedBytes()
    {
        byte[] expectedResult1 = null;
        byte[] expectedResult2 = {1, 2, 3};
        byte[] expectedResult3 = {};
        byte[] bytes1 = {1, 0, 0, 1, -5};
        byte[] bytes2 = {4, 0, 0, 1, 3, 1, 2, 3};
        byte[] bytes3 = {1, 0, 0, 1, 0};
        MySQLPacket array1 = new MySQLPacket(bytes1.length
                - MySQLPacket.HEADER_LENGTH, bytes1, (byte) 1);
        MySQLPacket array2 = new MySQLPacket(bytes2.length
                - MySQLPacket.HEADER_LENGTH, bytes2, (byte) 1);
        MySQLPacket array3 = new MySQLPacket(bytes3.length
                - MySQLPacket.HEADER_LENGTH, bytes3, (byte) 1);

        compareBytes(expectedResult1, array1.getLenEncodedBytes());
        compareBytes(expectedResult2, array2.getLenEncodedBytes());
        compareBytes(expectedResult3, array3.getLenEncodedBytes());
    }

    /**
     * Test the getUnsignedInt32() method.
     */
    public void testGetUnsignedInt32()
    {
        byte[] bytes = {16, 0, 0, 1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, -128,
                -1, -1, -1, 127};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        assertEquals(0xffffffffL, array.getUnsignedInt32());
        assertEquals(0x0, array.getUnsignedInt32());
        assertEquals(0x80000000L, array.getUnsignedInt32());
        assertEquals(0x7FFFFFFFL, array.getUnsignedInt32());
    }

    /**
     * Test the getInt32() method.
     */
    public void testGetInt32()
    {
        byte[] bytes = {16, 0, 0, 1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, -128,
                -1, -1, -1, 127};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        assertEquals(-1, array.getInt32());
        assertEquals(0, array.getInt32());
        assertEquals(-0x80000000, array.getInt32());
        assertEquals(0x7fffffff, array.getInt32());
    }

    /**
     * Test the getInt24() method.
     */
    public void testGetUnsignedInt24()
    {
        byte[] bytes = {12, 0, 0, 1, -1, -1, -1, 0, 0, 0, 0, 0, -128, -1, -1,
                127};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        assertEquals(0xffffff, array.getUnsignedInt24());
        assertEquals(0, array.getUnsignedInt24());
        assertEquals(0x800000, array.getUnsignedInt24());
        assertEquals(0x7fffff, array.getUnsignedInt24());
    }

    /**
     * Test the getInt24() method.
     */
    public void testGetInt24()
    {
        byte[] bytes = {12, 0, 0, 1, -1, -1, -1, 0, 0, 0, 0, 0, -128, -1, -1,
                127};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        assertEquals(-1, array.getInt24());
        assertEquals(0, array.getInt24());
        assertEquals(-0x800000, array.getInt24());
        assertEquals(0x7fffff, array.getInt24());
    }

    /**
     * Test the getLong() method.
     */
    public void testGetLong()
    {
        byte[] bytes = {32, 0, 0, 1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -128, -1, -1, -1, -1, -1,
                -1, -1, 127};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        assertEquals(-1, array.getLong());
        assertEquals(0, array.getLong());
        assertEquals(-0x8000000000000000L, array.getLong());
        assertEquals(0x7FFFFFFFFFFFFFFFL, array.getLong());
    }

    /**
     * Test the getUnsignedLong() method.
     */
    public void testGetUnsignedLong()
    {
        byte[] bytes = {32, 0, 0, 1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -128, -1, -1, -1, -1, -1,
                -1, -1, 127};
        MySQLPacket array = new MySQLPacket(bytes.length
                - MySQLPacket.HEADER_LENGTH, bytes, (byte) 1);

        assertEquals("ffffffffffffffff", array.getUnsignedLong().toString(16));
        assertEquals("0", array.getUnsignedLong().toString(16));
        assertEquals("8000000000000000", array.getUnsignedLong().toString(16));
        assertEquals("7fffffffffffffff", array.getUnsignedLong().toString(16));
    }

    /**
     * Test the getString() method.
     */
    public void testGetString()
    {
        byte[] bytes1 = {3, 0, 0, 1, 'a', 'b', 'c'};
        byte[] bytes2 = {4, 0, 0, 1, 'a', 'b', 'c', 0};
        MySQLPacket array1 = new MySQLPacket(bytes1.length
                - MySQLPacket.HEADER_LENGTH, bytes1, (byte) 1);
        MySQLPacket array2 = new MySQLPacket(bytes2.length
                - MySQLPacket.HEADER_LENGTH, bytes2, (byte) 1);

        assertEquals("abc", array1.getString());
        assertEquals("abc", array2.getString());
    }

    /**
     * Test putByte method.
     */
    public void testPutByte()
    {
        byte[] expectedResult = {3, 0, 0, 1, 1, 2, 3};
        MySQLPacket array = new MySQLPacket(0, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putByte((byte) 1);
            array.putByte((byte) 2);
            array.putByte((byte) 3);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test putByte method.
     */
    public void testPutBytes()
    {
        byte[] expectedResult = {3, 0, 0, 1, 1, 2, 3};
        byte[] bytes = {1, 2, 3};
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putBytes(bytes);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test the putFieldLength() method.
     */
    public void testPutFieldLength()
    {
        byte[] expectedResult1 = {1, 0, 0, 1, 1};
        byte[] expectedResult2 = {3, 0, 0, 1, -4, 57, 48};
        byte[] expectedResult3 = {4, 0, 0, 1, -3, 78, 97, -68};
        byte[] expectedResult4 = {9, 0, 0, 1, -2, -46, 2, -106, 73, 0, 0, 0, 0};
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putFieldLength(1L);
            array.write(output);
            compareBytes(expectedResult1, output.toByteArray());

            array.reset();
            output.reset();
            array.putFieldLength(12345L);
            array.write(output);
            compareBytes(expectedResult2, output.toByteArray());

            array.reset();
            output.reset();
            array.putFieldLength(12345678L);
            array.write(output);
            compareBytes(expectedResult3, output.toByteArray());

            array.reset();
            output.reset();
            array.putFieldLength(1234567890L);
            array.write(output);
            compareBytes(expectedResult4, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test putInt16 method.
     */
    public void testPutInt16()
    {
        byte[] expectedResult = {6, 0, 0, 1, 1, 0, 2, 0, 3, 0};
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putInt16(1);
            array.putInt16(2);
            array.putInt16(3);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test putLenBytes method.
     */
    public void testPutLenBytes()
    {
        byte[] expectedResult = {4, 0, 0, 1, 3, 1, 2, 3};
        byte[] bytes = {1, 2, 3};
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putLenBytes(bytes);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test the putLenString() method.
     */
    public void testPutLenString()
    {
        byte[] expectedResult = {4, 0, 0, 1, 3, 'a', 'b', 'c'};
        String string = "abc";
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putLenString(string);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test putInt32 method.
     */
    public void testPutInt32()
    {
        byte[] expectedResult = {12, 0, 0, 1, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0,
                0};
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putInt32(1);
            array.putInt32(2);
            array.putInt32(3);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test putUnsignedInt32 method.
     */
    public void testPutUnsignedInt32()
    {
        byte[] expectedResult = {16, 0, 0, 1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0,
                0, -128, -1, -1, -1, 127};
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putUnsignedInt32(0xffffffffL);
            array.putUnsignedInt32(0x0);
            array.putUnsignedInt32(0x80000000L);
            array.putUnsignedInt32(0x7FFFFFFFL);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test putInt24 method.
     */
    public void testPutInt24()
    {
        byte[] expectedResult = {9, 0, 0, 1, 1, 0, 0, 2, 0, 0, 3, 0, 0};
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putInt24(1);
            array.putInt24(2);
            array.putInt24(3);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test the putLong method.
     */
    public void testPutLong()
    {
        byte[] expectedResult = {24, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0,
                0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0};
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putLong(1);
            array.putLong(2);
            array.putLong(3);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test the putString() method.
     */
    public void testPutString()
    {
        byte[] expectedResult = {4, 0, 0, 1, 'a', 'b', 'c', 0};
        String string = "abc";
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putString(string);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Test the putStringNoNull() method.
     */
    public void testPutStringNoNull()
    {
        byte[] expectedResult = {3, 0, 0, 1, 'a', 'b', 'c'};
        String string = "abc";
        MySQLPacket array = new MySQLPacket(16, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try
        {
            array.putStringNoNull(string);
            array.write(output);
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Tests the testPutDate() method with a simple date (2008-03-10)
     */
    public void testPutDate()
    {
        byte[] expectedResult = {16, 0, 0, 1, (byte) 0xD8, (byte) 0x07, 3, 10,
                (byte) 0xE8, (byte) 0x03, 1, 1, (byte) 0x0F, (byte) 0x27, 12,
                31, (byte) 0xAC, (byte) 0x01, 5, 21}; // 4 bytes for each date
        GregorianCalendar cNowOurDays = new GregorianCalendar(2008,
                2/* march, but zero based */, 10);
        GregorianCalendar cLowRange = new GregorianCalendar(1000, 0/* jan */,
                1);
        GregorianCalendar cHighRange = new GregorianCalendar(9999,
                11/* dec */, 31);
        // Note: MySQL supported range is above that (>1000-01-01) but it's
        // harmless to test
        GregorianCalendar cBeforeChrist = new GregorianCalendar(-427,
                4/* may */, 21);

        MySQLPacket array = new MySQLPacket(32, (byte) 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try
        {
            array.putDate(new Date(cNowOurDays.getTimeInMillis()));
            array.putDate(new Date(cLowRange.getTimeInMillis()));
            array.putDate(new Date(cHighRange.getTimeInMillis()));
            array.putDate(new Date(cBeforeChrist.getTimeInMillis()));
            array.write(output);
            System.out.println("exp:"
                    + Utils.byteArrayToHexString(expectedResult));
            System.out.println("act:"
                    + Utils.byteArrayToHexString(output.toByteArray()));
            compareBytes(expectedResult, output.toByteArray());
        }
        catch (IOException e)
        {
            fail("This should not happen!");
        }
    }

    /**
     * Compare two byte arrays.
     * 
     * @param expectedResult expected result
     * @param actualResult actual result
     */
    private void compareBytes(byte[] expectedResult, byte[] actualResult)
    {
        if (expectedResult == null)
        {
            assertNull(actualResult);
            return;
        }

        assertNotNull(actualResult);

        assertEquals("Size of the arrays are not equals: ",
                expectedResult.length, actualResult.length);
        for (int i = 0; i < expectedResult.length; i++)
        {
            assertEquals("Bytes at index " + i + " differs: expected 0x"
                    + Utils.byteToHexString(expectedResult[i]) + " got 0x"
                    + Utils.byteToHexString(actualResult[i]),
                    expectedResult[i], actualResult[i]);
        }
    }
}
