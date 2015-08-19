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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.common.parsing.bytes.test;

import java.io.UnsupportedEncodingException;

import junit.framework.TestCase;

import com.continuent.tungsten.common.parsing.bytes.ByteState;
import com.continuent.tungsten.common.parsing.bytes.ByteTranslationStateMachine;
import com.continuent.tungsten.common.parsing.bytes.MySQLStatementTranslator;

/**
 * Implements a unit test for byte parsing algorithms for MySQL. This test
 * includes a performance test that compares byte parsing with simple string
 * translation ({@link #testPerformance()}); iterations for that case are
 * controlled by setting property "test.iterations".
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ByteParsingTest extends TestCase
{
    /**
     * Verifies that an empty byte state machine always accepts anything thrown
     * at it.
     */
    public void testEmpty() throws Exception
    {
        ByteTranslationStateMachine bsm = new ByteTranslationStateMachine();
        bsm.init();
        for (int i = 0; i < 256; i++)
        {
            ByteState state = bsm.add((byte) i);
            assertEquals("null state machine always accepts",
                    ByteState.ACCEPTED, state);
        }
    }

    /**
     * Verifies that tokens are accepted with and without substitutions.
     */
    public void testTokens() throws Exception
    {
        ByteTranslationStateMachine bsm = new ByteTranslationStateMachine();
        bsm.init();
        final int NULL = 1;
        final int EVEN = 2;
        final int EVEN_SUB = 3;

        // Load values. Nulls and even types are tokens.
        for (int i = 0; i < 256; i++)
        {
            byte[] string = new byte[1];
            string[0] = (byte) i;
            if (i == 0)
                bsm.load(string, NULL, null, false);
            else if (i % 4 == 0)
                bsm.load(string, EVEN_SUB, "sub".getBytes(), false);
            else if (i % 2 == 0)
                bsm.load(string, EVEN, null, false);
        }

        // Test values
        for (int i = 0; i < 256; i++)
        {
            ByteState state = bsm.add((byte) i);
            assertEquals("should be accepted", ByteState.ACCEPTED, state);
            if (i == 0)
            {
                assertTrue("null should be token", bsm.isToken());
                assertEquals("token should be NULL", NULL, bsm.getToken());
            }
            else if (i % 2 == 0)
            {
                assertTrue("even byte should be token", bsm.isToken());
                if (i % 4 == 0)
                {
                    assertEquals("token should be EVEN_SUB", EVEN_SUB, bsm
                            .getToken());
                    assertTrue("EVEN_SUB should have sub", bsm.isSubstitute());
                    assertEquals("looking for substitution", "sub", new String(
                            bsm.getSubstitute()));
                }
                else
                {
                    assertEquals("token should be EVEN", EVEN, bsm.getToken());
                    assertFalse("EVEN should not have sub", bsm.isSubstitute());
                    assertEquals("looking for substitution", null, bsm
                            .getSubstitute());
                }
            }
            else
            {
                assertEquals("odd should just be accepted", ByteState.ACCEPTED,
                        state);
            }
        }
    }

    /**
     * Verifies that extended tokens of 2 or more bytes are accepted and
     * correctly disambiguated.
     */
    public void testExtendedToken() throws Exception
    {
        ByteTranslationStateMachine bsm = new ByteTranslationStateMachine();
        bsm.init();

        byte[] s1 = "va1".getBytes();
        byte[] s2 = "va2".getBytes();
        byte[] s3 = "\\\\".getBytes();

        // Load values.
        bsm.load(s1, 99, null, false);
        bsm.load(s2, 100, null, false);
        bsm.load(s3, -1, "\\".getBytes(), false);

        // Check values.
        checkForValue(bsm, s1, 99, null);
        checkForValue(bsm, s2, 100, null);
        checkForValue(bsm, s3, -1, "\\".getBytes());
    }

    private void checkForValue(ByteTranslationStateMachine bsm, byte[] s,
            int token, byte[] sub)
    {
        ByteState lastState = null;
        for (byte b : s)
        {
            lastState = bsm.add(b);
        }
        assertEquals("Check for matching end state", ByteState.ACCEPTED,
                lastState);
        assertEquals("Check for matching token", token, bsm.getToken());
        if (sub == null)
            assertEquals("Check for null substitute", null, bsm.getSubstitute());
        else
            assertEquals("Check for matching substitute", new String(sub),
                    new String(bsm.getSubstitute()));
    }

    /**
     * Verify that an ordinary string without introducers is returned unchanged.
     */
    public void testSimpleStringParsing() throws Exception
    {
        String testString = "INSERT INTO foo VALUES(1)";
        MySQLStatementTranslator translator = new MySQLStatementTranslator(
                "UTF-8");
        byte[] testBytes = testString.getBytes("UTF-8");
        String testString2 = translator.toJavaString(testBytes, 0,
                testBytes.length);
        assertEquals("Ordinary strings are unaltered", testString, testString2);
    }

    public void testTUC94() throws Exception
    {
        String testString = "INSERT INTO `Test` VALUES (715,3968,'\\0\\0\\0\\0\\0*'),(716,0,'=o\\rM=�'),(716,64,'w;�w2�H');";
        MySQLStatementTranslator translator = new MySQLStatementTranslator(
                "UTF-8");
        byte[] testBytes = testString.getBytes();
        // Replacing one byte to demonstrate side effect of TUC-94
        testBytes[71]=(byte)-11;
        testString = new String(testBytes, "UTF-8");
        String testString2 = translator.toJavaString(testBytes, 0,
                testBytes.length);
        assertEquals("Ordinary strings are unaltered", testString, testString2);
    }

    /**
     * Verify that introducers inside normal strings (including strings with
     * escaped quotes) and comments are ignored.
     */
    public void testEmbeddedStringParsing() throws Exception
    {
        String testStrings[] = {"INSERT INTO foo VALUES(1) /* _binary'\0' */",
                "INSERT INTO `foo_binary'f'` VALUES(1)",
                "INSERT INTO foo VALUES('_latin1\"f\"')",
                "INSERT INTO foo VALUES('\\'_latin1\"\0\"')",
                "INSERT INTO foo VALUES(\"\\\"_binary'\0'\")"};
        MySQLStatementTranslator translator = new MySQLStatementTranslator(
                "UTF-8");
        for (String testString : testStrings)
        {
            byte[] testBytes = testString.getBytes("UTF-8");
            String testString2 = translator.toJavaString(testBytes, 0,
                    testBytes.length);
            System.out.println("Output: " + testString2);
            
            assertEquals("Embedded string introducer is ignored", testString,
                    testString2);
        }
    }

    /** 
     * Verify that normal SQL statements with back-tics, quote, and double quotes do not 
     * confuse parsing. 
     */
    public void testOrdinaryStrings() throws Exception
    {
        String testStrings[] = {
                "UPDATE `table1` SET `val` = _binary'\0'", 
                "UPDATE `table1` SET `val` = _binary'`\\'\0'", 
                "UPDATE `db`.`mytable` SET `col1` = _binary'`̢<80>' WHERE (`mytable`.`id` = 1) ", 
                "INSERT into `db`.mytable VALUES(\"a'\", 'b\"', _binary'\\0')" 
                };
        MySQLStatementTranslator translator = new MySQLStatementTranslator(
                "UTF-8");
        for (String testString : testStrings)
        {
            System.out.println("Input: " + testString);
            byte[] testBytes = testString.getBytes("UTF-8");
            String testString2 = translator.toJavaString(testBytes, 0,
                    testBytes.length);
            System.out.println("Output: " + testString2);
            
            assertFalse("Strings should be different due to translation", 
                    testString.equals(testString2));
        }
    }



    /**
     * Verify that byte strings embedded with a variety of introducers are
     * correctly parsed even when supported escape characters are present. This
     * test checks introducer with both single and double quotes.
     */
    public void testBasicIntroducers() throws Exception
    {
        String start = "INSERT INTO foo VALUES(_";
        String endSingle = "')";
        String endDouble = "\")";

        String[] introducers = {"binary", "latin1", "sjis", "cp850", "utf8"};

        byte[] nulls = new byte[]{0x5c, 0x30, 0x5c, 0x30};
        byte[] backslash = new byte[]{0x5c, 0x5c};
        byte[] tic = new byte[]{0x5c, 0x27};
        byte[][] middles = {nulls, backslash, tic};

        MySQLStatementTranslator translator = new MySQLStatementTranslator(
                "UTF-8");

        for (String introducer : introducers)
        {
            String fullStart = start + introducer;
            String startSingle = fullStart + "'";
            String startDouble = fullStart + "\"";
            byte[] startSingleBytes = startSingle.getBytes("UTF-8");
            byte[] startDoubleBytes = startDouble.getBytes("UTF-8");
            byte[] endSingleBytes = endSingle.getBytes("UTF-8");
            byte[] endDoubleBytes = endDouble.getBytes("UTF-8");

            for (byte[] middle : middles)
            {
                // Test with single quotes.
                String translated = translate(translator, startSingleBytes, middle,
                        endSingleBytes);
                assertTrue("Must start with start string", translated
                        .startsWith(fullStart));
                assertTrue("Must end with single quote end string", translated
                        .endsWith(endSingle));

                // Test with single quotes.
                translated = translate(translator, startDoubleBytes, middle,
                        endDoubleBytes);
                assertTrue("Must start with start string", translated
                        .startsWith(fullStart));
                assertTrue("Must end with double quote end string", translated
                        .endsWith(endDouble));
            }
        }
    }

    /**
     * Verify that byte strings with random characters are correctly parsed
     * regardless of the character set of the enclosing statement.
     */
    public void testRandomBinary() throws Exception
    {
        String[] charsets = {"latin1", "UTF-8", "sjis"};
        for (String charset : charsets)
        {
            randomizedBinary(charset);
        }
    }

    private void randomizedBinary(String charset) throws Exception
    {
        String start = "INSERT foo SET a=_binary'";
        String end = "' WHERE id=35)";

        byte[] startBytes = start.getBytes(charset);
        byte[] endBytes = end.getBytes(charset);

        MySQLStatementTranslator translator = new MySQLStatementTranslator(
                charset);

        for (int i = 0; i < 3; i++)
        {
            byte[] data = new byte[10];
            for (int j = 0; j < data.length; j++)
                data[j] = (byte) (100 + (i * 10) + j);
            translate(translator, startBytes, data, endBytes);
        }
    }

    /**
     * Ensure that we can handle a string with multiple binary values.
     */
    public void testMultipleBinary() throws Exception
    {
        String start = "INSERT INTO foo VALUES(_binary'";
        String middle = "', _binary'";
        String end = "')";

        byte[] nulls = new byte[]{0x5c, 0x30, 0x5c, 0x30};

        byte[] data = new byte[1000];
        int length = copy(data, start.getBytes("UTF-8"), 0);
        length = copy(data, nulls, length);
        length = copy(data, middle.getBytes("UTF-8"), length);
        length = copy(data, nulls, length);
        length = copy(data, end.getBytes("UTF-8"), length);

        MySQLStatementTranslator translator = new MySQLStatementTranslator(
                "UTF-8");
        String output = translator.toJavaString(data, 0, length);
        System.out.println("Output: " + output);
    }

    /**
     * Check performance. This case uses the value in property "test.iterations"
     * to set the number of iterations. Overhead of 10% seems reasonable for
     * large numbers of iterations (> 20K).
     */
    public void testPerformance() throws Exception
    {
        String charset = "UTF-8";

        // Get number of iterations.
        String testIterations = System.getProperty("test.iterations");
        int iterations;
        if (testIterations == null)
            iterations = 1;
        else
            iterations = new Integer(testIterations);

        // Construct large insert with embedded binary data.
        String start = "INSERT INTO foo VALUES(_binary'";
        String end = "')";

        byte[] startBytes = start.getBytes(charset);
        byte[] endBytes = end.getBytes(charset);
        byte[] middleBytes = new byte[10000];
        for (int j = 0; j < middleBytes.length; j++)
            middleBytes[j] = (byte) (100 + (j % 50));

        byte[] statement = new byte[startBytes.length + middleBytes.length
                + endBytes.length];
        int length = this.copy(statement, startBytes, 0);
        length = this.copy(statement, middleBytes, length);
        length = this.copy(statement, endBytes, length);

        // Create translator.
        MySQLStatementTranslator translator = new MySQLStatementTranslator(
                charset);

        // Test parsing translation time.
        String testString = null;
        long startMillis = System.currentTimeMillis();
        for (int i = 0; i < iterations; i++)
        {
            testString = translator
                    .toJavaString(statement, 0, statement.length);
        }
        long endMillis = System.currentTimeMillis();
        System.out.println("Parsing & translation: iterations=" + iterations
                + " string length=" + testString.length() + " seconds="
                + ((endMillis - startMillis) / 1000.0));

        startMillis = System.currentTimeMillis();
        for (int i = 0; i < iterations; i++)
        {
            testString = new String(statement, charset);
        }
        endMillis = System.currentTimeMillis();
        System.out.println("Translation only: iterations=" + iterations
                + " string length=" + testString.length() + " seconds="
                + ((endMillis - startMillis) / 1000.0));
    }

    // Copy binary data into a buffer.
    private int copy(byte[] buffer, byte[] sub, int offset)
    {
        for (int i = 0; i < sub.length; i++)
            buffer[offset++] = sub[i];
        return offset;
    }

    // Assemble and translate a string.
    private String translate(MySQLStatementTranslator translator, byte[] begin,
            byte[] middle, byte[] end) throws UnsupportedEncodingException
    {
        byte[] data = new byte[begin.length + middle.length + end.length];
        copy(data, begin, 0);
        int offset = copy(data, begin, 0);
        offset = copy(data, middle, offset);
        offset = copy(data, end, offset);

        String output = translator.toJavaString(data, 0, data.length);
        System.out.println("Output: " + output);
        return output;
    }
}