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

package com.continuent.tungsten.common.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.junit.Assert;
import org.junit.Test;

/**
 * Implements a basic unit test of CSV input and output.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class CsvTest
{
    /**
     * Verify that we can write a file to output and read it back in.
     */
    @Test
    public void testOutputInput() throws Exception
    {
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);

        // Write values.
        csvWriter.addColumnName("a");
        csvWriter.addColumnName("bb");
        csvWriter.addColumnName("ccc");
        csvWriter.put("a", "r1a");
        csvWriter.put("bb", "r1b");
        csvWriter.put("ccc", "r1c");
        csvWriter.write();
        csvWriter.put("a", "r2a");
        csvWriter.put("bb", "r2b");
        csvWriter.put("ccc", "r2c");
        csvWriter.flush();
        String csv = sw.toString();

        // Read values back in again and validate.
        StringReader sr = new StringReader(csv);
        CsvReader csvReader = new CsvReader(sr);

        // Validate names.
        Assert.assertTrue("First read succeeded", csvReader.next());
        Assert.assertEquals("size of names", 3, csvReader.getNames().size());
        Assert.assertEquals("a", csvReader.getNames().get(0));
        Assert.assertEquals("bb", csvReader.getNames().get(1));
        Assert.assertEquals("ccc", csvReader.getNames().get(2));

        // Check row 1 values using index and names.
        Assert.assertEquals("r1 val1", "r1a", csvReader.getString(1));
        Assert.assertEquals("r1 val1", "r1a", csvReader.getString("a"));

        Assert.assertEquals("r1 val2", "r1b", csvReader.getString(2));
        Assert.assertEquals("r1 val2", "r1b", csvReader.getString("bb"));

        Assert.assertEquals("r1 val1", "r1c", csvReader.getString(3));
        Assert.assertEquals("r1 val1", "r1c", csvReader.getString("ccc"));

        // Check row 2 values.
        Assert.assertTrue("Second read succeeded", csvReader.next());
        Assert.assertEquals("r1 val1", "r2a", csvReader.getString(1));
        Assert.assertEquals("r1 val1", "r2a", csvReader.getString("a"));

        Assert.assertEquals("r1 val2", "r2b", csvReader.getString(2));
        Assert.assertEquals("r1 val2", "r2b", csvReader.getString("bb"));

        Assert.assertEquals("r1 val1", "r2c", csvReader.getString(3));
        Assert.assertEquals("r1 val1", "r2c", csvReader.getString("ccc"));

        // Ensure we are done.
        Assert.assertFalse("Third read failed", csvReader.next());
    }

    /**
     * Verify that we can write a file to output and read it back in using
     * CsvReader and CsvWriter instances generated from a CsvSpecification.
     */
    @Test
    public void testOutputInputFromSpecification() throws Exception
    {
        // Generate a default specification but set the field separator just for
        // fun.
        CsvSpecification csvSpec = new CsvSpecification();
        csvSpec.setFieldSeparator(":");
        csvSpec.setUseHeaders(true);

        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = csvSpec.createCsvWriter(sw);

        // Write values.
        csvWriter.addColumnName("a");
        csvWriter.addColumnName("bb");
        csvWriter.addColumnName("ccc");
        csvWriter.put("a", "r1a");
        csvWriter.put("bb", "r1b");
        csvWriter.put("ccc", "r1c");
        csvWriter.flush();
        String csv = sw.toString();

        // Read values back in again and validate.
        StringReader sr = new StringReader(csv);
        CsvReader csvReader = csvSpec.createCsvReader(sr);

        // Validate names.
        Assert.assertTrue("First read succeeded", csvReader.next());
        Assert.assertEquals("size of names", 3, csvReader.getNames().size());
        Assert.assertEquals("a", csvReader.getNames().get(0));
        Assert.assertEquals("bb", csvReader.getNames().get(1));
        Assert.assertEquals("ccc", csvReader.getNames().get(2));

        // Check row 1 values using index and names.
        Assert.assertEquals("r1 val1", "r1a", csvReader.getString(1));
        Assert.assertEquals("r1 val1", "r1a", csvReader.getString("a"));

        Assert.assertEquals("r1 val2", "r1b", csvReader.getString(2));
        Assert.assertEquals("r1 val2", "r1b", csvReader.getString("bb"));

        Assert.assertEquals("r1 val1", "r1c", csvReader.getString(3));
        Assert.assertEquals("r1 val1", "r1c", csvReader.getString("ccc"));

        // Ensure we are done.
        Assert.assertFalse("Read failed", csvReader.next());
    }

    /**
     * Verify that a CsvException results if the client tries to add a new
     * column name after writing the first row.
     */
    @Test
    public void testSetHeaderAfterWrite() throws Exception
    {
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);

        csvWriter.addColumnName("a");
        csvWriter.put("a", "r1a");
        csvWriter.write();

        try
        {
            csvWriter.addColumnName("bb");
            throw new Exception("Can add column after writing!");
        }
        catch (CsvException e)
        {
            // Expected.
        }
    }

    /**
     * Verify that a CsvException results if the client issues a write while we
     * have an incomplete row.
     */
    @Test
    public void testWriteIncompleteRow() throws Exception
    {
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);

        csvWriter.addColumnName("a");
        csvWriter.addColumnName("bb");
        csvWriter.put("a", "r1a");
        try
        {
            csvWriter.write();
            throw new Exception("Can write partial row!");
        }
        catch (CsvException e)
        {
            // Expected.
        }
        try
        {
            csvWriter.write();
            throw new Exception("Can flush partial row!");
        }
        catch (CsvException e)
        {
            // Expected.
        }

        // Verify that we can write as well as flush once we add the extra row.
        csvWriter.put("bb", "r1b");
        csvWriter.write();
        csvWriter.flush();
    }

    /**
     * Verify that a CsvException results if the client issues a write past the
     * end of the row.
     */
    @Test
    public void testWriteExtraColumn() throws Exception
    {
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);

        csvWriter.addColumnName("a");
        csvWriter.put(1, "good value");
        try
        {
            csvWriter.put(2, "bad value");
            throw new Exception("Can write extra column!");
        }
        catch (CsvException e)
        {
            // Expected.
        }
    }

    /**
     * Verify that if a client attempts to read a non-existent column an
     * IOException results.
     */
    @Test
    public void testReadNonExistent() throws Exception
    {
        // Load a CSV file.
        String[] colNames = {"a", "b", "c"};
        String csv = this.createCsvFile(colNames, 10);
        StringReader sr = new StringReader(csv);
        CsvReader csvReader = new CsvReader(sr);

        // Validate reading existing fields.
        Assert.assertTrue("First read succeeded", csvReader.next());
        Assert.assertEquals("size of names", 3, csvReader.getNames().size());
        Assert.assertEquals("r1_c1", csvReader.getString("a"));
        Assert.assertEquals("r1_c2", csvReader.getString("b"));
        Assert.assertEquals("r1_c3", csvReader.getString("c"));

        // Read non-existing column name.
        try
        {
            csvReader.getString("d");
            throw new Exception("Able to read invalid column name");
        }
        catch (CsvException e)
        {
            // Expected.
        }

        // Read non-existing column index.
        try
        {
            csvReader.getString(4);
            throw new Exception("Able to read invalid column index");
        }
        catch (CsvException e)
        {
            // Expected.
        }
    }

    /**
     * Verify that enabling a row ID allows us to output and read back
     * automatically generated row values.
     */
    @Test
    public void testRowIds() throws Exception
    {
        // Create a file with row IDs enabled.
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);
        csvWriter.addColumnName("a");
        csvWriter.addRowIdName("my_row_id");
        csvWriter.addColumnName("c");
        csvWriter.setWriteHeaders(true);

        // Write values and flush to a string we can read again.
        for (int i = 1; i <= 10; i++)
        {
            csvWriter.put("a", new Integer(i).toString());
            csvWriter.put("c", new Integer(i * 2).toString());
            csvWriter.write();
        }
        csvWriter.flush();
        String csv = sw.toString();

        // Read CSV data back in and check that row ID is correct.
        StringReader sr = new StringReader(csv);
        CsvReader csvReader = new CsvReader(sr);
        csvReader.setUseHeaders(true);

        for (int i = 1; i <= 10; i++)
        {
            // Row ID is i + 1 because we have a header row.
            csvReader.next();
            Assert.assertEquals(new Integer(i).toString(),
                    csvReader.getString("a"));
            Assert.assertEquals(new Integer(i + 1).toString(),
                    csvReader.getString("my_row_id"));
            Assert.assertEquals(new Integer(i * 2).toString(),
                    csvReader.getString("c"));
        }
    }

    /**
     * Verify that quoting, character escaping and suppression within strings
     * works properly.
     */
    @Test
    public void testEscapingAndSuppression() throws Exception
    {
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);
        csvWriter.setEscapeChar('X');
        csvWriter.setEscapedChars("ABC");
        csvWriter.setSuppressedChars("abc");
        csvWriter.setQuoteChar('Q');
        csvWriter.setQuoted(true);

        // Write values.
        csvWriter.addColumnName("data");
        csvWriter.put("data", "abc").write();
        csvWriter.put("data", "ABC").write();
        csvWriter.put("data", "aAbBcC").write();
        csvWriter.put("data", "Q").write();
        csvWriter.flush();
        String csv = sw.toString();

        // Read values back in again.
        StringReader sr = new StringReader(csv);
        CsvReader csvReader = new CsvReader(sr);

        // Characters in first row should be empty string surrounded by quotes.
        Assert.assertTrue("First read", csvReader.next());
        Assert.assertEquals("suppressed characters", "QQ",
                csvReader.getString(1));

        // Characters in second row should be escaped with quotes.
        Assert.assertTrue("Second read", csvReader.next());
        Assert.assertEquals("escaped characters", "QXAXBXCQ",
                csvReader.getString(1));

        // Characters in third row should be escaped with quotes with suppressed
        // characters omitted.
        Assert.assertTrue("Second read", csvReader.next());
        Assert.assertEquals("escaped characters", "QXAXBXCQ",
                csvReader.getString(1));

        // Characters in fourth row be an escaped quote character.
        Assert.assertTrue("Second read", csvReader.next());
        Assert.assertEquals("escaped characters", "QXQQ",
                csvReader.getString(1));
    }

    /**
     * Verify that partially written rows are accepted if nullAutofill is true
     * or rejected if not.
     */
    @Test
    public void testNullAutofill() throws Exception
    {
        // Write with autofill disabled. This generates an exception.
        StringWriter sw = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(sw);
        csvWriter.setNullPolicy(NullPolicy.emptyString);

        // Write values.
        csvWriter.addColumnName("d1");
        csvWriter.addColumnName("d2");
        try
        {
            csvWriter.put("d1", "something").write();
            throw new Exception(
                    "Able to write partial row when nullAutofill=false");
        }
        catch (CsvException e)
        {
            // Expected.
        }

        // Write with autofill enabled. This should add "NULL" for null fields.
        sw = new StringWriter();
        csvWriter = new CsvWriter(sw);
        csvWriter.setNullPolicy(NullPolicy.nullValue);
        csvWriter.setNullValue("NULL");
        csvWriter.setNullAutofill(true);

        // Add two rows.
        csvWriter.addColumnName("d1");
        csvWriter.addColumnName("d2");

        csvWriter.put("d1", "something").write();
        csvWriter.put("d1", null).write();
        csvWriter.put("d2", "else").write();

        csvWriter.flush();
        String csv = sw.toString();

        // Read values back in again.
        StringReader sr = new StringReader(csv);
        CsvReader csvReader = new CsvReader(sr);

        // First row should have d1 + null value.
        Assert.assertTrue("First read", csvReader.next());
        Assert.assertEquals("d1 written", "something", csvReader.getString(1));
        Assert.assertEquals("d2 NULL", "NULL", csvReader.getString(2));

        // Second row should have only null values.
        Assert.assertTrue("Second read", csvReader.next());
        Assert.assertEquals("d1 NULL", "NULL", csvReader.getString(1));
        Assert.assertEquals("d2 NULL", "NULL", csvReader.getString(2));

        // Third row should have null + d2 value.
        Assert.assertTrue("Second read", csvReader.next());
        Assert.assertEquals("d1 NULL", "NULL", csvReader.getString(1));
        Assert.assertEquals("d2 written", "else", csvReader.getString(2));
    }

    // Create a CSV file with no extra separators.
    private String createCsvFile(String[] colNames, int rows)
            throws IOException
    {
        // Set up output.
        StringWriter sw = new StringWriter();
        BufferedWriter bw = new BufferedWriter(sw);

        // Write headers.
        for (int c = 1; c <= colNames.length; c++)
        {
            if (c > 1)
                bw.append(",");
            bw.append(colNames[c - 1]);
        }

        // Write each row.
        for (int r = 1; r <= rows; r++)
        {
            bw.newLine();
            for (int c = 1; c <= colNames.length; c++)
            {
                if (c > 1)
                    bw.append(",");
                String value = "r" + r + "_c" + c;
                bw.append(value);
            }
        }

        // Flush output and return value.
        bw.flush();
        return sw.toString();
    }
}