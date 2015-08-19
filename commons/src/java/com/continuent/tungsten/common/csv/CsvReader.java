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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads CSV format output with appropriate conversions to Java data types.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class CsvReader
{
    // Properties.
    private String               fieldSeparator     = ",";
    private String               recordSeparator    = "\n";
    private boolean              collapseSeparators = false;
    private boolean              useHeaders         = true;

    // State.
    private Map<String, Integer> names              = new HashMap<String, Integer>();
    private List<String>         row;
    private BufferedReader       reader;
    private int                  rowCount           = 0;

    /**
     * Instantiate a new instance with input from provided reader.
     */
    public CsvReader(Reader reader)
    {
        this(new BufferedReader(reader));
    }

    /**
     * Instantiate a new instance with input from provided buffered reader. This
     * call allows clients to set buffering parameters themselves.
     */
    public CsvReader(BufferedReader reader)
    {
        this.reader = new BufferedReader(reader);
    }

    /**
     * Sets the field separator character.
     */
    public void setFieldSeparator(String inputSeparator)
    {
        this.fieldSeparator = inputSeparator;
    }

    /**
     * Returns field separator character.
     */
    public String getFieldSeparator()
    {
        return fieldSeparator;
    }

    /**
     * Returns true if successive input separators should be treated as a single
     * separator.
     */
    public boolean isCollapseSeparators()
    {
        return collapseSeparators;
    }

    /**
     * If set to true treat successive input separators as a single separator.
     */
    public void setCollapseSeparators(boolean collapseSeparators)
    {
        this.collapseSeparators = collapseSeparators;
    }

    /**
     * Sets the record separator character.
     */
    public void setRecordSeparator(String inputSeparator)
    {
        this.recordSeparator = inputSeparator;
    }

    /**
     * Returns record separator character.
     */
    public String getRecordSeparator()
    {
        return recordSeparator;
    }

    /**
     * Returns true if input should contain column headers in first row.
     */
    public boolean isUseHeaders()
    {
        return useHeaders;
    }

    /**
     * If set to true first row must contain column headers.
     */
    public void setUseHeaders(boolean useHeaders)
    {
        this.useHeaders = useHeaders;
    }

    /**
     * Returns the current count of rows read.
     */
    public int getRowCount()
    {
        return rowCount;
    }

    /**
     * Return names in column order.
     */
    public List<String> getNames()
    {
        // Create null-filled array.
        List<String> nameList = new ArrayList<String>(names.size());
        for (int i = 0; i < names.size(); i++)
            nameList.add(null);

        // Add names to correct positions in array.
        for (String name : names.keySet())
        {
            int index = names.get(name);
            nameList.set(index - 1, name);
        }
        return nameList;
    }

    /**
     * Return the number of columns.
     */
    public int getWidth()
    {
        return names.size();
    }

    /**
     * Positions to next row and returns true if there are data to be read.
     * 
     * @throws IOException Thrown if there is an IO error.
     */
    public boolean next() throws IOException
    {
        // If we are on row 1 and headers are enabled, read and store
        // column names.
        if (rowCount == 0 && useHeaders)
        {
            List<String> row1 = this.read();
            if (row1 == null)
                return false;
            for (int i = 0; i < row1.size(); i++)
            {
                String name = row1.get(i);
                names.put(name, i + 1);
            }
            rowCount++;
        }

        // Read the next row.
        row = this.read();
        if (row == null)
            return false;
        else
        {
            rowCount++;
            return true;
        }
    }

    /**
     * Gets a value from the current row.
     * 
     * @param index Column index where indexes are numbered 1,2,3,...,N with N
     *            being the width of the row in columns
     * @throws CsvException Thrown if read is invalid
     */
    public String getString(int index) throws CsvException
    {
        // Ensure we have a row.
        if (row == null)
        {
            throw new CsvException("Attempt to read when no row is available");
        }

        // Ensure the index is within bounds.
        if (index > names.size() || index < 1)
            throw new CsvException("Attempt to read non-existent index: row="
                    + rowCount + " index=" + index + " columns=" + names.size());

        return row.get(index - 1);
    }

    /**
     * Gets a string from the current row.
     */
    public String getString(String key) throws CsvException
    {
        Integer index = names.get(key);
        if (index == null)
            throw new CsvException("Attempt to read non-existent key: row="
                    + rowCount + " key=" + key);
        return getString(index);
    }

    // Reads a single row.
    private List<String> read() throws IOException
    {
        String regex = "[" + fieldSeparator + "]";
      
        String s = reader.readLine();
        if (s != null && s.length() != 0)
        {
            s = s.trim();
            return getList(s, regex);
        }
        else
            return null;
    }

    // Splits a CSV separated list.
    private List<String> getList(String s, String regex)
    {
        String[] values = s.split(regex);
        ArrayList<String> list = new ArrayList<String>();
        for (String value : values)
        {
            if (value.length() > 0 || !collapseSeparators)
                list.add(value);
        }
        return list;
    }
}
