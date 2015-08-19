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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.common.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes CSV output. This class implements CSV formatting roughly as described
 * in RFC4180 (http://tools.ietf.org/html/rfc4180) with practical alterations to
 * match specify DBMS implementations.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class CsvWriter
{
    // Properties.
    private String               fieldSeparator  = ",";
    private String               recordSeparator = "\n";
    private boolean              writeHeaders    = true;
    private boolean              quoted          = false;
    private NullPolicy           nullPolicy      = NullPolicy.skip;
    private String               nullValue       = null;
    private boolean              nullAutofill    = false;
    private char                 quoteChar       = '\"';
    private char                 escapeChar      = '\\';
    private String               escapedChars    = "";
    private String               suppressedChars = "";
    private String               rowId           = null;

    // State.
    private Map<String, Integer> names           = new HashMap<String, Integer>();
    private List<String>         row;
    private BufferedWriter       writer;
    private int                  rowCount        = 0;
    private int                  colCount        = 0;

    // Enum and table to describe disposition of specific characters.
    enum Disposition
    {
        escape, suppress
    }

    private Map<Character, Disposition> disposition;

    /**
     * Instantiate a new instance with output to provided writer.
     */
    public CsvWriter(Writer writer)
    {
        this(new BufferedWriter(writer));
    }

    /**
     * Instantiate a new instance with output to provided buffered writer. This
     * call allows clients to set buffering parameters themselves.
     */
    public CsvWriter(BufferedWriter writer)
    {
        this.writer = writer;
    }

    /**
     * Sets the field separator characters.
     */
    public void setFieldSeparator(String fieldSeparators)
    {
        this.fieldSeparator = fieldSeparators;
    }

    /**
     * Returns field separator character.
     */
    public String getFieldSeparator()
    {
        return this.fieldSeparator;
    }

    /**
     * Sets the record separator characters.
     */
    public void setRecordSeparator(String recordSeparator)
    {
        this.recordSeparator = recordSeparator;
    }

    /**
     * Returns record separator character.
     */
    public String getRecordSeparator()
    {
        return this.recordSeparator;
    }

    /** Returns true if values will be enclosed by a quote character. */
    public synchronized boolean isQuoted()
    {
        return quoted;
    }

    /** Set to true to enable quoting. */
    public synchronized void setQuoted(boolean quoted)
    {
        this.quoted = quoted;
    }

    /** Returns the policy for handling null values. */
    public synchronized NullPolicy getNullPolicy()
    {
        return nullPolicy;
    }

    /** Sets the policy for handling null values. */
    public synchronized void setNullPolicy(NullPolicy nullPolicy)
    {
        this.nullPolicy = nullPolicy;
    }

    /** Gets the null value identifier string. */
    public synchronized String getNullValue()
    {
        return nullValue;
    }

    /**
     * Sets the null value identifier string. This applies only when null policy
     * is NullPolicy.nullValue.
     */
    public synchronized void setNullValue(String nullValue)
    {
        this.nullValue = nullValue;
    }

    /** Returns true to fill nulls automatically. */
    public synchronized boolean isNullAutofill()
    {
        return nullAutofill;
    }

    /**
     * Sets the null autofill policy for columns that have no value (partial
     * rows). If true, unwritten columns are filled with the prevailing null
     * value. If false, partial rows prompt an exception.
     */
    public synchronized void setNullAutofill(boolean nullAutofill)
    {
        this.nullAutofill = nullAutofill;
    }

    /** Returns the quote character. */
    public synchronized char getQuoteChar()
    {
        return quoteChar;
    }

    /** Sets the quote character. */
    public synchronized void setQuoteChar(char quoteChar)
    {
        this.quoteChar = quoteChar;
    }

    /**
     * Sets the quote character from string input.
     */
    public synchronized void setQuoteChar(String quoteString)
    {
        if (quoteString != null && quoteString.length() > 0)
            this.quoteChar = quoteString.charAt(0);
    }

    /**
     * Sets character used to escape quotes and other escaped characters.
     */
    public synchronized void setEscapeChar(char quoteEscapeChar)
    {
        this.escapeChar = quoteEscapeChar;
    }

    /**
     * Sets the escape character from string input.
     */
    public synchronized void setEscapeChar(String escapeString)
    {
        if (escapeString != null && escapeString.length() > 0)
            this.escapeChar = escapeString.charAt(0);
    }

    /** Returns the escape character. */
    public synchronized char getEscapeChar()
    {
        return escapeChar;
    }

    /**
     * Returns a string of characters that must be preceded by escape character.
     */
    public synchronized String getEscapedChars()
    {
        return escapedChars;
    }

    /**
     * Defines zero or more characters that must be preceded by escape
     * character.
     */
    public synchronized void setEscapedChars(String escapedChars)
    {
        if (escapedChars == null)
            this.escapedChars = "";
        else
            this.escapedChars = escapedChars;
    }

    /**
     * Returns a string of characters that are suppressed in CSV output.
     */
    public synchronized String getSuppressedChars()
    {
        return suppressedChars;
    }

    /**
     * Sets characters to be suppressed in CSV output.
     */
    public synchronized void setSuppressedChars(String suppressedChars)
    {
        if (suppressedChars == null)
            this.suppressedChars = "";
        else
            this.suppressedChars = suppressedChars;
    }

    /**
     * Returns the current count of rows written.
     */
    public int getRowCount()
    {
        return rowCount;
    }

    /**
     * Get the underlying writer.
     */
    public Writer getWriter()
    {
        return writer;
    }

    /** If true, write headers. */
    public synchronized boolean isWriteHeaders()
    {
        return writeHeaders;
    }

    /** Set to true to write headers. */
    public synchronized void setWriteHeaders(boolean writeHeaders)
    {
        this.writeHeaders = writeHeaders;
    }

    /**
     * Add a column name. Columns are indexed 1,2,3,...,N in the order added.
     * You must add all names before writing the first row.
     * 
     * @param name Column name
     * @throws CsvException Thrown
     */
    public void addColumnName(String name) throws CsvException
    {
        if (rowCount > 0)
        {
            throw new CsvException(
                    "Attempt to add column after writing one or more rows");
        }
        int index = names.size() + 1;
        names.put(name, index);
    }

    /**
     * Add a row id name. Row IDs are a numeric counter that can be inserted in
     * any column. By defining the row id name, the matching column always has
     * the batch row number automatically added to it.
     * 
     * @param name Row ID name
     * @throws CsvException Thrown if the row ID has already been set.
     */
    public void addRowIdName(String name) throws CsvException
    {
        if (rowCount > 0)
        {
            throw new CsvException(
                    "Attempt to add row ID after writing one or more rows");
        }
        else if (rowId != null)
        {
            throw new CsvException("Attempt to add row ID twice");
        }
        this.rowId = name;
        addColumnName(rowId);
    }

    /**
     * Return names in column order.
     */
    public List<String> getNames()
    {
        // Create null-filled array. The array differs by one according
        // to whether we use row IDs or not.
        int size = names.size();
        List<String> nameList = new ArrayList<String>(names.size());
        for (int i = 0; i < size; i++)
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
     * Writes current row, including headers if we are on the first row.
     * 
     * @throws CsvException Thrown if there is an inconsistency like too many
     *             columns
     * @throws IOException Thrown due to a write error
     */
    public CsvWriter write() throws CsvException, IOException
    {
        // At the top of the file optionally write headers and set the row
        // ID name.
        if (rowCount == 0 && writeHeaders)
        {
            if (writeHeaders)
            {
                writeRow(getNames());
                rowCount++;
            }
        }

        // If we have a pending row, write it now.
        if (row != null)
        {
            // Add the row count value if row IDs are enabled.
            if (rowId != null)
            {
                put(rowId, new Integer(rowCount + 1).toString());
            }

            // Check for writing too few columns.
            if (!nullAutofill && colCount < names.size())
            {
                throw new CsvException("Attempt to write partial row: row="
                        + (rowCount + 1) + " columns required=" + names.size()
                        + " columns written=" + colCount);
            }

            // Write the row.
            writeRow(row);
            row = null;
            colCount = 0;
            rowCount++;
        }

        return this;
    }

    /**
     * Forces a write of any pending row(s) and flushes data on writer.
     * 
     * @throws CsvException Thrown on an I/O failure
     */
    public CsvWriter flush() throws IOException, CsvException
    {
        write();
        writer.flush();
        return this;
    }

    /**
     * Writes value to current row. This is the base value.
     * 
     * @param index Column index where indexes are numbered 1,2,3,...,N with N
     *            being the width of the row in columns
     * @param value String value to write, already escaped if necessary
     * @throws CsvException Thrown if client attempts to write same column value
     *             twice or the row is not wide enough
     */
    public CsvWriter put(int index, String value) throws CsvException
    {
        // Initialize the character disposition table if necessary.
        if (disposition == null)
        {
            disposition = new HashMap<Character, Disposition>(256);
            for (char c : escapedChars.toCharArray())
            {
                disposition.put(c, Disposition.escape);
            }
            for (char c : suppressedChars.toCharArray())
            {
                disposition.put(c, Disposition.suppress);
            }
        }

        // Start a new row if required and fill columns with null values.
        if (row == null)
        {
            int size = getWidth();
            row = new ArrayList<String>(size);
            for (int i = 0; i < size; i++)
                row.add(null);
            colCount = 0;
        }

        // Check for invalid index.
        if (index < 1 || index > row.size())
        {
            throw new CsvException(
                    "Attempt to write to invalid column index: index=" + index
                            + " value=" + value + " row size=" + row.size());
        }

        // Check for a double write to same column. This is a safety violation.
        int arrayIndex = index - 1;
        if (row.get(arrayIndex) != null)
        {
            throw new CsvException(
                    "Attempt to write value twice to same row: index="
                            + index
                            + " old value="
                            + row.get(arrayIndex)
                            + " new value="
                            + value
                            + " (does table have a PK and is it single-column?)");
        }

        // Set the column value.
        if (value == null)
        {
            // Nulls are handled according to the null value policy.
            if (this.nullPolicy == NullPolicy.emptyString)
                value = processString("");
            else if (nullPolicy == NullPolicy.skip)
                value = null;
            else
                value = nullValue;
        }
        else
        {
            value = processString(value);
        }
        row.set(arrayIndex, value);
        colCount++;

        return this;
    }

    /**
     * Writes value to key in current row.
     */
    public CsvWriter put(String key, String value) throws CsvException
    {
        int index = names.get(key);
        return put(index, value);
    }

    // Utility routine to escape characters and enclose string in
    // quotes if so desired.
    private String processString(String base)
    {
        StringBuffer sb = new StringBuffer();
        if (quoted)
            sb.append(quoteChar);
        for (int i = 0; i < base.length(); i++)
        {
            // Fetch character and look up its disposition.
            char next = base.charAt(i);
            Disposition disp = disposition.get(next);

            // Emit the character according to CSV formatting rules.
            if (next == quoteChar && quoted)
            {
                // Escape any quote character.
                sb.append(escapeChar).append(quoteChar);
            }
            else if (disp == Disposition.escape)
            {
                // Prefix an escape character.
                sb.append(escapeChar).append(next);
            }
            else if (disp == Disposition.suppress)
            {
                // Drop the character.
                continue;
            }
            else
            {
                // If all else fails, emit the character as is.
                sb.append(next);
            }
        }
        if (quoted)
            sb.append(quoteChar);
        return sb.toString();
    }

    /**
     * Write contents of a single row, including separator.
     * 
     * @param row
     * @throws IOException
     */
    private void writeRow(List<String> row) throws IOException
    {
        for (int i = 0; i < row.size(); i++)
        {
            if (i > 0)
                writer.append(fieldSeparator);
            String value = row.get(i);
            if (value == null)
            {
                // Nulls are handled according to the null value policy.
                if (this.nullPolicy == NullPolicy.emptyString)
                    writer.append(processString(""));
                else if (nullPolicy == NullPolicy.skip)
                    writer.append(null);
                else
                    writer.append(nullValue);
            }
            else
                writer.append(row.get(i));
        }
        writer.append(recordSeparator);
    }
}