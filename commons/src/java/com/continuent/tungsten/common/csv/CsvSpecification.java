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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Reader;
import java.io.Writer;

import org.apache.commons.lang.StringEscapeUtils;

/**
 * Holds specification of properties for CSV input and output from which it
 * generates CSVWriter and CSVReader instances.
 */
public class CsvSpecification
{
    // Properties.
    private String     fieldSeparator          = ",";
    private String     recordSeparator         = "\n";
    private boolean    collapseFieldSeparators = false;
    private boolean    useHeaders              = false;
    private boolean    useQuotes               = false;
    private String     quote                   = "\"";
    private String     escape                  = "\\";
    private String     escapedChars            = "";
    private String     suppressedChars         = "";
    private NullPolicy nullPolicy              = NullPolicy.nullValue;
    private String     nullValue               = null;
    private boolean    nullAutofill            = false;

    /**
     * Returns a specification suitable for a particular DBMS store type.
     * Supported types include the following:
     * <p/>
     * <ul>
     * <li>default - Default settings</li>
     * <li>hive - Standard settings for Hadoop Hive external table</li>
     * </ul>
     * (Other settings will be added in due time.)
     * 
     * @param type
     * @return The specified type or a null if the type is unknown
     */
    public static CsvSpecification getSpecification(String type)
    {
        CsvSpecification spec = null;
        if ("default".equals(type))
        {
            spec = new CsvSpecification();
        }
        else if ("hive".equals(type))
        {
            spec = new CsvSpecification();
            spec.setFieldSeparator("\u0001");
            spec.setRecordSeparator("\n");
            spec.setEscape("\\");
            spec.setEscapedChars("\u0001\\");
            spec.setNullPolicy(NullPolicy.nullValue);
            spec.setNullValue("\\N");
            spec.setUseHeaders(false);
            spec.setUseQuotes(false);
            spec.setSuppressedChars("\n\r");
        }
        else if ("mysql".equals(type))
        {
            spec = new CsvSpecification();
            spec.setFieldSeparator(",");
            spec.setRecordSeparator("\n");
            spec.setEscape("\\");
            spec.setEscapedChars("\\");
            spec.setNullPolicy(NullPolicy.nullValue);
            spec.setNullValue("\\N");
            spec.setUseHeaders(false);
            spec.setUseQuotes(true);
            spec.setQuote("\"");
        }
        else if ("oracle".equals(type))
        {
            spec = new CsvSpecification();
            spec.setFieldSeparator(",");
            spec.setRecordSeparator("\n");
            spec.setEscape("\\");
            spec.setEscapedChars("\\");
            spec.setNullPolicy(NullPolicy.nullValue);
            spec.setNullValue("\\N");
            spec.setUseHeaders(false);
            spec.setUseQuotes(true);
            spec.setQuote("\"");
        }
        else if ("vertica".equals(type))
        {
            spec = new CsvSpecification();
            spec.setFieldSeparator(",");
            spec.setRecordSeparator("\n");
            spec.setEscape("\\");
            spec.setEscapedChars("\\");
            spec.setNullPolicy(NullPolicy.skip);
            spec.setUseHeaders(false);
            spec.setUseQuotes(true);
            spec.setQuote("\"");
            spec.setSuppressedChars("\n");
        }
        else if ("redshift".equals(type))
        {
            spec = new CsvSpecification();
            spec.setFieldSeparator(",");
            spec.setRecordSeparator("\n");
            spec.setEscape("\""); // Escaped a quote with a quote in Redshift.
            spec.setEscapedChars(""); // Nothing to escape apart quotes.
            spec.setNullPolicy(NullPolicy.skip);
            spec.setUseHeaders(false);
            spec.setUseQuotes(true);
            spec.setQuote("\"");
            spec.setSuppressedChars("\n");
        }
        return spec;
    }

    /**
     * Sets the field separator character.
     */
    public void setFieldSeparator(String fieldSeparator)
    {
        this.fieldSeparator = StringEscapeUtils.unescapeJava(fieldSeparator);
    }

    /**
     * Returns field separator character.
     */
    public String getFieldSeparator()
    {
        return this.fieldSeparator;
    }

    /**
     * Returns true if successive input separators should be treated as a single
     * separator.
     */
    public boolean isCollapseFieldSeparators()
    {
        return collapseFieldSeparators;
    }

    /**
     * If set to true treat successive input separators as a single separator.
     */
    public void setCollapseFieldSeparators(boolean collapseFieldSeparators)
    {
        this.collapseFieldSeparators = collapseFieldSeparators;
    }

    /**
     * Sets the record separator character.
     */
    public void setRecordSeparator(String recordSeparator)
    {
        this.recordSeparator = StringEscapeUtils.unescapeJava(recordSeparator);
    }

    /**
     * Returns record separator character.
     */
    public String getRecordSeparator()
    {
        return this.recordSeparator;
    }

    /**
     * Returns true if CSV contains column headers in first row.
     */
    public synchronized boolean isUseHeaders()
    {
        return useHeaders;
    }

    /**
     * If set to true first row must contain column headers.
     */
    public synchronized void setUseHeaders(boolean useHeaders)
    {
        this.useHeaders = useHeaders;
    }

    /** Returns true if values will be enclosed by a quote character. */
    public synchronized boolean isUseQuotes()
    {
        return useQuotes;
    }

    /** Set to true to enable quoting. */
    public synchronized void setUseQuotes(boolean quoted)
    {
        this.useQuotes = quoted;
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
    public synchronized String getQuote()
    {
        return this.quote;
    }

    /** Sets the quote character. */
    public synchronized void setQuote(String quoteChar)
    {
        this.quote = quoteChar;
    }

    /**
     * Sets character used to escape quotes and other escaped characters.
     */
    public synchronized void setEscape(String quoteEscapeChar)
    {
        this.escape = StringEscapeUtils.unescapeJava(quoteEscapeChar);
    }

    /** Returns the escape character. */
    public synchronized String getEscape()
    {
        return escape;
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
     * Instantiate a new CsvWriter with output to provided writer.
     */
    public CsvWriter createCsvWriter(Writer writer)
    {
        return createCsvWriter(new BufferedWriter(writer));
    }

    /**
     * Instantiate a new CsvWriter with output to provided buffered writer. This
     * call allows clients to set buffering parameters themselves.
     */
    public CsvWriter createCsvWriter(BufferedWriter writer)
    {
        CsvWriter csvWriter = new CsvWriter(writer);
        csvWriter.setEscapeChar(escape);
        csvWriter.setEscapedChars(escapedChars);
        csvWriter.setNullAutofill(nullAutofill);
        csvWriter.setNullPolicy(nullPolicy);
        csvWriter.setNullValue(nullValue);
        csvWriter.setQuoteChar(quote);
        csvWriter.setQuoted(useQuotes);
        csvWriter.setFieldSeparator(fieldSeparator);
        csvWriter.setRecordSeparator(recordSeparator);
        csvWriter.setSuppressedChars(suppressedChars);
        csvWriter.setWriteHeaders(useHeaders);
        return csvWriter;
    }

    /**
     * Instantiate a new CsvReader with input from provided reader.
     */
    public CsvReader createCsvReader(Reader reader)
    {
        return createCsvReader(new BufferedReader(reader));
    }

    /**
     * Instantiate a new CsvWriter with input from provided buffered reader.
     * This call allows clients to set buffering parameters themselves.
     */
    public CsvReader createCsvReader(BufferedReader reader)
    {
        CsvReader csvReader = new CsvReader(reader);
        csvReader.setFieldSeparator(fieldSeparator);
        csvReader.setRecordSeparator(recordSeparator);
        csvReader.setUseHeaders(useHeaders);
        return csvReader;
    }
}
