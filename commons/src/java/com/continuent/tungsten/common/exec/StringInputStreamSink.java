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

package com.continuent.tungsten.common.exec;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.log4j.Logger;

/**
 * Runnable to read from an InputStream until exhausted or the character limit
 * is exceeded and store results in a String.
 */
public class StringInputStreamSink implements InputStreamSink
{
    private static final Logger logger    = Logger
                                                  .getLogger(StringInputStreamSink.class);
    private static final int    MAX_CHARS = 1000000;
    private final InputStream   inputStream;
    private final String        tag;
    private final int           maxChars;
    private final StringBuffer  output    = new StringBuffer();

    /**
     * Creates a new instance.
     * 
     * @param tag A tag for this processor to help with logging
     * @param in InputStream from which we read
     * @param maxChars Maximum number of characters to read (0 = MAX_CHARS)
     */
    public StringInputStreamSink(String tag, InputStream in, int maxChars)
    {
        this.tag = tag;
        this.inputStream = in;
        if (maxChars <= 0 || maxChars > MAX_CHARS)
            this.maxChars = MAX_CHARS;
        else
            this.maxChars = maxChars;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.exec.InputStreamSink#run()
     */
    public void run()
    {
        Reader reader = new InputStreamReader(inputStream);

        try
        {
            int c;
            int read = 0;

            while ((c = reader.read()) != -1)
            {
                read++;
                if (read <= maxChars)
                {
                    output.append((char) c);
                }
            }
        }
        catch (IOException ioe)
        {
            logger.warn("[" + tag + "] Error on reading stream data: "
                    + ioe.getMessage(), ioe);
        }
        finally
        {
            // Must close stream in this thread to avoid synchronization
            // problems
            try
            {
                reader.close();
            }
            catch (IOException ioe)
            {
                logger.warn("[" + tag + "] Error while closing stream: "
                        + ioe.getMessage(), ioe);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.exec.InputStreamSink#getOutput()
     */
    public String getOutput()
    {
        return output.toString();
    }
}
