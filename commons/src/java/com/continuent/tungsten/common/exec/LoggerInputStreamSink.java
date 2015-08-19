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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):
 */

package com.continuent.tungsten.common.exec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.log4j.Logger;

/**
 * Runnable to read from an InputStream until exhausted and append results into
 * a Logger.
 */
public class LoggerInputStreamSink implements InputStreamSink
{
    private static final Logger logger    = Logger
                                                  .getLogger(LoggerInputStreamSink.class);
    private final InputStream   inputStream;
    private final Logger        outLogger;
    private final String        tag;
    private final boolean       info;
    private final StringBuffer  output    = new StringBuffer();

    /**
     * Creates a new instance.
     * 
     * @param tag A tag for this processor to help with logging. If "stderr" is
     *            given, log will be appended into ERROR stream instead of the
     *            usual INFO.
     * @param in InputStream from which we read.
     * @param outLogger Logger to append output of the stream into.
     */
    public LoggerInputStreamSink(String tag, InputStream in, Logger outLogger)
    {
        this.tag = tag;
        if(tag.compareTo("stderr")==0)
            this.info = false;
        else
            this.info = true;
        this.inputStream = in;
        this.outLogger = outLogger;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.exec.InputStreamSink#run()
     */
    public void run()
    {
        Reader reader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(reader);

        try
        {
            String s;
            while ((s = bufferedReader.readLine()) != null)
            {
                if (info)
                    outLogger.info(s);
                else
                    outLogger.error(s);
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
