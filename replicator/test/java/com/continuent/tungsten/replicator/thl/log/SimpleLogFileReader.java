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

package com.continuent.tungsten.replicator.thl.log;

/**
 * Log reader task used for log file reading tests.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SimpleLogFileReader implements Runnable
{
    LogFile   tf;
    long      howMany;
    long      recordsRead;
    long      bytesRead;
    long      crcFailures;
    Exception error;

    /** Store file instance. */
    public SimpleLogFileReader(LogFile tf, long maxRecords)
    {
        this.tf = tf;
        this.howMany = maxRecords;
    }

    /** Read all records from file. */
    public void run()
    {
        while (recordsRead < howMany)
        {
            try
            {
                // Read until we run out of records or hit exception.
                LogRecord rec;
                rec = tf.readRecord(2000);
                if (rec.isEmpty())
                    break;

                // Update counters.
                recordsRead++;
                bytesRead += rec.getRecordLength();
                if (!rec.checkCrc())
                    crcFailures++;
            }
            catch (Exception e)
            {
                error = e;
                break;
            }
        }
    }
}
