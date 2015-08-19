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

import java.io.File;

/**
 * Provides utility methods to open log files quickly.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogHelper
{
    /**
     * Create and return a writable file. Any existing file is deleted.
     */
    static LogFile createLogFile(String name, long seqno) throws Exception
    {
        // Create the file and return it if we want to write.
        File logfile = new File(name);
        logfile.delete();
        LogFile tf = new LogFile(logfile);
        tf.create(seqno);
        return tf;
    }

    /**
     * Open an existing file for reading.
     */
    static LogFile openExistingFileForRead(String name) throws Exception
    {
        File logfile = new File(name);
        LogFile tf = new LogFile(logfile);
        tf.openRead();
        return tf;
    }

    /**
     * Open an existing file for writing.
     */
    static LogFile openExistingFileForWrite(String name) throws Exception
    {
        File logfile = new File(name);
        LogFile tf = new LogFile(logfile);
        tf.openWrite();
        return tf;
    }
}
