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

package com.continuent.tungsten.common.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Implements useful utility functions related to file system I/O.
 */
public class FileIOUtils
{
    /**
     * Copy uninterpreted bytes from an input stream to an output stream. This
     * routine uses the current positions of each stream whatever those happen
     * to be.
     * 
     * @param in InputStream from which to read
     * @param out OutputStream to which to write
     * @param bufferSize Size of transfer buffer
     * @param close If true, try to close streams at end
     * @throws IOException Thrown if the copy fails for any reason, including
     *             due to inability to close streams
     */
    public static void copyBytes(InputStream in, OutputStream out,
            int bufferSize, boolean close) throws IOException
    {
        BufferedInputStream from = new BufferedInputStream(in);
        BufferedOutputStream to = new BufferedOutputStream(out);

        // Copy data.
        byte[] bytes = new byte[bufferSize];
        int size;
        while ((size = from.read(bytes, 0, bufferSize)) >= 0)
        {
            to.write(bytes, 0, size);
        }

        // Close streams if requested.
        if (close)
        {
            from.close();
            to.flush();
            to.close();
        }
    }
}