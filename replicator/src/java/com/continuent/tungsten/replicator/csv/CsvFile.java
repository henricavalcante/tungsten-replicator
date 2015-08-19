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

package com.continuent.tungsten.replicator.csv;

import java.io.File;

import com.continuent.tungsten.common.csv.CsvWriter;

/**
 * Defines information specific to a single file within a CSV file set.
 */
public class CsvFile
{
    private final CsvKey    key;
    private final File      file;
    private final CsvWriter writer;

    /**
     * Creates a new instance.
     * 
     * @param key Key for this file within CSV file set
     * @param file Location of the file on file system
     * @param writer A CSV writer for the file
     */
    public CsvFile(CsvKey key, File file, CsvWriter writer)
    {
        this.key = key;
        this.file = file;
        this.writer = writer;
    }

    public CsvKey getKey()
    {
        return key;
    }

    public File getFile()
    {
        return file;
    }

    public CsvWriter getWriter()
    {
        return writer;
    }
}
