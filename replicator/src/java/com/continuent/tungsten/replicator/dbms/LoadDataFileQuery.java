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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.dbms;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class LoadDataFileQuery extends StatementData
{
    private static final long    serialVersionUID           = 1L;

    private int                  fileId;
    private int                  filenameStartPos;
    
    private int                  filenameEndPos;

    private static final String  LOAD_DATA_FILENAME         = "((LOW_PRIORITY|CONCURRENT)\\s+)?(LOCAL\\s+)?INFILE\\s+(\\'.*\\')\\s+((REPLACE|IGNORE)\\s+)?INTO";

    private static final Pattern LOAD_DATA_FILENAME_PATTERN = Pattern
                                                                    .compile(
                                                                            LOAD_DATA_FILENAME,
                                                                            Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

    public LoadDataFileQuery(String queryString, long time, String defaultDb,
            int fileId, int startingPos, int endingPos)
    {
        super(queryString, time, defaultDb);
        this.fileId = fileId;
        this.filenameStartPos = startingPos;
        this.filenameEndPos = endingPos;
    }

    public int getFileID()
    {
        return fileId;
    }

    public void setLocalFile(File temporaryFile)
    {
        String query = this.getQuery();

        StringBuffer strBuf = new StringBuffer(query.substring(0,
                filenameStartPos));

        String fileName = getFileName();
        Matcher matcher = LOAD_DATA_FILENAME_PATTERN.matcher(fileName);
        if (matcher.matches())
        {
            strBuf.append(" ");
            strBuf.append(fileName.replace(matcher.group(4), "'"
                    + temporaryFile.getPath() + "'"));
            strBuf.append(" ");
        }

        strBuf.append(query.substring(filenameEndPos));
        this.setQuery(strBuf.toString());
    }

    public boolean isLocal()
    {
        Matcher matcher = LOAD_DATA_FILENAME_PATTERN.matcher(getFileName());
        if (matcher.matches())
        {
            return matcher.group(3) != null;
        }
        return false;
    }

    private String getFileName()
    {
        return this.getQuery().substring(filenameStartPos, filenameEndPos)
                .trim();
    }

    public int getFilenameStartPos()
    {
        return filenameStartPos;
    }

    public int getFilenameEndPos()
    {
        return filenameEndPos;
    }

}
