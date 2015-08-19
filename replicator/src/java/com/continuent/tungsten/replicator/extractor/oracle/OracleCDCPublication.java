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

package com.continuent.tungsten.replicator.extractor.oracle;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class OracleCDCPublication
{
    private long   pubId;
    private String name;

    private List<String> columns;

    public OracleCDCPublication(String name, long pubId)
    {
        super();
        this.name = name;
        this.pubId = pubId;
        this.columns = new LinkedList<String>();
    }

    public void addColumn(String columnName)
    {
        columns.add(columnName);
    }

    public String getPublicationName()
    {
        return name;
    }

    public long getPublicationId()
    {
        return pubId;
    }

    public String getColumnList()
    {
        StringBuffer colList = new StringBuffer();
        for (Iterator<String> iterator = columns.iterator(); iterator
                .hasNext();)
        {
            String col = iterator.next();
            if (colList.length() > 0)
                colList.append(',');
            colList.append(col);
        }
        return colList.toString();
    }

    public int getColumnsCount()
    {
        return columns.size();
    }
}
