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

import java.sql.Timestamp;

import com.continuent.tungsten.replicator.dbms.RowChangeData;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class CDCDataMessage extends CDCMessage
{

    private RowChangeData rowChange;
    private Timestamp     timestamp;
    private long          SCN;

    public CDCDataMessage(RowChangeData rowChange, Timestamp sourceTStamp,
            long currentSCN)
    {
        this.rowChange = rowChange;
        this.timestamp = sourceTStamp;
        this.SCN = currentSCN;
    }

    public RowChangeData getRowChange()
    {
        return rowChange;
    }

    public Timestamp getTimestamp()
    {
        return timestamp;
    }

    public long getSCN()
    {
        return SCN;
    }
}
