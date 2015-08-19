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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges, Stephane Giron
 */

package com.continuent.tungsten.replicator.dbms;

/**
 * Defines a SQL statement that must be replicated.  
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class RowIdData extends DBMSData
{
    private static final long serialVersionUID = 1L;

    public static final int LAST_INSERT_ID = 1;
    public static final int INSERT_ID = 2;
    
    private long rowId;
    private int type;
  
    @Deprecated
    public RowIdData(long rowId)
    {
        this(rowId, INSERT_ID);
    }
    
    public RowIdData(long value, int type)
    {
        this.rowId = value;
        this.type = type;
    }

    /**
     * Returns the SQL statement that must be replicated. 
     */
    public long getRowId()
    {
        return rowId;
    }

    /**
     * Returns the type value.
     * 
     * @return Returns the type.
     */
    public int getType()
    {
        return type;
    }

}
