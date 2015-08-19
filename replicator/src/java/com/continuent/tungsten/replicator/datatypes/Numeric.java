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

package com.continuent.tungsten.replicator.datatypes;

import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;

/**
 * This class represents a numeric value saved in THL event.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class Numeric
{
    boolean    isNegative   = false;
    Long       extractedVal = null;
    ColumnSpec columnSpec   = null;

    public Numeric(ColumnSpec columnSpec, ColumnVal value)
    {
        this.columnSpec = columnSpec;
        if (value.getValue() instanceof Integer)
        {
            int val = (Integer) value.getValue();
            isNegative = val < 0;
            extractedVal = Long.valueOf(val);
        }
        else if (value.getValue() instanceof Long)
        {
            long val = (Long) value.getValue();
            isNegative = val < 0;
            extractedVal = Long.valueOf(val);
        }
    }

    /**
     * Is the number negative?
     */
    public boolean isNegative()
    {
        return isNegative;
    }

    /**
     * Representation of the numeric value in Long format.
     * 
     * @return null, if value couldn't be extracted (type mismatch?).
     */
    public Long getExtractedValue()
    {
        return extractedVal;
    }

    /**
     * Returns column specification used to create this numeric object.
     */
    public ColumnSpec getColumnSpec()
    {
        return columnSpec;
    }
    
    public String toString()
    {
        return extractedVal.toString();
    }
}
