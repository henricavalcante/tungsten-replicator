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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.dbms;

import java.util.ArrayList;
import java.util.LinkedList;

import com.continuent.tungsten.replicator.event.ReplOption;

/**
 * This class defines a set of one or more row changes.
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class RowChangeData extends DBMSData
{
    public enum ActionType
    {
        INSERT, DELETE, UPDATE
    }

    private static final long       serialVersionUID = 1L;
    private ArrayList<OneRowChange> rowChanges;

    /**
     * Creates a new <code>RowChangeData</code> object
     */
    public RowChangeData()
    {
        super();
        rowChanges = new ArrayList<OneRowChange>();
    }

    public ArrayList<OneRowChange> getRowChanges()
    {
        return rowChanges;
    }

    public void setRowChanges(ArrayList<OneRowChange> rowChanges)
    {
        this.rowChanges = rowChanges;
    }

    public void appendOneRowChange(OneRowChange rowChange)
    {
        this.rowChanges.add(rowChange);
    }

    public void addOptions(LinkedList<ReplOption> savedOptions)
    {
        this.options.addAll(savedOptions);
    }
}
