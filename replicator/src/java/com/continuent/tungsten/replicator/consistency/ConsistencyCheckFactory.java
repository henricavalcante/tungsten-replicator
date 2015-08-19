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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.consistency;

import com.continuent.tungsten.replicator.database.Table;

/**
 * ConsistencyCheckFactory creates ConsistencyCheck objects
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class ConsistencyCheckFactory
{
    public static ConsistencyCheck createConsistencyCheck(int id, Table table,
            int rowOffset, int rowLimit, String method,
            boolean checkColumnNames, boolean checkColumnTypes)
            throws ConsistencyException
    {
        if (method.compareToIgnoreCase(ConsistencyCheck.Method.MD5) == 0)
        {
            return new ConsistencyCheckMD5(id, table, rowOffset, rowLimit,
                    checkColumnNames, checkColumnTypes);
        }
        else if (method.compareToIgnoreCase(ConsistencyCheck.Method.MD5PK) == 0)
        {
            return new ConsistencyCheckMD5(id, table, rowOffset, rowLimit,
                    checkColumnNames, checkColumnTypes, true);
        }
        else
        {
            throw new ConsistencyException(
                    "Unsupported consistency check method: '" + method + "'");
        }
    }
}
