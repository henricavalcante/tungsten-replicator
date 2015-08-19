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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.consistency;

import java.io.Serializable;
import java.sql.ResultSet;
import com.continuent.tungsten.replicator.database.Database;

/**
 * This interface defines a ConsistencyCheck.
 * 
 * Each consistency check class represents a consistency check specification. It
 * consists of integer check ID, String schema and table names of a table to be
 * checked and a method that returns SELECT statment that actually does the
 * crc/count calculation for the given DBMS. Result set from SELECT should
 * contain integer field 'this_cnt' and String field 'this_crc'
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public interface ConsistencyCheck extends Serializable
{
    /**
     * Enumeration of supported methods
     */
    class Method
    {
        static final String MD5   = "md5";

        /**
         * Method that utilizes primary key for offset & limit, as opposed to
         * row position (via LIMIT clause).
         */
        static final String MD5PK = "md5pk";
    };

    /**
     * @return consistency check ID
     */
    int getCheckId();

    /**
     * @return schema of the checked table
     */
    String getSchemaName();

    /**
     * @return name of the checked table
     */
    String getTableName();

    /**
     * @return offset of the row the check starts with. 1st row has offset 0.
     *         Rows counted as sorted by primary key or by all columns if
     *         there's no primary key.
     */
    int getRowOffset();

    /**
     * @return how many rows to check
     */
    int getRowLimit();

    /**
     * @return String representation of a consistency check method
     */
    String getMethod();

    /**
     * @return ResultSet should contain at least two values: int 'this_cnt' and
     *         char[] 'this_crc'.
     */
    ResultSet performConsistencyCheck(Database conn)
            throws ConsistencyException;
}