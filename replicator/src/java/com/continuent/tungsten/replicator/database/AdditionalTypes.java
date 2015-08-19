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
 * Initial developer(s): Scott Martin
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.database;

/**
 * 
 * This class defines a AdditionalTypes
 * 
 * These are types that are not part of java.sql.Types but are used in some database
 * vendors.  An example would be a column type in Oracle that has no corresponding
 * java.sql.Type.
 * 
 * There is a copy of this file in the bristlecone project.
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class AdditionalTypes
{
    /**
     * We have started the types at 1500 as it appears to be available
     * address space in java.sql.Types.
     */
    
    /**
     *  Oracle specific types.
     */
    public final static int TIMESTAMPLOCAL     =  1500; // TIMESTAMP WITH LOCAL TIME ZONE
    public final static int XML                =  1501; // XML Type

    /**
     *  Unsigned MySQL types
     */
    public final static int UTINYINT    =  1510; // UNSIGNED TINYINT   0-255
    public final static int USMALLINT   =  1511; // UNSIGNED SMALLINT  0-65535
    public final static int UMEDIUMINT  =  1512; // UNSIGNED MEDIUMINT 0-16777215
    public final static int UINT        =  1513; // UNSIGNED INT       0-4294967295
    public final static int UBIGINT     =  1514; // UNSIGNED BIGINT    0-18446744073709551615

    public final static int MEDIUMINT   =  1515; // MySQL's "mediumint" col type


    // Prevent instantiation
    private AdditionalTypes() {}
}
