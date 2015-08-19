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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to transform java.sql.Types integer values into readable strings.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class SQLTypes
{
    private Map<Integer, String> sqlTypes = new HashMap<Integer, String>();

    public SQLTypes()
    {
        // Initialize available SQL types.
        for (Field field : java.sql.Types.class.getFields())
        {
            try
            {
                sqlTypes.put((Integer) field.get(null), field.getName());
            }
            catch (IllegalArgumentException e)
            {
                // Ignore.
            }
            catch (IllegalAccessException e)
            {
                // Ignore.
            }
        }
    }

    /**
     * Transforms integer, representing java.sql.Types value, into to human
     * readable string.
     * 
     * @param sqlType java.sql.Types value.
     * @return String representation (name) of the java.sql.Types variable.
     *         null, if translation was not found.
     */
    public String sqlTypeToString(int sqlType)
    {
        return sqlTypes.get(sqlType);
    }
}
