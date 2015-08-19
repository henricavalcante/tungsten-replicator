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
 * Initial developer(s): Robert Hodges
 * Contributor(s):  
 */

package com.continuent.tungsten.replicator.prefetch;

/**
 * Contains utility methods to perform standard transformations to aid prefetch.
 * This logic is encapsulated in a separate class that is easy to unit test.
 */
public class PrefetchSqlTransformer
{
    /**
     * Adds a LIMIT clause to the query if none is present already *and* the
     * limit is greater than 0.
     * 
     * @param query Query which should be adorned
     * @param limit Number of rows to limit; must be greater than 0
     * @return Query with limit clause
     */
    public String addLimitToQuery(String query, int limit)
    {
        // Check preconditions, then add the clause.
        if (limit <= 0)
            return query;
        else if (query.toLowerCase().contains("limit"))
            return query;
        else
        {
            String limitClause = String.format(" LIMIT %d", limit);
            return query + limitClause;
        }
    }
}