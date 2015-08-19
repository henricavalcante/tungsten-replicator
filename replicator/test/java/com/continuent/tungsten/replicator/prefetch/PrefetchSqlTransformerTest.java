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

import junit.framework.TestCase;

/**
 * Tests prefetch transformation.
 */
public class PrefetchSqlTransformerTest extends TestCase
{
    /**
     * Verify we add limit if none exists and the limit is greater than 0.
     */
    public void testAddLimit() throws Exception
    {
        PrefetchSqlTransformer pst = new PrefetchSqlTransformer();

        String[] queries = {"select * from foo",
                "SELECT count(*) FROM foo ORDER by id ascending"};

        for (String q1 : queries)
        {
            String q2 = pst.addLimitToQuery(q1, 1);
            assertTrue("Length: " + q1, q2.length() > q1.length());
            assertTrue("Contents: " + q1, q2.toLowerCase().contains("limit 1"));

            String q3 = pst.addLimitToQuery(q1, 0);
            assertEquals("limit 0: " + q1, q1, q3);
        }
    }

    /**
     * Verify we do not add a limit if it already anywhere in the query.
     */
    public void testAddLimitWhenExists() throws Exception
    {
        PrefetchSqlTransformer pst = new PrefetchSqlTransformer();

        String[] queries = {"select * from foo limit 25",
                "select * from foo LIMIT 1",
                "SELECT count(*) FROM mylimit ORDER by id ascending"};

        for (String q1 : queries)
        {
            String q2 = pst.addLimitToQuery(q1, 25);
            assertEquals("No limited added: " + q1, q1, q2);
        }
    }
}