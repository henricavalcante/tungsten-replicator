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

package com.continuent.tungsten.replicator.filter;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.EventGenerationHelper;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;

/**
 * This class implements a test of the ReplicateFilter class. It checks the
 * basic semantics of dropping and allowing various combinations of schema names
 * and tables. Test cases check row updates as well as statements.
 */
public class ReplicateFilterTest extends TestCase
{
    private FilterVerificationHelper filterHelper = new FilterVerificationHelper();
    private EventGenerationHelper    eventHelper  = new EventGenerationHelper();

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {

    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Verify that the filter allows events through if no properties are set.
     */
    public void testUnspecifiedProperties() throws Exception
    {
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        filterHelper.setFilter(rf);
        verifyStmtAccept(filterHelper, 0, null,
                "insert into bar(val) values(1)");
        filterHelper.done();
    }

    /**
     * Verify that we allow operations on schemas including wildcard
     * substitutions.
     */
    public void testSchemasAccepted() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setDo("foo,ba?, foobar*");
        filterHelper.setFilter(rf);

        // Confirm that the following commands are accepted if the default
        // database is foo.
        verifyStmtAccept(filterHelper, 0, null, "create database foo");
        verifyStmtAccept(filterHelper, 1, null, "drop database bar");
        verifyStmtAccept(filterHelper, 2, null,
                "insert into foobar1.x1 values(1,2,3)");
        verifyStmtAccept(filterHelper, 3, null,
                "update test_tab set age=32 where id=1");

        // Just for calibration ensure we ignore a non-matching schema.
        verifyStmtIgnore(filterHelper, 4, null,
                "insert into test.tab values(52,1)");

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we properly ignore operations on schemas including those with
     * wildcard substitutions.
     */
    public void testSchemasIgnored() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setIgnore("foo,foobar?,bar*");
        filterHelper.setFilter(rf);

        // Confirm that the following commands are ignored if the default
        // database is foo.
        verifyStmtIgnore(filterHelper, 0, "bar", "create database foo");
        verifyStmtIgnore(filterHelper, 1, "foo", "drop database bar");
        verifyStmtIgnore(filterHelper, 2, "foo",
                "delete from bar2.test where id=2");
        verifyStmtIgnore(filterHelper, 3, "foo",
                "insert into foobar1.x1 values(1,2,3)");
        verifyStmtIgnore(filterHelper, 4, "foo",
                "update test_tab set age=32 where id=1");

        // Just for calibration ensure we accept a non-matching schema.
        verifyStmtAccept(filterHelper, 5, "foo",
                "insert into test.tab values(52,1)");

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we handle cases where a subset of databases and/or tables is
     * ignored.
     */
    public void testSchemasIgnoreSubset() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setDo("foobar?,bar*,foo");
        rf.setIgnore("foo.test,foobar2,bar23*");
        filterHelper.setFilter(rf);

        // Confirm that the following commands are accepted if the default
        // database is foo.
        verifyStmtAccept(filterHelper, 0, null, "create database foobar1");
        verifyStmtAccept(filterHelper, 1, "foo", "drop table foo.test2");
        verifyStmtAccept(filterHelper, 2, "foo",
                "delete from bar2.test where id=2");

        // Confirm that we ignore subset values.
        verifyStmtIgnore(filterHelper, 3, "foo",
                "delete from foo.test where id=2");
        verifyStmtIgnore(filterHelper, 4, "foo",
                "create table foobar2.foobar1 (id int)");
        verifyStmtIgnore(filterHelper, 5, "foo", "drop database bar234");

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we accept and ignore databases appropriately when using row
     * updates.
     */
    public void testRowHandling() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setDo("foobar?,bar*,foo");
        rf.setIgnore("*.test,foobar2,bar23*");
        filterHelper.setFilter(rf);

        // Verify accepted events.
        String names[] = {"id"};
        Long values[] = {new Long(99)};
        verifyRowAccept(filterHelper, 0, "foobar1", "foobar2", names, values);
        verifyRowAccept(filterHelper, 1, "bar2", "foo", names, values);
        verifyRowAccept(filterHelper, 2, "foo", "test2", names, values);

        // Verify ignored events.
        verifyRowIgnore(filterHelper, 3, "foo", "test", names, values);
        verifyRowIgnore(filterHelper, 4, "foobar2", "foobar1", names, values);
        verifyRowIgnore(filterHelper, 5, "bar234", "foobar1", names, values);

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we handle table acceptance including cases where the table
     * names are wildcarded.
     */
    public void testTableAccept() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setDo("foo.*,bar.test1,bar.wild*,bar.w?");
        filterHelper.setFilter(rf);

        // Confirm that the following commands are accepted.
        verifyStmtAccept(filterHelper, 0, "bar",
                "insert into foo.test values(1)");
        verifyStmtAccept(filterHelper, 1, "foo", "delete from test1 where id=1");
        verifyStmtAccept(filterHelper, 2, "bar", "insert into wild1 values(1)");
        verifyStmtAccept(filterHelper, 3, "bar", "update w2 set age=29");

        // Confirm that the following are ignored, because they do not trigger
        // acceptance.
        verifyStmtIgnore(filterHelper, 4, null, "create database foo");
        verifyStmtIgnore(filterHelper, 5, "bar", "insert into test2 values(1)");
        verifyStmtIgnore(filterHelper, 6, "bar", "create table will1 (id int)");
        verifyStmtIgnore(filterHelper, 7, "bar", "delete from w22");

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we always accept the Tungsten catalog even if it is
     * explicitly ignored.
     */
    public void testTungstenCatalogAccept() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setIgnore("tungsten_foo");
        filterHelper.setFilter(rf);

        // Confirm that the following commands are accepted.
        verifyStmtAccept(filterHelper, 0, "bar",
                "delete from tungsten_foo.trep_commit_seqno where task_id=9");
        verifyRowAccept(filterHelper, 1, "tungsten_foo", "trep_commit_seqno",
                new String[]{"task_id"}, new Object[]{0});

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we allow operations on schemas including wildcard
     * substitutions.
     */
    public void testSchemasAcceptedWithFile() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setFilePrefix("filter/testSchemasAcceptedWithFile");
        filterHelper.setFilter(rf);

        // Confirm that the following commands are accepted if the default
        // database is foo.
        verifyStmtAccept(filterHelper, 0, null, "create database foo");
        verifyStmtAccept(filterHelper, 1, null, "drop database bar");
        verifyStmtAccept(filterHelper, 2, null,
                "insert into foobar1.x1 values(1,2,3)");
        verifyStmtAccept(filterHelper, 3, null,
                "update test_tab set age=32 where id=1");

        // Just for calibration ensure we ignore a non-matching schema.
        verifyStmtIgnore(filterHelper, 4, null,
                "insert into test.tab values(52,1)");

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we properly ignore operations on schemas including those with
     * wildcard substitutions.
     */
    public void testSchemasIgnoredWithFile() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setFilePrefix("filter/testSchemasIgnoredWithFile");
        filterHelper.setFilter(rf);

        // Confirm that the following commands are ignored if the default
        // database is foo.
        verifyStmtIgnore(filterHelper, 0, "bar", "create database foo");
        verifyStmtIgnore(filterHelper, 1, "foo", "drop database bar");
        verifyStmtIgnore(filterHelper, 2, "foo",
                "delete from bar2.test where id=2");
        verifyStmtIgnore(filterHelper, 3, "foo",
                "insert into foobar1.x1 values(1,2,3)");
        verifyStmtIgnore(filterHelper, 4, "foo",
                "update test_tab set age=32 where id=1");

        // Just for calibration ensure we accept a non-matching schema.
        verifyStmtAccept(filterHelper, 5, "foo",
                "insert into test.tab values(52,1)");

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we handle cases where a subset of databases and/or tables is
     * ignored.
     */
    public void testSchemasIgnoreSubsetWithFile() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setFilePrefix("filter/testSchemasIgnoreSubsetWithFile");
        filterHelper.setFilter(rf);

        // Confirm that the following commands are accepted if the default
        // database is foo.
        verifyStmtAccept(filterHelper, 0, null, "create database foobar1");
        verifyStmtAccept(filterHelper, 1, "foo", "drop table foo.test2");
        verifyStmtAccept(filterHelper, 2, "foo",
                "delete from bar2.test where id=2");

        // Confirm that we ignore subset values.
        verifyStmtIgnore(filterHelper, 3, "foo",
                "delete from foo.test where id=2");
        verifyStmtIgnore(filterHelper, 4, "foo",
                "create table foobar2.foobar1 (id int)");
        verifyStmtIgnore(filterHelper, 5, "foo", "drop database bar234");

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we accept and ignore databases appropriately when using row
     * updates.
     */
    public void testRowHandlingWithFile() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setFilePrefix("filter/testRowHandlingWithFile");
        filterHelper.setFilter(rf);

        // Verify accepted events.
        String names[] = {"id"};
        Long values[] = {new Long(99)};
        verifyRowAccept(filterHelper, 0, "foobar1", "foobar2", names, values);
        verifyRowAccept(filterHelper, 1, "bar2", "foo", names, values);
        verifyRowAccept(filterHelper, 2, "foo", "test2", names, values);

        // Verify ignored events.
        verifyRowIgnore(filterHelper, 3, "foo", "test", names, values);
        verifyRowIgnore(filterHelper, 4, "foobar2", "foobar1", names, values);
        verifyRowIgnore(filterHelper, 5, "bar234", "foobar1", names, values);

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we handle table acceptance including cases where the table
     * names are wildcarded.
     */
    public void testTableAcceptWithFile() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setFilePrefix("filter/testTableAcceptWithFile");
        filterHelper.setFilter(rf);

        // Confirm that the following commands are accepted.
        verifyStmtAccept(filterHelper, 0, "bar",
                "insert into foo.test values(1)");
        verifyStmtAccept(filterHelper, 1, "foo", "delete from test1 where id=1");
        verifyStmtAccept(filterHelper, 2, "bar", "insert into wild1 values(1)");
        verifyStmtAccept(filterHelper, 3, "bar", "update w2 set age=29");

        // Confirm that the following are ignored, because they do not trigger
        // acceptance.
        verifyStmtIgnore(filterHelper, 4, null, "create database foo");
        verifyStmtIgnore(filterHelper, 5, "bar", "insert into test2 values(1)");
        verifyStmtIgnore(filterHelper, 6, "bar", "create table will1 (id int)");
        verifyStmtIgnore(filterHelper, 7, "bar", "delete from w22");

        // All done.
        filterHelper.done();
    }

    /**
     * Verify that we always accept the Tungsten catalog even if it is
     * explicitly ignored.
     */
    public void testTungstenCatalogAcceptWithFile() throws ReplicatorException,
            InterruptedException
    {
        // Configure the filter to allow databases including wild cards.
        ReplicateFilter rf = new ReplicateFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setFilePrefix("filter/testTungstenCatalogAcceptWithFile");
        filterHelper.setFilter(rf);

        // Confirm that the following commands are accepted.
        verifyStmtAccept(filterHelper, 0, "bar",
                "delete from tungsten_foo.trep_commit_seqno where task_id=9");
        verifyRowAccept(filterHelper, 1, "tungsten_foo", "trep_commit_seqno",
                new String[]{"task_id"}, new Object[]{0});

        // All done.
        filterHelper.done();
    }

    // Confirms that a particular event is accepted.
    private void verifyAccept(FilterVerificationHelper filterHelper,
            ReplDBMSEvent e) throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e2 = filterHelper.filter(e);
        assertTrue("Should allow event, seqno=" + e.getSeqno(), e == e2);
    }

    // Confirms that a particular event is ignored.
    private void verifyIgnore(FilterVerificationHelper filterHelper,
            ReplDBMSEvent e) throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e2 = filterHelper.filter(e);
        assertNull("Should ignore event: seqno=" + e.getSeqno(), e2);
    }

    // Confirms that a particular statement-based event is accepted.
    private void verifyStmtAccept(FilterVerificationHelper filterHelper,
            long seqno, String defaultSchema, String query)
            throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e = eventHelper.eventFromStatement(seqno, defaultSchema,
                query);
        verifyAccept(filterHelper, e);
    }

    // Confirms that a particular statement-based event is ignored.
    private void verifyStmtIgnore(FilterVerificationHelper filterHelper,
            long seqno, String defaultSchema, String query)
            throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e = eventHelper.eventFromStatement(seqno, defaultSchema,
                query);
        verifyIgnore(filterHelper, e);
    }

    // Confirms that a particular row event is accepted. We use inserts for this
    // purpose.
    private void verifyRowAccept(FilterVerificationHelper filterHelper,
            long seqno, String schema, String table, String[] names,
            Object[] values) throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e = eventHelper.eventFromRowInsert(seqno, schema, table,
                names, values, 0, true);
        verifyAccept(filterHelper, e);
    }

    // Confirms that a particular row event is accepted. We use inserts for this
    // purpose.
    private void verifyRowIgnore(FilterVerificationHelper filterHelper,
            long seqno, String schema, String table, String[] names,
            Object[] values) throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e = eventHelper.eventFromRowInsert(seqno, schema, table,
                names, values, 0, true);
        verifyIgnore(filterHelper, e);
    }

    /**
     * Verify that we allow or ignore specific tables.
     */
}