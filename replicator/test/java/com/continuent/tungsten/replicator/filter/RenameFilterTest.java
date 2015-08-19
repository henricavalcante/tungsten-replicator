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

package com.continuent.tungsten.replicator.filter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.EventGenerationHelper;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;

/**
 * This class implements a test of the RenameFilter class. It checks that
 * renaming of schemas, tables and columns works as expected. Test cases check
 * row updates.
 */
public class RenameFilterTest extends TestCase
{
    private static Logger            logger          = Logger.getLogger(RenameFilterTest.class);

    private FilterVerificationHelper filterHelper    = new FilterVerificationHelper();
    private EventGenerationHelper    eventHelper     = new EventGenerationHelper();

    private final String             definitionsFile = "RenameFilterTest.csv";

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
        File file = new File(definitionsFile);
        file.delete();
    }

    /**
     * Verify that the filter raises exception if no definitions file is
     * provided.
     */
    public void testUnspecifiedProperties() throws Exception
    {
        RenameFilter rf = new RenameFilter();
        rf.setTungstenSchema("tungsten_foo");
        try
        {
            filterHelper.setFilter(rf);
            filterHelper.done();

            fail("Exception not thrown during configure though definitionsFile property was not set");
        }
        catch (ReplicatorException e)
        {
            // OK - it should have threw an exception.
            logger.info(e);
        }
    }

    /**
     * Verify schemas are renamed as expected.
     */
    public void testRenameSchema() throws ReplicatorException,
            InterruptedException, IOException
    {
        // Prepare rename definitions file.
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,*,*,schemaz2,-,- # Schema renamed for all tables. Least priority.");
        out.println("schemaz,tablew,*,schemaz1,-,- # This table moved to a different schema.");
        out.println("schemaz,tablee,*,schemazz1,tableee,- # This table renamed and moved to a different schema.");
        out.println("schemaz,tableq,*,-,tableq1,- # Just table renamed.");
        out.close();

        // Instantiate the filter.
        RenameFilter rf = new RenameFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setDefinitionsFile(definitionsFile);
        filterHelper.setFilter(rf);

        // True positives - renames should happen.
        assertSchemaTableChanged("schemaz", "anytable", "schemaz2", "anytable");
        assertSchemaTableChanged("schemaz", "tablew", "schemaz1", "tablew");
        assertSchemaTableChanged("schemaz", "tablee", "schemazz1", "tableee");
        assertSchemaTableChanged("schemaz", "tableq", "schemaz", "tableq1");

        // True negatives - no renames should happen.
        assertSchemaTableChanged("schemax", "anytable", "schemax", "anytable");
        assertSchemaTableChanged("schemax", "tablew", "schemax", "tablew");
        assertSchemaTableChanged("schemax", "tablee", "schemax", "tablee");
        assertSchemaTableChanged("schemax", "tableq", "schemax", "tableq");

        // All done.
        filterHelper.done();
    }

    public void testRenameSBRSchema() throws IOException, ReplicatorException,
            InterruptedException
    {
        // Prepare rename definitions file.
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,*,*,schemaz2,-,- # Schema renamed for all tables. Least priority.");
        out.println("schemaz,tablew,*,schemaz1,-,- # This table moved to a different schema.");
        out.println("schemaz,tablee,*,schemazz1,tableee,- # This table renamed and moved to a different schema.");
        out.println("schemaz,tableq,*,-,tableq1,- # Just table renamed.");
        out.close();

        // Instantiate the filter.
        RenameFilter rf = new RenameFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setDefinitionsFile(definitionsFile);
        filterHelper.setFilter(rf);

        // True positives - renames should happen.
        assertSBRSchemaChanged("schemaz", "schemaz2");

        // True negatives - no renames should happen.
        assertSBRSchemaChanged("schemaa", "schemaa");
        assertSBRSchemaChanged(null, null);

        // All done.
        filterHelper.done();
    }

    /**
     * Verify tables are renamed as expected.
     */
    public void testRenameTable() throws IOException, ReplicatorException,
            InterruptedException
    {
        // Prepare rename definitions file.
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,*,-,tableqq,-");
        out.println("*,tablew,*,-,tableww,-");
        out.println("schemax,*,*,schemaxx,-,- # Schema change - table shouldn't be renamed.");
        out.println("schemax,tablee,cola,-,-,colaa # Column change - table shouldn't be renamed.");
        out.close();

        // Instantiate the filter.
        RenameFilter rf = new RenameFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setDefinitionsFile(definitionsFile);
        filterHelper.setFilter(rf);

        // Check renaming happens correctly.
        assertSchemaTableChanged("schemaz", "tableq", "schemaz", "tableqq");
        assertSchemaTableChanged("schemaz", "tablew", "schemaz", "tableww");
        assertSchemaTableChanged("schemax", "tablew", "schemaxx", "tableww");
        assertSchemaTableChanged("schemax", "anytable", "schemaxx", "anytable");
        assertSchemaTableChanged("anyschema", "tablew", "anyschema", "tableww");

        // True negatives - no renames should happen.
        assertSchemaTableChanged("schemaz", "tablee", "schemaz", "tablee");
        assertSchemaTableChanged("schemac", "tablee", "schemac", "tablee");
    }

    /**
     * Verify columns are renamed as expected.
     */
    public void testRenameColumn() throws IOException, ReplicatorException,
            InterruptedException
    {
        // Prepare rename definitions file.
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,cola,-,-,colaa # Only rename column.");
        out.println("schemaz,tablew,cols,-,-,colss # Rename column and table below:");
        out.println("schemaz,tablew,*,-,tableww,- # Only rename column.");
        out.println("schemax,tablee,cold,-,-,coldd # Rename column and move to other schema below:");
        out.println("schemax,tablee,*,schemaxx,-,- # Rename column and move to other schema below:");
        out.println("schemaz,*,colf,-,-,colff # Rename column across all tables of this schema.");
        out.println("*,*,colg,-,-,colgg # Rename column across all tables in all schemas.");
        out.close();

        // Instantiate the filter.
        RenameFilter rf = new RenameFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setDefinitionsFile(definitionsFile);
        filterHelper.setFilter(rf);

        // Expected to be renamed.
        assertColumnChanged("schemaz", "tableq", new String[]{"cola", "id",
                "name", "color"}, "schemaz", "tableq", new String[]{"colaa",
                "id", "name", "color"});
        assertColumnChanged("schemaz", "tablew", new String[]{"id", "name",
                "cols", "color"}, "schemaz", "tableww", new String[]{"id",
                "name", "colss", "color"});
        assertColumnChanged("schemax", "tablee", new String[]{"id", "name",
                "color", "cold"}, "schemaxx", "tablee", new String[]{"id",
                "name", "color", "coldd"});
        assertColumnChanged("schemax", "tablee", new String[]{"id", "name",
                "color", "cold"}, "schemaxx", "tablee", new String[]{"id",
                "name", "color", "coldd"});
        assertColumnChanged("anyschema", "anytable", new String[]{"id", "name",
                "color", "colg"}, "anyschema", "anytable", new String[]{"id",
                "name", "color", "colgg"});
        assertColumnChanged("anyschema", "tablee", new String[]{"id", "name",
                "color", "colg"}, "anyschema", "tablee", new String[]{"id",
                "name", "color", "colgg"});

        // Expected to be left as is.
        assertColumnChanged("schemaz", "tableq", new String[]{"cols", "id",
                "name", "color"}, "schemaz", "tableq", new String[]{"cols",
                "id", "name", "color"});
        assertColumnChanged("schemaz", "tablew", new String[]{"id", "name",
                "cola", "color"}, "schemaz", "tableww", new String[]{"id",
                "name", "cola", "color"});
        assertColumnChanged("schemax", "tablee", new String[]{"id", "name",
                "color", "cols"}, "schemaxx", "tablee", new String[]{"id",
                "name", "color", "cols"});
        assertColumnChanged("schemax", "tablee", new String[]{"id", "name",
                "color", "colh"}, "schemaxx", "tablee", new String[]{"id",
                "name", "color", "colh"});
        assertColumnChanged("anyschema", "anytable", new String[]{"id", "name",
                "color", "colj"}, "anyschema", "anytable", new String[]{"id",
                "name", "color", "colj"});
        assertColumnChanged("anyschema", "tablee", new String[]{"id", "name",
                "color", "colk"}, "anyschema", "tablee", new String[]{"id",
                "name", "color", "colk"});
    }

    public void testRenameComposite() throws ReplicatorException,
            InterruptedException, IOException
    {
        // Prepare rename definitions file.
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("scheman,*,*,scheman2,-,- # Casual tables moved out.");
        out.println("scheman,tabley,*,-,tabley2,- # But one table is left.");
        out.println("scheman,tabley,colj,-,-,colj2");
        out.println("scheman,tabley,colk,-,-,colk2");
        out.println("scheman,tableu,*,scheman3,tableu2,- # Another special table.");
        out.println("schemam,*,*,schemam2,-,-");
        out.println("schemam,tableu,*,schemam2,tableu2,-");
        out.println("schemam,tableu,colj,-,-,colj2");
        out.close();

        // Instantiate the filter.
        RenameFilter rf = new RenameFilter();
        rf.setTungstenSchema("tungsten_foo");
        rf.setDefinitionsFile(definitionsFile);
        filterHelper.setFilter(rf);

        // Expected to be renamed.
        assertColumnChanged("scheman", "casualtable", new String[]{"id"},
                "scheman2", "casualtable", new String[]{"id"});
        assertColumnChanged("scheman", "tabley", new String[]{"id", "colk",
                "colj"}, "scheman", "tabley2", new String[]{"id", "colk2",
                "colj2"});
        assertColumnChanged("scheman", "tableu", new String[]{"id", "temp"},
                "scheman3", "tableu2", new String[]{"id", "temp"});
        assertColumnChanged("schemam", "tableu", new String[]{"id", "colk",
                "colj"}, "schemam2", "tableu2", new String[]{"id", "colk",
                "colj2"});
        assertColumnChanged("schemam", "anytable", new String[]{"id", "colk",
                "colj"}, "schemam2", "anytable", new String[]{"id", "colk",
                "colj"});
    }

    private void assertSBRSchemaChanged(String originalSchema,
            String expectedSchema) throws ReplicatorException,
            InterruptedException
    {
        // Create StatementData event.
        ReplDBMSEvent e = eventHelper.eventFromStatement(1, originalSchema,
                "TRUNCATE TABLE test");

        // Transform.
        ReplDBMSEvent e2 = filterHelper.filter(e);

        // Confirm results are correct.
        StatementData sdata = (StatementData) e2.getDBMSEvent().getData()
                .get(0);

        assertEquals("Wrong schema name after filtering", expectedSchema,
                sdata.getDefaultSchema());
    }

    /**
     * @param expectedColumns if null, don't compare column changes.
     */
    private void assertColumnChanged(String originalSchema,
            String originalTable, String originalColumns[],
            String expectedSchema, String expectedTable,
            String expectedColumns[]) throws ReplicatorException,
            InterruptedException
    {
        // Create a row change event.
        Long values[] = new Long[originalColumns.length];
        for (int c = 0; c < originalColumns.length; c++)
        {
            values[c] = new Long(333);
        }
        ReplDBMSEvent e = eventHelper.eventFromRowInsert(1, originalSchema,
                originalTable, originalColumns, values, 0, true);

        // Transform.
        ReplDBMSEvent e2 = filterHelper.filter(e);

        // Confirm results are correct.
        RowChangeData row = (RowChangeData) e2.getDBMSEvent().getData().get(0);
        OneRowChange orc = row.getRowChanges().get(0);

        assertEquals("Wrong schema name after filtering", expectedSchema,
                orc.getSchemaName());
        assertEquals("Wrong table name after filtering", expectedTable,
                orc.getTableName());

        if (expectedColumns != null)
        {
            for (int c = 0; c < originalColumns.length; c++)
            {
                assertEquals("Wrong column name after filtering",
                        expectedColumns[c], orc.getColumnSpec().get(c)
                                .getName());
            }
        }
    }

    private void assertSchemaTableChanged(String originalSchema,
            String originalTable, String expectedSchema, String expectedTable)
            throws ReplicatorException, InterruptedException
    {
        // Create a row change event.
        String columns[] = {"id"};
        assertColumnChanged(originalSchema, originalTable, columns,
                expectedSchema, expectedTable, null);
    }
}