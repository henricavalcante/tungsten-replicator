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
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.log4j.Logger;
import org.junit.After;

import com.continuent.tungsten.replicator.ReplicatorException;

import junit.framework.TestCase;

/**
 * This class tests RenameDefinitions.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */
public class RenameDefinitionsTest extends TestCase
{
    private static Logger logger          = Logger.getLogger(RenameDefinitionsTest.class);

    private final String  definitionsFile = "RenameDefinitionsTest.csv";

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

    private void testInvalidFormat(String row, String message)
            throws IOException
    {
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("# Incorrect format.");
        out.println();
        out.println(row);
        out.close();

        try
        {
            RenameDefinitions renameDefinitions = new RenameDefinitions(
                    definitionsFile);
            renameDefinitions.parseFile();

            fail(message);
        }
        catch (ReplicatorException e)
        {
            // OK - it should have threw an exception because of invalid format.
            logger.info(e);
        }
    }

    public void testNonsenseDefinitions1() throws IOException
    {
        testInvalidFormat("*,*,*,-,-,- # This does nothing.",
                "Exception not thrown on a line that does nothing");
    }

    public void testNonsenseDefinitions2() throws IOException
    {
        testInvalidFormat("schemaz,*,*,-,-,- # This does nothing.",
                "Exception not thrown on a line that does nothing");
    }

    public void testNonsenseDefinitions3() throws IOException
    {
        testInvalidFormat("*,*,*,-,-,col # This makes no sense.",
                "Exception not thrown on a line which makes no sense");
    }

    public void testNonsenseDefinition4() throws IOException
    {
        testInvalidFormat(
                "schemax,tablee,colg,schemax,tablee,- # Does nothing.",
                "Exception not thrown on a line which does nothing");
    }

    public void testUnsupportedDefinition1() throws IOException
    {
        testInvalidFormat(
                "schemax,tabler,cols,-,tablerr,cols",
                "Exception not thrown on a request to move column to a different table, which is unsupported");
    }

    public void testUnsupportedDefinition2() throws IOException
    {
        testInvalidFormat(
                "schemax,tabler,cols,schemaxx,tablerr,cols",
                "Exception not thrown on a request to move column to a different schema & table, which is unsupported");
    }

    public void testUnsupportedDefinition3() throws IOException
    {
        testInvalidFormat(
                "*,tableq,cols,-,tableqq,colss",
                "Exception not thrown on a request to move column to a different table and rename, which is unsupported");
    }

    public void testNotEnoughColumns() throws IOException
    {
        testInvalidFormat("schemaz,tableq,cola,-,- # Not enough columns.",
                "Exception not thrown on not enough columns");
    }

    public void testTooManyColumns() throws IOException
    {
        testInvalidFormat("schemaz,tableq,cola,-,talbeqq,-,wrong",
                "Exception not thrown on too many columns");
    }

    public void testMisplacedAsterisk() throws IOException
    {
        testInvalidFormat("schemaz,tableq,cola,-,*,colaa",
                "Exception not thrown on * in the wrong place");
    }

    public void testMisplacedMinus() throws IOException
    {
        testInvalidFormat("schemaz,-,cola,-,-,colaa",
                "Exception not thrown on - in the wrong place");
    }

    public void testUnsupportedAsterisks() throws IOException
    {
        testInvalidFormat("schemaz,table*,cola,-,-,colaa",
                "Exception not thrown on * tried to be used as part-string matcher");
    }

    public void testMissingFile() throws IOException, ReplicatorException
    {
        try
        {
            RenameDefinitions renameDefinitions = new RenameDefinitions(
                    "nosuchfile.csv");
            renameDefinitions.parseFile();

            fail("File doesn't exist but no exception thrown");
        }
        catch (FileNotFoundException e)
        {
            // OK - expected in this case.
        }
    }

    public void testDuplicateThrowsError() throws IOException,
            ReplicatorException
    {
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schema1,ta,*,-,taa,-");
        out.println("schema2,tb,*,-,tbb,-");
        out.println("schema1,ta,*,schema2,taa,- # Matching part is duplicated.");
        out.close();

        try
        {
            RenameDefinitions renameDefinitions = new RenameDefinitions(
                    definitionsFile);
            renameDefinitions.parseFile();

            fail("Duplicate not caught");
        }
        catch (ReplicatorException e)
        {
            // OK - it should have threw an exception because of invalid format.
            logger.info(e);
        }

    }

    public void testShouldRenameColumnSpecific() throws IOException,
            ReplicatorException
    {
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("# By the way, testing comment with commas, commas.");
        out.println("schemaz,tableq,cola,-,-,colaa");
        out.println("schemaz,tablew,cols,-,-,colss");
        out.println("schemaz,tablee,*,-,tableee,-");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemaz", "tableq"));
        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemaz", "tablew"));
        assertFalse("Not requested to rename, but it's reported",
                renameDefinitions.shouldRenameColumn("s", "t"));
        assertFalse("Not requested to rename, but it's reported",
                renameDefinitions.shouldRenameColumn("schemaz", "tablee"));
    }

    public void testShouldRenameColumnAllTables() throws IOException,
            ReplicatorException
    {
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,*,-,tableqq,- # Dummy.");
        out.println("schemaz,*,cols,-,-,colss");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemaz", "tableq"));
        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemaz", "t"));
        assertFalse("Not requested to rename, but it's reported",
                renameDefinitions.shouldRenameColumn("s", "t"));
    }

    public void testShouldRenameColumnAllSchemas() throws IOException,
            ReplicatorException
    {
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,*,-,tableqq,- # Dummy.");
        out.println("*,tablew,cols,-,-,colss");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemaz", "tablew"));
        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemax", "tablew"));
        assertFalse("Not requested to rename, but it's reported",
                renameDefinitions.shouldRenameColumn("schemax", "tableq"));
    }

    public void testShouldRenameColumnEverywhere() throws IOException,
            ReplicatorException
    {
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,*,-,tableqq,- # Noise.");
        out.println("*,*,cols,-,-,colss");
        out.println("schemaz,tablew,cola,-,-,colaa # Asterisk above should still work.");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemaz", "tablew"));
        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemax", "tablew"));
        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemax", "tableq"));
    }

    public void testRenameColumns() throws IOException, ReplicatorException
    {
        String errorMismatch = "Rename definition doesn't match rename result";
        String errorRenamed = "No rename should have been made";

        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,*,-,tableqq,- # Dummy.");
        out.println(); // Noise.
        out.println("*,*,cola,-,-,colaa");
        out.println("# Noise.");
        out.println("schemaz,tablew,cols,-,-,colss # Asterisk above should still work.");
        out.println("schemaz,*,cold,-,-,coldd");
        out.println("*,tableq,colf,-,-,colff # Noisy comment.");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertEquals(errorMismatch, "colaa",
                renameDefinitions.getNewColumnName("anyschema", "anytable",
                        "cola"));
        assertEquals(errorMismatch, "colaa",
                renameDefinitions.getNewColumnName("schemaz", "anytable",
                        "cola"));
        assertEquals(errorMismatch, "colaa",
                renameDefinitions.getNewColumnName("schemaz", "tableq", "cola"));
        assertNull(errorRenamed, renameDefinitions.getNewColumnName(
                "anyschema", "anytable", "cols"));
        assertEquals(errorMismatch, "colss",
                renameDefinitions.getNewColumnName("schemaz", "tablew", "cols"));
        assertNull(errorRenamed, renameDefinitions.getNewColumnName("schemaz",
                "tablew", "leaveit"));
        assertEquals(errorMismatch, "coldd",
                renameDefinitions.getNewColumnName("schemaz", "anytable",
                        "cold"));
        assertNull(errorRenamed, renameDefinitions.getNewColumnName(
                "anyschema", "anytable", "colf"));
        assertEquals(errorMismatch, "colff",
                renameDefinitions.getNewColumnName("anyschema", "tableq",
                        "colf"));
    }

    /**
     * Testing that in case of multiple rename definitions the following order
     * of preference is maintained:<br/>
     * 1. schema.table<br/>
     * 2. schema.*<br/>
     * 3. *.table<br/>
     * 4. *.*<br/>
     */
    public void testRenameColumnsOrder1() throws IOException,
            ReplicatorException
    {
        String error = "Wrong rename definition picked (order of preference mismatch)";

        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,cola,-,-,cola1");
        out.println("schemaz,*,cola,-,-,cola2");
        out.println("*,tableq,cola,-,-,cola3");
        out.println("*,*,cola,-,-,cola4");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertEquals(error, "cola1",
                renameDefinitions.getNewColumnName("schemaz", "tableq", "cola"));
        assertEquals(error, "cola2", renameDefinitions.getNewColumnName(
                "schemaz", "othertable", "cola"));
        assertEquals(error, "cola3", renameDefinitions.getNewColumnName(
                "otherschema", "tableq", "cola"));
        assertEquals(error, "cola4", renameDefinitions.getNewColumnName(
                "otherschema", "othertable", "cola"));
    }

    /**
     * @see #testRenameColumnsOrder1()
     */
    public void testRenameColumnsOrder2() throws IOException,
            ReplicatorException
    {
        String error = "Wrong rename definition picked (order of preference mismatch)";

        // Changing line order compared to the other similar test.
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("*,*,cola,-,-,cola4");
        out.println("*,tableq,cola,-,-,cola3");
        out.println("schemaz,*,cola,-,-,cola2");
        out.println("schemaz,tableq,cola,-,-,cola1");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertEquals(error, "cola1",
                renameDefinitions.getNewColumnName("schemaz", "tableq", "cola"));
        assertEquals(error, "cola2", renameDefinitions.getNewColumnName(
                "schemaz", "othertable", "cola"));
        assertEquals(error, "cola3", renameDefinitions.getNewColumnName(
                "otherschema", "tableq", "cola"));
        assertEquals(error, "cola4", renameDefinitions.getNewColumnName(
                "otherschema", "othertable", "cola"));
    }

    public void testRenameTable() throws IOException, ReplicatorException
    {
        String error = "Rename definition doesn't match rename result";

        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,*,-,tableqq,-");
        out.println("*,tablew,*,-,tableww,-");
        out.println("schemax,*,*,schemaxx,-,- # Schema change - table shouldn't be renamed.");
        out.println("schemax,tablee,cola,-,-,colaa # Column change - table shouldn't be renamed.");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertEquals(error, "tableqq",
                renameDefinitions.getNewTableName("schemaz", "tableq"));
        assertEquals(error, "tableww",
                renameDefinitions.getNewTableName("schemaz", "tablew"));
        assertEquals(error, "tableww",
                renameDefinitions.getNewTableName("anyschema", "tablew"));
        assertNull("Table shouldn't have been renamed",
                renameDefinitions.getNewTableName("schemaz", "tablee"));
        assertNull("Schema change, but table renamed",
                renameDefinitions.getNewTableName("schemax", "anytable"));
        assertNull("Column change, but table renamed",
                renameDefinitions.getNewTableName("schemax", "tablee"));
    }

    public void testRenameTableOrder1() throws IOException, ReplicatorException
    {
        String error = "Rename definition doesn't match rename result";

        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,*,-,tableq1,-");
        out.println("*,tableq,*,-,tableq2,-");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertEquals(error, "tableq1",
                renameDefinitions.getNewTableName("schemaz", "tableq"));
        assertEquals(error, "tableq2",
                renameDefinitions.getNewTableName("anyschema", "tableq"));
        assertNull("Table shouldn't have been renamed",
                renameDefinitions.getNewTableName("schemaz", "tablew"));
    }

    /**
     * @see #testRenameTableOrder1()
     */
    public void testRenameTableOrder2() throws IOException, ReplicatorException
    {
        String error = "Rename definition doesn't match rename result";

        // Changing line order compared to the other similar test.
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("*,tableq,*,-,tableq2,-");
        out.println("schemaz,tableq,*,-,tableq1,-");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertEquals(error, "tableq1",
                renameDefinitions.getNewTableName("schemaz", "tableq"));
        assertEquals(error, "tableq2",
                renameDefinitions.getNewTableName("anyschema", "tableq"));
        assertNull("Table shouldn't have been renamed",
                renameDefinitions.getNewTableName("schemaz", "tablew"));
    }

    public void testRenameSchema() throws IOException, ReplicatorException
    {
        String error = "Rename definition doesn't match rename result";

        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,*,*,schemaz2,-,- # Schema renamed for all tables. Least priority.");
        out.println("schemaz,tablew,*,schemaz1,-,- # This table moved to a different schema.");
        out.println("schemaz,tablee,*,schemazz1,tableee,- # This table renamed and moved to a different schema.");
        out.println("schemaz,tableq,*,-,tableq1,- # Just table renamed.");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertEquals(error, "schemaz2",
                renameDefinitions.getNewSchemaName("schemaz", "anytable"));
        assertEquals(error, "schemaz1",
                renameDefinitions.getNewSchemaName("schemaz", "tablew"));

        assertEquals(error, "schemazz1",
                renameDefinitions.getNewSchemaName("schemaz", "tablee"));
        assertEquals(error, "tableee",
                renameDefinitions.getNewTableName("schemaz", "tablee"));

        assertNull("Schema should have not been changed",
                renameDefinitions.getNewSchemaName("schemaz", "tableq"));
        assertEquals(error, "tableq1",
                renameDefinitions.getNewTableName("schemaz", "tableq"));
    }

    /**
     * Tests in-line comments with following commas.
     */
    public void testCommentWithComma() throws IOException, ReplicatorException
    {
        // This should use commons CsvWriter but a simple println also works. 
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,cola,-,-,colaa # Finally, comment with a comma should work.");
        out.close();

        try
        {
            RenameDefinitions renameDefinitions = new RenameDefinitions(
                    definitionsFile);
            renameDefinitions.parseFile();
        }
        catch (ReplicatorException e)
        {
            // Ignoring this exception only in order not to break the builds
            // until new CSV reader is chosen.
            logger.error(e);
        }
    }

    public void testRenameSchemaTableColumn() throws IOException,
            ReplicatorException
    {
        String error = "Rename definition doesn't match rename result";

        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,tableq,cola,-,-,colaa # Rename one column in this table.");
        out.println("schemaz,tableq,colb,-,-,colbb # Rename another column in this table.");
        out.println("schemaz,tableq,*,schemazz,tableqq,- # Rename the whole table and move it to another schema.");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertTrue("There are columns to be renamed, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemaz", "tableq"));
        assertEquals(error, "schemazz",
                renameDefinitions.getNewSchemaName("schemaz", "tableq"));
        assertEquals(error, "tableqq",
                renameDefinitions.getNewTableName("schemaz", "tableq"));
        assertEquals(error, "colaa",
                renameDefinitions.getNewColumnName("schemaz", "tableq", "cola"));
        assertEquals(error, "colbb",
                renameDefinitions.getNewColumnName("schemaz", "tableq", "colb"));
    }

    public void testDifferentCases() throws IOException, ReplicatorException
    {
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        // These lines should be ignored:
        out.println("# oSchema,oTable,oCol,nSchema,nTable,nCol");
        out.println("# * - all occurances");
        out.println("# - - left as original");
        out.println();
        // Essential lines:
        out.println("schemaz,tableq,cola,-,-,colaa # Column of this particular table renamed.");
        out.println("*,tablew,colb,-,-,colbb # Column renamed in each table of this name across all schemas.");
        out.println("*,*,colc,-,-,colcc # Column renamed accross all tables.");
        out.println("schemax,*,cold,-,-,coldd # Column renamed across all tables in this schema.");
        out.println("schemac,*,*,schemacc,-,- # All tables from this schema are in a different one.");
        out.println("schemav,tablee,*,-,tableee,- # Table renamed in this particular schema.");
        out.println("schemab,tabler,*,schemabb,-,- # Table moved to other schema.");
        out.println("scheman,tablet,*,schemann,tablett,- # Table moved to other schema and renamed.");
        // Messy line should be cleaned up:
        out.println("*,tabley,*,-,tableyy,-     # Tables with this name renamed across all schemas.");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("schemaz", "tableq"));
        assertTrue("Requested to rename column, but it's not reported",
                renameDefinitions.shouldRenameColumn("any", "many"));
    }

    public void testCaseSensitivity() throws IOException, ReplicatorException
    {
        String error1 = "Rename definition doesn't match rename result";
        String error2 = "Rename not case sensitive any more";

        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("schemaz,*,*,schemazz,-,-");
        out.println("schemax,tableq,*,-,tableqq,-");
        out.println("schemac,tablew,cola,-,-,colaa");
        out.close();

        RenameDefinitions renameDefinitions = new RenameDefinitions(
                definitionsFile);
        renameDefinitions.parseFile();

        // Expected to be renamed.
        assertEquals(error1, "schemazz",
                renameDefinitions.getNewSchemaName("schemaz", "anytable"));
        assertEquals(error1, "tableqq",
                renameDefinitions.getNewTableName("schemax", "tableq"));
        assertEquals(error1, "colaa",
                renameDefinitions.getNewColumnName("schemac", "tablew", "cola"));

        // Expected to be left as is.
        assertNull(error2,
                renameDefinitions.getNewSchemaName("schemaZ", "anytable"));
        assertNull(error2,
                renameDefinitions.getNewTableName("schemax", "tableQq"));
        assertNull(error2,
                renameDefinitions.getNewColumnName("schemac", "tablew", "colA"));
    }
}
