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

package com.continuent.tungsten.common.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.continuent.tungsten.common.utils.ManifestParser;

/**
 * Unit test against ManifestParser.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class ManifestParserTest
{
    private static Logger       logger            = Logger.getLogger(ManifestParserTest.class);

    private static final String product           = "Tungsten Enterprise";
    private static final int    versionMajor      = 1;
    private static final int    versionMinor      = 3;
    private static final int    versionRevision   = 2;
    private static final long   hudsonBuildNumber = 999;

    /**
     * Test method for
     * {@link com.continuent.tungsten.common.utils.ManifestParser#ManifestParser()}
     * with no manifest file. .
     */
    @Test
    public void testManifestParserNoFile() throws IOException
    {
        ManifestParser manifest = new ManifestParser();

        deleteJSONManifest(manifest.getManifestPath());

        logger.info("Parsing with ManifestParser. Error expected as there is no manifest:");
        manifest.parse();
        assertFalse(
                "isFileParsed() should return false if no manifest is provided",
                manifest.isFileParsed());

        assertNull(
                "getRelease() should return null if manifest was not parsed",
                manifest.getProduct());

        assertNull(
                "getHudsonBuildNumber() should return null if manifest was not parsed",
                manifest.getHudsonBuildNumber());

        assertNotNull("getReleaseWithBuildNumber() should never return null",
                manifest.getReleaseWithBuildNumber());
        logger.info("getReleaseWithBuildNumber(): "
                + manifest.getReleaseWithBuildNumber());
    }

    /**
     * Removes .manifest.json file form the file system.
     */
    private void deleteJSONManifest(String manifestPath) throws IOException
    {
        logger.info("Deleting " + manifestPath);
        File file = new File(manifestPath);
        if (file.exists())
            if (!file.delete())
                throw new IOException("Unable to delete " + manifestPath);
    }

    /**
     * Writes a simplified .manifest.json file for testing against.
     */
    private void writeSampleJSONManifest(String manifestPath)
            throws IOException
    {
        logger.info("Writing sample " + manifestPath);
        FileWriter outFile = new FileWriter(manifestPath);
        PrintWriter out = new PrintWriter(outFile);
        out.println("{");
        out.println("  \"date\": \"Mon May  9 18:53:41 CEST 2011\",");
        out.println("  \"product\": \"" + product + "\",");
        out.println("  \"version\":");
        out.println("  {");
        out.println("    \"major\": " + versionMajor + ",");
        out.println("    \"minor\": " + versionMinor + ",");
        out.println("    \"revision\": " + versionRevision);
        out.println("  },");
        out.println("  \"userAccount\": \"linasvirbalas\",");
        out.println("  \"host\": \"hostalpha\",");
        out.println("  \"hudson\":");
        out.println("  {");
        out.println("    \"buildNumber\": " + hudsonBuildNumber + ",");
        out.println("    \"buildId\": 306");
        out.println("  },");
        out.println("  \"SVN\":");
        out.println("  {");
        out.println("    \"commons\":");
        out.println("    {");
        out.println("      \"URL\": \"https://tungsten.svn.sourceforge.net/svnroot/tungsten/branches/tungsten-1.3/commons\",");
        out.println("      \"revision\": 2939");
        out.println("    }");
        out.println("  }");
        out.println("}");
        out.close();
    }

    /**
     * Test method for
     * {@link com.continuent.tungsten.common.utils.ManifestParser#ManifestParser()}
     * with manifest file. Ensures that basic functionality works.
     */
    @Test
    public void testManifestParserWithFile() throws IOException
    {
        ManifestParser manifest = new ManifestParser();

        writeSampleJSONManifest(manifest.getManifestPath());
        manifest.parse();

        assertTrue(
                "isFileParsed() should return true when manifest is provided",
                manifest.isFileParsed());

        assertNotNull(
                "getRelease() should return a String as it is defined in the manifest",
                manifest.getProduct());
        assertEquals(product, manifest.getProduct());
        logger.info("getRelease(): " + manifest.getProduct());

        assertNull(
                "getHudsonJobName() should return null when hudson.jobName is not defined in the manifest",
                manifest.getHudsonSVNRevision());

        assertNotNull(
                "getHudsonBuildNumber() should return a valid build number as it is defined in the manifest",
                manifest.getHudsonBuildNumber());
        assertEquals(hudsonBuildNumber, manifest.getHudsonBuildNumber());
        logger.info("getHudsonBuildNumber(): "
                + manifest.getHudsonBuildNumber());

        assertNotNull("getReleaseWithBuildNumber() should never return null",
                manifest.getReleaseWithBuildNumber());
        logger.info("getReleaseWithBuildNumber(): "
                + manifest.getReleaseWithBuildNumber());

        deleteJSONManifest(manifest.getManifestPath());
    }
}
