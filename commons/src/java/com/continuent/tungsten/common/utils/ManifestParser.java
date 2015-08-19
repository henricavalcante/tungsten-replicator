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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Parses .manifest.json file and makes properties easily accessible.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class ManifestParser
{
    private static Logger       logger           = Logger.getLogger(ManifestParser.class);
    private static final String manifestFileName = ".manifest.json";

    private JSONParser          parser           = null;
    private JSONObject          rootJSONObject   = null;
    private String              manifestPath     = null;

    /**
     * Prepare JSONParser and find out the home folder. Call parse() afterwards.
     * 
     * @see #parse()
     */
    public ManifestParser()
    {
        parser = new JSONParser();
        findHome();
    }

    /**
     * Read and parse manifest file. No exceptions are thrown upon failure
     * intentionally.
     * 
     * @return true, if parsed successfully, otherwise false.
     * @see #isFileParsed()
     * @see #getReleaseWithBuildNumber()
     */
    public boolean parse()
    {
        try
        {
            String jsonManifest = readManifestFile(manifestPath);
            Object obj = parser.parse(jsonManifest);
            rootJSONObject = (JSONObject) obj;
            return true;
        }
        catch (IOException ioe)
        {
            logger.error("Error reading " + manifestPath + ": "
                    + ioe.toString());
            return false;
        }
        catch (ParseException pe)
        {
            logger.error("Error parsing JSON from " + manifestPath + ": "
                    + pe.getPosition());
            logger.error(pe);
            return false;
        }
    }

    /**
     * Find the home folder of installation.
     */
    private void findHome()
    {
        String home = System.getProperty("replicator.home.dir");
        if (home == null)
            home = System.getProperty("manager.home");
        if (home == null)
            home = System.getProperty("cluster.home");
        if (home == null)
            home = System.getProperty("user.dir");
        if (home != null) // Home determined.
            manifestPath = home + File.separator + ".." + File.separator
                    + manifestFileName;
        else
            manifestPath = manifestFileName; // Not found - use current folder.
    }

    /**
     * Path to the manifest file.
     */
    public String getManifestPath()
    {
        return manifestPath;
    }

    private String returnStringOrNull(Object val)
    {
        if (val == null)
            return null;
        else
            return (String) val;
    }

    private Number returnNumberOrNull(Object val)
    {
        if (val == null)
            return null;
        else
            return (Number) val;
    }

    private Object getFromJSONObject(JSONObject object, Object key)
    {
        if (object != null)
            return object.get(key);
        else
            return null;
    }

    private JSONObject getHudson()
    {
        return (JSONObject) getFromJSONObject(rootJSONObject, "hudson");
    }

    private JSONObject getVersion()
    {
        return (JSONObject) getFromJSONObject(rootJSONObject, "version");
    }

    /**
     * Returns product name from the manifest.
     */
    public String getProduct()
    {
        Object val = getFromJSONObject(rootJSONObject, "product");
        return returnStringOrNull(val);
    }

    /**
     * Returns major (first) number of the version.
     */
    public Number getVersionMajor()
    {
        Object val = getFromJSONObject(getVersion(), "major");
        return returnNumberOrNull(val);
    }

    /**
     * Returns minor (middle) number of the version.
     */
    public Number getVersionMinor()
    {
        Object val = getFromJSONObject(getVersion(), "minor");
        return returnNumberOrNull(val);
    }

    /**
     * Returns revision (last) number of the version.
     */
    public Number getVersionRevision()
    {
        Object val = getFromJSONObject(getVersion(), "revision");
        return returnNumberOrNull(val);
    }

    /**
     * Returns concatenated version in format: MAJOR.MINOR.REVISION
     * 
     * @return a) Version in MAJOR.MINOR.REVISION format;<br/>
     *         b) null if non of the major, minor or revision numbers are
     *         defined.<br/>
     *         c) If only one or two numbers are missing, returns question mark
     *         (?) in place of it. Eg.: 1.3.?
     */
    public String getVersionFull()
    {
        Number major = getVersionMajor();
        Number minor = getVersionMinor();
        Number revision = getVersionRevision();
        if (major == null && minor == null && revision == null)
            return null;
        else
        {
            StringBuilder builder = new StringBuilder();
            if (major != null)
                builder.append(major);
            else
                builder.append("?");
            builder.append(".");
            if (minor != null)
                builder.append(minor);
            else
                builder.append("?");
            builder.append(".");
            if (revision != null)
                builder.append(revision);
            else
                builder.append("?");
            return builder.toString();
        }
    }

    public String getBuildDate()
    {
        Object val = getFromJSONObject(rootJSONObject, "date");
        return returnStringOrNull(val);
    }

    /**
     * Returns Hudson's build number from the manifest.
     */
    public Number getHudsonBuildNumber()
    {
        Object val = getFromJSONObject(getHudson(), "buildNumber");
        return returnNumberOrNull(val);
    }

    /**
     * Returns Hudson's used SVN revision from the manifest.
     */
    public String getHudsonSVNRevision()
    {
        Object val = getFromJSONObject(getHudson(), "SVNRevision");
        return returnStringOrNull(val);
    }

    /**
     * Returns a representative string of release name, version and build
     * number. This is a safe method in a sense it will never return a null.
     * 
     * @return Release and build number or an error message that manifest file
     *         couldn't be read.
     */
    public String getReleaseWithBuildNumber()
    {
        String product = getProduct();
        String version = getVersionFull();
        Number build = getHudsonBuildNumber();
        StringBuilder builder = new StringBuilder();
        if (product != null)
            builder.append(product);
        if (version != null)
        {
            builder.append(" ");
            builder.append(version);
        }
        if (build != null)
        {
            builder.append(" build ");
            builder.append(build);
        }
        if (product == null && version == null && build == null)
        {
            builder.append("Unable to determine release name - does ");
            builder.append(manifestPath);
            builder.append(" exist?");
        }
        return builder.toString();
    }

    /**
     * Convenience method for a single line call to get a representative string
     * of release name, version and build number.
     */
    public static String parseReleaseWithBuildNumber()
    {
        ManifestParser manifest = new ManifestParser();
        manifest.parse();
        return manifest.getReleaseWithBuildNumber();
    }

    /**
     * Convenience method for a single line call to log a representative string
     * of release name, version and build number.
     * 
     * @param logger Logger to log into.
     */
    public static void logReleaseWithBuildNumber(Logger logger)
    {
        logger.info(ManifestParser.parseReleaseWithBuildNumber());
    }

    /**
     * @return True, if manifest was successfully parsed.
     */
    public boolean isFileParsed()
    {
        if (rootJSONObject != null)
            return true;
        else
            return false;
    }

    /**
     * Reads the whole manifest file into a String.
     * 
     * @throws Exception if the file cannot be found.
     */
    private String readManifestFile(String file) throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader(file));
        try
        {
            StringBuilder builder = new StringBuilder();
            String line = null;
            String newLine = System.getProperty("line.separator");
            while ((line = br.readLine()) != null)
            {
                builder.append(line);
                builder.append(newLine);
            }
            return builder.toString();
        }
        finally
        {
            try
            {
                if (br != null)
                    br.close();
            }
            catch (IOException ex)
            {
                logger.warn("Unable to close file " + file + ": "
                        + ex.toString());
            }
        }
    }
}
