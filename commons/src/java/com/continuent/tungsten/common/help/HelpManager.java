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

package com.continuent.tungsten.common.help;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * This class fetches and presents online help text. Help is stored in text
 * files that are located in a help directory. Help operates as a separate
 * subsystem with a factory method to fetch manager instances. Help output
 * defaults to System.out.
 */
public class HelpManager
{
    // Location of help files.
    private static File helpDir;

    // Default name if no topic is specified.
    private static String DEFAULT = "default";

    // Message to print if topic is not found.
    private static String NOT_FOUND = "not_found.hlp";

    // File type for help files.
    private static String FILE_TYPE = ".hlp";

    private PrintWriter out = new PrintWriter(System.out);

    // Private constructor.
    private HelpManager()
    {
    }

    /**
     * Initialize help. This must be called before fetching a help manager.
     * 
     * @param helpDir Location of help files.
     * @throws HelpException Thrown if help directory is invalid
     */
    public static void initialize(File helpDir) throws HelpException
    {
        if (helpDir.isDirectory() && helpDir.exists())
            HelpManager.helpDir = helpDir;
        else
            throw new HelpException("Help directory is missing or invalid: "
                    + helpDir.getAbsolutePath(), null);
    }

    /**
     * Return a help manager instance.
     */
    public static HelpManager getInstance()
    {
        return new HelpManager();
    }

    /**
     * Set the print output writer.
     */
    public void setWriter(Writer writer)
    {
        out = new PrintWriter(writer);
    }

    /**
     * Display help for a topic. The name will be lower-cased and extended with
     * the help file suffix, then displayed.
     * 
     * @param name Help topic name or null to get the default value
     * @return True if help file was successfully displayed, otherwise false.
     */
    public boolean displayTopic(String name)
    {
        // Construct the help name.
        String topic = null;
        if (name == null)
            topic = DEFAULT + FILE_TYPE;
        else
            topic = name.toLowerCase() + FILE_TYPE;

        // Fetch the file.
        File helpFile = new File(helpDir, topic);
        return display(helpFile);
    }

    /**
     * Display help for a topic. The topic consists of one or more strings. To
     * locate the corresponding file, we lower case the strings, connect them by
     * "_" characters, and add the help file suffix.
     * 
     * @param names An array of names used to construct the topic name
     * @return True if help file was successfully displayed, otherwise false.
     */
    public boolean displayTopicFromNames(String[] names)
    {
        // Construct the help name.
        if (names.length == 0)
            return displayTopic(DEFAULT);
        else
        {
            // Construct the help file name from the arrays.
            StringBuffer helpName = new StringBuffer();
            for (String name : names)
            {
                if (helpName.length() > 0)
                    helpName.append("_");
                helpName.append(name);
            }
            return displayTopic(helpName.toString());
        }
    }

    /**
     * Displays the requested help file if it is found. If not, we first try to
     * display the default not found message. If that is not available, we
     * display a default message.
     * 
     * @param helpFile Requested help file.
     * @return True if found and properly displayed
     */
    protected boolean display(File helpFile) throws HelpException
    {
        if (helpFile.exists())
        {
            loadAndWrite(helpFile);
            return true;
        }
        else
        {
            File notFound = new File(helpDir, NOT_FOUND);
            if (notFound.exists())
                loadAndWrite(notFound);
            else
                throw new HelpException(
                        "Topic not found, try 'help' for more information");
            return false;
        }
    }

    /**
     * Copy help file contents to the current output.
     */
    protected void loadAndWrite(File file) throws HelpException
    {
        FileReader fr = null;
        BufferedReader br = null;
        try
        {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null)
            {
                out.println(line);
            }
            out.flush();
        }
        catch (IOException e)
        {

        }
        finally
        {
            if (fr != null)
            {
                try
                {
                    fr.close();
                }
                catch (IOException e)
                {
                }
            }
            if (br != null)
                try
                {
                    br.close();
                }
                catch (IOException e)
                {
                }

        }
    }
}