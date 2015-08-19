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
 * Initial developer(s): Joe Daly
 * Contributor(s):
 */

package com.continuent.tungsten.common.commands;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Methods to help loading manipulating a propertlies file
 * 
 * @author <a href="mailto:joe.daly@continuent.com">Joe Daly</a>
 * @version 1.0
 */
public class PropertyCommands
{

    static Logger logger = Logger.getLogger(PropertyCommands.class);

    /**
     * Loads a property file that in name=value. This is used rather then using
     * the Properties.load(InputStream inStream) to allow paths on windows to be
     * loaded without having to add \\ for paths.
     * 
     * @param propertyFile the property file to read in.
     * @return a listing of properties for the given property file
     * @throws Exception if the property file can not be loaded
     */
    public static Properties readPropertyFile(String propertyFile)
            throws Exception
    {
        Properties properties = null;
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(propertyFile));

            properties = new Properties();
            String str;
            while ((str = br.readLine()) != null)
            {
                str = str.trim();
                if ((!str.startsWith("#")) && (str.length() > 0))
                {
                    // Look for the "=" sign and split on that.
                    int indexOf = str.indexOf("=");
                    int length = str.length();
                    String propertyName = str.substring(0, indexOf);
                    // Check that this is not a empty value
                    String value = "";
                    int valueStart = indexOf + 1;
                    if (valueStart == length)
                    {
                        value = "";
                    }
                    else
                    {
                        value = str.substring(valueStart, length);
                    }
                    properties.setProperty(propertyName, value);
                }
            }
        }
        catch (Throwable t)
        {
            throw new Exception("Unable to load property file" + propertyFile,
                    t);
        }
        return properties;
    }

    /**
     * Updates a property in a file.
     * 
     * @param property the property to update
     * @param value the value to update to
     * @param propertyFile the file path
     * @param keepBackup if an update was done, keep the old copy around
     * @param add if the property does not exist append to the end of the file
     * @return true if the file was update, false if the property did not exist
     *         and no update was done
     * @throws Exception
     */
    public static boolean updatePropertyInPropertyFile(String property,
            String value, String propertyFile, boolean keepBackup, boolean add)
            throws Exception
    {
        BufferedReader br = new BufferedReader(new FileReader(propertyFile));
        String tmpPropertiesFile = propertyFile + ".tmp";
        File tmpFile = new File(tmpPropertiesFile);
        FileOutputStream out = new FileOutputStream(tmpFile);
        PrintStream printStream = new PrintStream(out);

        String str;
        boolean didUpdate = false;
        while ((str = br.readLine()) != null)
        {
            str = str.trim();
            if ((str.startsWith("#")))
            {
                printStream.println(str);
            }
            else
            {
                // Check if this line contains the property
                if (str.indexOf(property) > -1)
                {
                    StringBuffer buffer = new StringBuffer(50);
                    buffer.append(property).append("=").append(value);
                    printStream.println(buffer.toString());
                    didUpdate = true;
                }
                else
                {
                    printStream.println(str);
                }
            }
        }

        if (!didUpdate)
        {
            if (add)
            {
                StringBuffer buffer = new StringBuffer(50);
                buffer.append(property).append("=").append(value);
                printStream.println(buffer.toString());
                didUpdate = true;
            }
        }

        printStream.close();
        // Copy over the new file now ontop of the old
        if (didUpdate)
        {
            if (keepBackup)
            {
                File oldFile = new File(propertyFile);
                String parentDirectory = oldFile.getParent();
                String fileName = oldFile.getName();
                String nextBackupName = getNextBackupName(parentDirectory,
                        fileName, ".bak");
                String nextBackupPath = parentDirectory + File.separator
                        + nextBackupName;
                DirectoryCommands.copyFile(propertyFile, nextBackupPath);
            }
            DirectoryCommands.copyFile(tmpPropertiesFile, propertyFile);
            tmpFile.delete();
            return true;
        }
        else
        {
            tmpFile.delete();
            return false;
        }
    }

    private static String getNextBackupName(String directoryPath,
            String fileName, String extension)
    {

        String startFileName = fileName + extension;

        File directory = new File(directoryPath);

        int value = -1;

        if (directory.isDirectory())
        {
            File[] files = directory.listFiles();
            for (int i = 0; i < files.length; i++)
            {
                File f = files[i];
                if (f.isFile())
                {
                    String tmpFileName = f.getName();

                    if (tmpFileName.startsWith(startFileName))
                    {
                        logger.debug("fileName is: " + tmpFileName);

                        // get the number as the file should be in format
                        // file.bak.1
                        String intString = tmpFileName.substring(
                                startFileName.length() + 1,
                                tmpFileName.length());
                        try
                        {
                            int tmpInt = Integer.parseInt(intString);
                            if (tmpInt > value)
                            {
                                value = tmpInt;
                            }
                        }
                        catch (Throwable t)
                        {
                            logger.debug("file name not recognized ignoring="
                                    + tmpFileName);
                        }

                    }
                }
            }
        }

        int newValue = value + 1;
        return fileName + extension + "." + newValue;
    }

}
