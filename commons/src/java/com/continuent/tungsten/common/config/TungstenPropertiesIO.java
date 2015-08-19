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

package com.continuent.tungsten.common.config;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.TreeMap;

import com.continuent.tungsten.common.file.FileIO;
import com.continuent.tungsten.common.file.FileIOException;
import com.continuent.tungsten.common.file.FilePath;
import com.continuent.tungsten.common.file.JavaFileIO;

/**
 * This class reads and writes TungstenProperties data safely on a file system.
 * It uses the generic FileIO class to ensure that properties files are handled
 * as efficiently as possible.
 */
public class TungstenPropertiesIO
{
    /** Process file using JSON format serialization. */
    public static final String JSON            = "JSON";

    /** Process file using Java properties format serialization. */
    public static final String JAVA_PROPERTIES = "JAVA_PROPERTIES";

    // Properties with reasonable defaults.
    private String         format          = "JAVA_PROPERTIES";
    private String         charset         = "UTF-8";

    // Class to perform IO and location where to perform it.
    private final FileIO   fileIO;
    private final FilePath filePath;

    /**
     * Creates a new instance with user-specified FileIO implementation and file
     * path.
     */
    public TungstenPropertiesIO(FileIO fileIO, FilePath filePath)
    {
        this.fileIO = fileIO;
        this.filePath = filePath;
    }

    /**
     * Creates a new instance for OS file system operating on the
     * caller-specified file.
     */
    public TungstenPropertiesIO(File path)
    {
        this(new JavaFileIO(), new FilePath(path.getAbsolutePath()));
    }

    public String getFormat()
    {
        return format;
    }

    /** Sets the serialization format to use. */
    public void setFormat(String format)
    {
        this.format = format;
    }

    public String getCharset()
    {
        return charset;
    }

    /** Sets the character set to use. */
    public void setCharset(String charset)
    {
        this.charset = charset;
    }

    /** Returns true if the properties file exists. */
    public boolean exists()
    {
        return fileIO.exists(filePath);
    }

    /**
     * Delete the properties file.
     * 
     * @return true if fully successful, otherwise false.
     */
    public boolean delete()
    {
        return fileIO.delete(filePath, false);
    }

    /**
     * Write properties file to the file system using selected serialization
     * format and character set.
     * 
     * @param properties Properties to be written
     * @param fsync If true issue an fsync, otherwise just flush
     * @throws FileIOException Thrown if file is not writable
     */
    public void write(TungstenProperties properties, boolean fsync)
            throws FileIOException
    {
        String contents;
        if (JAVA_PROPERTIES.equals(format))
        {
            // Output into sorted properties format.
            TreeMap<String, String> map = new TreeMap<String, String>(
                    properties.map());
            StringBuffer sb = new StringBuffer();
            for (String key : map.keySet())
            {
                sb.append(String.format("%s=%s\n", key, map.get(key)));
            }
            contents = sb.toString();
        }
        else if (JSON.equals(format))
        {
            try
            {
                contents = properties.toJSON(true);
            }
            catch (Exception e)
            {
                throw new FileIOException("Unable to convert to JSON: file="
                        + filePath.toString() + " format=" + format, e);
            }
        }
        else
        {
            throw new FileIOException(
                    "Unrecognized property output format: file="
                            + filePath.toString() + " format=" + format);
        }

        // Write the results.
        fileIO.write(filePath, contents, charset, fsync);
    }

    /**
     * Read properties file from the file system using selected serialization
     * format and character set.
     * 
     * @return Properties instance
     * @throws FileIOException Thrown if file is not readable
     */
    public TungstenProperties read() throws FileIOException
    {
        // Read the file.
        String contents = fileIO.read(filePath, charset);

        // Deserialize using appropriate format.
        if (JAVA_PROPERTIES.equals(format))
        {
            StringReader sr = new StringReader(contents);
            Properties javaProps = new Properties();
            try
            {
                javaProps.load(sr);
            }
            catch (IOException e)
            {
                throw new FileIOException(
                        "Unable to read JSON properties: file="
                                + filePath.toString() + " format=" + format, e);
            }
            sr.close();
            TungstenProperties properties = new TungstenProperties();
            properties.load(javaProps);
            return properties;
        }
        else if (JSON.equals(format))
        {
            try
            {
                return TungstenProperties.loadFromJSON(contents);
            }
            catch (Exception e)
            {
                throw new FileIOException("Unable to convert to JSON: file="
                        + filePath.toString() + " format=" + format, e);
            }
        }
        else
        {
            throw new FileIOException(
                    "Unrecognized property input format: file="
                            + filePath.toString() + " format=" + format);
        }
    }
}