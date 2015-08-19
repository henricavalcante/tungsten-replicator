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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Commands to assist in directory manipulation
 * 
 * @author <a href="mailto:joe.daly@continuent.com">Joe Daly</a>
 * @version 1.0
 */
public class DirectoryCommands
{

    static Logger logger = Logger.getLogger(DirectoryCommands.class);

    /**
     * Checks if the given path is a directory
     * 
     * @param path the path to check if its a directory
     * @return true if the path is a directory
     */
    public static boolean isDirectory(String path)
    {
        File f = new File(path);
        return f.isDirectory();
    }

    /**
     * Checks if the given path is a file
     * 
     * @param path the path to check if its a file
     * @return true if the given path is a file
     */
    public static boolean isFile(String path)
    {
        File f = new File(path);
        return f.isFile();
    }

    /**
     * Checks if the path given exists. No distinction is made between a file or
     * directory
     * 
     * @param path the path to check
     * @return true if a file or directry exists
     */
    public static boolean exists(String path)
    {
        File f = new File(path);
        return f.exists();
    }

    /**
     * Creates a directory
     * 
     * @param path the path to create
     * @return true if the directory was created
     */
    public static boolean mkdir(String path)
    {
        File f = new File(path);
        return f.mkdir();
    }

    /**
     * Creates a directory and any subdirectories
     * 
     * @param path the path to create, will also create any subdirectories
     * @return true if the directory was created
     */
    public static boolean mkdirs(String path)
    {
        File f = new File(path);
        return f.mkdirs();
    }

    /**
     * Removes the given directory and any sub directories. This is equivalent
     * on unix to running rm -rf.
     * 
     * @param path remove this directory and any sub directories
     * @return true if the directory no longer exists
     */
    public static boolean deleteDirectory(String path)
    {
        return deleteDirectory(new File(path));
    }

    /**
     * Deletes files in a directory matching a specific pattern
     * 
     * @param directoryPath the directory to delete files in
     */
    public static boolean deleteFiles(String directoryPath)
    {
        int filesDeletedCount = 0;

        List<String> files = fileList(directoryPath, true);
        Iterator<String> filesIterator = files.iterator();
        while (filesIterator.hasNext())
        {
            String file = filesIterator.next();
            deleteFile(file);
            filesDeletedCount++;
        }

        return (filesDeletedCount > 0 ? true : false);
    }

    /**
     * Deletes a file
     * 
     * @param filePath the file to delete
     * @return true if the file is deleted
     */
    public static boolean deleteFile(String filePath)
    {
        File file = new File(filePath);
        if (file.isDirectory())
        {
            return false;
        }
        else
        {
            return file.delete();
        }
    }

    /**
     * Returns a listing of files in a directory matching a pattern.
     * 
     * @param directoryPath the directory to get a file listing of;
     *            listingPattern the pattern to search for in the file listing
     * @return the contents of the directory that match the pattern
     */
    public static List<String> fileList(String directoryPath,
            boolean includeFullPath)
    {

        List<String> fileList = new ArrayList<String>();

        String dir = directoryPath.substring(0,
                directoryPath.lastIndexOf(File.separator));
        String pattern = StringUtils.substringAfterLast(directoryPath, File.separator);
        
        
        File directory = new File(dir);

        if (directory.isDirectory())
        {
            File[] files = directory.listFiles();
            for (int i = 0; i < files.length; i++)
            {
                File f = files[i];
                if (f.isFile())
                {
                    String fileName = f.getName();
                    String fullPath = dir + File.separator + fileName;

                    // CONT-93
                    // The list of files is coming from the directoryPath, we only need to check the pattern against the filename.
                    if (fileName.matches(convertGlobToRegEx(pattern)))
                    {
                        if (includeFullPath)
                        {
                            fileList.add(fullPath);
                        }
                        else
                        {
                            fileList.add(fileName);
                        }
                    }
                }
            }
        }
        return fileList;
    }

    private static boolean deleteDirectory(File path)
    {
        if (path.exists())
        {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++)
            {
                if (files[i].isDirectory())
                {
                    deleteDirectory(files[i]);
                }
                else
                {
                    files[i].delete();
                }
            }
        }
        else
        {
            return true;
        }
        return (path.delete());
    }

    /**
     * Copy a file on the local filesystem
     * 
     * @param srcPath the source file
     * @param destPath where the file should be copied to
     * @throws Exception if there was a problem copying the file
     */
    public static void copyFile(String srcPath, String destPath)
            throws Exception
    {
        File srcFile = new File(srcPath);
        File destFile = new File(destPath);

        if (!srcFile.exists())
        {
            throw new Exception("source file does not exist");
        }

        String srcFileLocation = srcFile.getAbsolutePath();
        String destFileLocation = destFile.getAbsolutePath();
        if (srcFileLocation.equals(destFileLocation))
        {
            throw new Exception("source and destination are the same file");
        }

        InputStream input = new FileInputStream(srcFile);
        OutputStream output = new FileOutputStream(destFile);

        byte[] buf = new byte[1024];
        int length;
        while ((length = input.read(buf)) > 0)
        {
            output.write(buf, 0, length);
        }
        input.close();
        output.close();
    }

    static private String convertGlobToRegEx(String line)
    {
        line = line.trim();
        int strLen = line.length();
        StringBuilder sb = new StringBuilder(strLen);

        boolean escaping = false;
        int inCurlies = 0;
        for (char currentChar : line.toCharArray())
        {
            switch (currentChar)
            {
                case '*' :
                    if (escaping)
                        sb.append("\\*");
                    else
                        sb.append(".*");
                    escaping = false;
                    break;
                case '?' :
                    if (escaping)
                        sb.append("\\?");
                    else
                        sb.append('.');
                    escaping = false;
                    break;
                case '.' :
                case '(' :
                case ')' :
                case '+' :
                case '|' :
                case '^' :
                case '$' :
                case '@' :
                case '%' :
                    sb.append('\\');
                    sb.append(currentChar);
                    escaping = false;
                    break;
                case '\\' :
                    if (escaping)
                    {
                        sb.append("\\\\");
                        escaping = false;
                    }
                    else
                        escaping = true;
                    break;
                case '{' :
                    if (escaping)
                    {
                        sb.append("\\{");
                    }
                    else
                    {
                        sb.append('(');
                        inCurlies++;
                    }
                    escaping = false;
                    break;
                case '}' :
                    if (inCurlies > 0 && !escaping)
                    {
                        sb.append(')');
                        inCurlies--;
                    }
                    else if (escaping)
                        sb.append("\\}");
                    else
                        sb.append("}");
                    escaping = false;
                    break;
                case ',' :
                    if (inCurlies > 0 && !escaping)
                    {
                        sb.append('|');
                    }
                    else if (escaping)
                        sb.append("\\,");
                    else
                        sb.append(",");
                    break;
                default :
                    escaping = false;
                    sb.append(currentChar);
            }
        }
        return sb.toString();
    }

}
