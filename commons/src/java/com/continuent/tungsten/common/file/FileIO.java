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

package com.continuent.tungsten.common.file;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Denotes a class that implements simple utility methods for storing and
 * retrieving files located under a base directory. Implementations support
 * operations on both standard Linux file systems as well as Hadoop File System
 * (HDFS). For this reason we use the package-defined FileIOException as a
 * covering exception for all underlying exception types instead of exceptions
 * like IOException that only apply to on particular type of file system.
 */
public interface FileIO
{

    /** Returns true if path exists. */
    public boolean exists(FilePath path);

    /** Returns true if path is an ordinary file. */
    public boolean isFile(FilePath path);

    /** Returns true if path is a directory. */
    public boolean isDirectory(FilePath path);

    /** Returns true if path is writable. */
    public boolean writable(FilePath path);

    /** Returns true if path is readable. */
    public boolean readable(FilePath path);

    /**
     * Return a list of the names of children of this path.
     * 
     * @param path Path to search
     * @return An array of path names, which will be empty if there are no
     *         children
     */
    public String[] list(FilePath path);

    /**
     * Return a list of the names of children of this path that start with the
     * given prefix.
     * 
     * @param path Path to search
     * @param prefix Required file name prefix or null to return all children
     * @return An array of path names, which will be empty if there are no
     *         children
     */
    public String[] list(FilePath path, String prefix);

    /**
     * Create path as a new directory.
     * 
     * @param path Path to create
     * @return true if successful
     */
    public boolean mkdir(FilePath path);

    /**
     * Create path as a new directory including any intervening directories in
     * the path.
     * 
     * @param path Path to create
     * @return true if successful
     */
    public boolean mkdirs(FilePath path);

    /**
     * Delete path. This form ignoress children.
     * 
     * @param path Path to delete
     * @return true if fully successful, otherwise false.
     */
    public boolean delete(FilePath path);

    /**
     * Delete path and optionally any children. Recursive deletes fail if we
     * cannot delete all children as well as the original path.
     * 
     * @param path Path to delete
     * @param recursive If true delete child files/directories as well
     * @return true if fully successful, otherwise false.
     */
    public boolean delete(FilePath path, boolean recursive);

    /**
     * Write data to file system using UTF-8 charset for file encoding and with
     * flush only.
     * 
     * @param path The file path
     * @param value The string to write in the file
     * @throws FileIOException Thrown if path is not writable
     */
    public void write(FilePath path, String value) throws FileIOException;

    /**
     * Write data to file system with flush only.
     * 
     * @param path The file path
     * @param value The string to write in the file
     * @param charset Character set of file data (e.g., UTF-8)
     * @throws FileIOException Thrown if path is not writable
     */
    public void write(FilePath path, String value, String charset)
            throws FileIOException;

    /**
     * Writes a string into a file, replacing an existing contents. There are
     * two durability options. If fsync is true, we issue the Java equivalent of
     * fsync, which is generally sufficient to survive a file system crash. If
     * fsync is false, we just flush, which will generally survive a process
     * crash.
     * 
     * @param path The file path
     * @param value The string to write in the file
     * @param charset Character set of file data (e.g., UTF-8)
     * @param fsync If true issue an fsync, otherwise just flush
     * @throws FileIOException Thrown if path is not writable
     */

    public void write(FilePath path, String value, String charset, boolean fsync)
            throws FileIOException;

    /**
     * Returns the value of the contents of a file as a string using UTF-8 as
     * charset encoding.
     * 
     * @param path The file path
     * @return Contents of the file, which is an empty string for a 0-length
     *         file
     * @throws FileIOException Thrown if path is not readable
     */
    public String read(FilePath path) throws FileIOException;

    /**
     * Returns the value of the contents of a file as a string.
     * 
     * @param path The file path
     * @param charset Character set of file data (e.g., UTF-8)
     * @return Contents of the file, which is an empty string for a 0-length
     *         file
     * @throws FileIOException Thrown if path is not readable
     */
    public String read(FilePath path, String charset) throws FileIOException;

    /**
     * Returns an input stream that can be used to read bytes from a particular
     * path. It is the responsibility of users to close the input stream.
     * 
     * @param path The path from which to read.
     * @return An InputStream instance
     * @throws FileIOException Thrown if path is not readable
     */
    public InputStream getInputStream(FilePath path) throws FileIOException;

    /**
     * Returns an output stream that can be used to write bytes to a particular
     * path, overwriting any current contents. It is the responsibility of users
     * to close the output stream.
     * 
     * @param path The path to which to write
     * @return An OutputStream instance
     * @throws FileIOException Thrown if path is not writable
     */
    public OutputStream getOutputStream(FilePath path) throws FileIOException;
}