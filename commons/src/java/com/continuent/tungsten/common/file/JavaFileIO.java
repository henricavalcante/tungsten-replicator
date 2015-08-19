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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * Implements FileIO operations for generic OS file system supported by java.io
 * package.
 */
public class JavaFileIO implements FileIO
{
    /**
     * Internal filter class to select file names based on a prefix. If the
     * prefix value is null all names are selected.
     */
    public class LocalFilenameFilter implements FilenameFilter
    {
        private final String prefix;

        public LocalFilenameFilter(String prefix)
        {
            this.prefix = prefix;
        }

        public boolean accept(File dir, String name)
        {
            if (prefix == null)
                return true;
            else
                return name.startsWith(prefix);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#exists(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean exists(FilePath path)
    {
        return new File(path.toString()).exists();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#isFile(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean isFile(FilePath path)
    {
        return new File(path.toString()).isFile();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#isDirectory(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean isDirectory(FilePath path)
    {
        return new File(path.toString()).isDirectory();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#writable(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean writable(FilePath path)
    {
        return new File(path.toString()).canWrite();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#readable(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean readable(FilePath path)
    {
        return new File(path.toString()).canRead();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#list(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public String[] list(FilePath path)
    {
        return list(path, null);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#list(com.continuent.tungsten.common.file.FilePath,
     *      java.lang.String)
     */
    @Override
    public String[] list(FilePath path, String prefix)
    {
        // Children can only exist on directories.
        if (isDirectory(path))
        {
            File dir = new File(path.toString());
            String[] seqnoFileNames = dir.list(new LocalFilenameFilter(prefix));
            return seqnoFileNames;
        }
        else
        {
            return new String[0];
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#mkdir(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean mkdir(FilePath path)
    {
        return new File(path.toString()).mkdirs();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#mkdirs(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean mkdirs(FilePath path)
    {
        return new File(path.toString()).mkdirs();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#delete(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean delete(FilePath path)
    {
        return delete(path, false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#delete(com.continuent.tungsten.common.file.FilePath,
     *      boolean)
     */
    @Override
    public boolean delete(FilePath path, boolean recursive)
    {
        // If the node does not exist, return immediately.
        if (!exists(path))
            return true;

        // Try to delete children if this is recursive. Otherwise,
        // we cannot continuent and must return.
        if (isDirectory(path))
        {
            for (String child : list(path))
            {
                if (recursive)
                {
                    boolean deleted = delete(new FilePath(path, child),
                            recursive);
                    if (!deleted)
                        return false;
                }
                else
                    return false;
            }
        }

        // Delete the path for which we were called.
        File fileToDelete = new File(path.toString());
        return fileToDelete.delete();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#write(com.continuent.tungsten.common.file.FilePath,
     *      java.lang.String)
     */
    @Override
    public void write(FilePath path, String value) throws FileIOException
    {
        write(path, value, "UTF-8", false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#write(com.continuent.tungsten.common.file.FilePath,
     *      java.lang.String, java.lang.String)
     */
    @Override
    public void write(FilePath path, String value, String charset)
            throws FileIOException
    {
        write(path, value, charset, false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#write(com.continuent.tungsten.common.file.FilePath,
     *      java.lang.String, java.lang.String, boolean)
     */

    @Override
    public void write(FilePath path, String value, String charset, boolean fsync)
            throws FileIOException
    {
        // Write the value and flush to storage. This overwrites any
        // previous version.
        FileOutputStream fos = null;
        try
        {
            fos = getOutputStream(path);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos,
                    charset));
            bw.write(value);
            bw.flush();
            if (fsync)
            {
                fos.getFD().sync();
            }
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to write data to file: file="
                    + path.toString() + " value=" + safeSynopsis(value, 20), e);
        }
        finally
        {
            if (fos != null)
            {
                try
                {
                    fos.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }

    // Print a prefix of a string value avoiding accidental index
    // out-of-bounds errors.
    private String safeSynopsis(String value, int length)
    {
        if (value.length() <= length)
            return value;
        else
            return value.substring(0, 10) + "...";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#read(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public String read(FilePath path) throws FileIOException
    {
        return read(path, "UTF-8");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#read(com.continuent.tungsten.common.file.FilePath,
     *      java.lang.String)
     */
    @Override
    public String read(FilePath path, String charset) throws FileIOException
    {
        // Read from storage.
        FileInputStream fos = null;
        try
        {
            fos = getInputStream(path);
            BufferedReader bf = new BufferedReader(new InputStreamReader(fos,
                    charset));
            StringBuffer buf = new StringBuffer();
            int nextChar = 0;
            while ((nextChar = bf.read()) > -1)
            {
                buf.append((char) nextChar);
            }
            return buf.toString();
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to read data from file: file="
                    + path.toString(), e);
        }
        finally
        {
            if (fos != null)
            {
                try
                {
                    fos.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#getInputStream(com.continuent.tungsten.common.file.FilePath)
     */
    public FileInputStream getInputStream(FilePath path) throws FileIOException
    {
        try
        {
            File f = new File(path.toString());
            return new FileInputStream(f);
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to read data from file: file="
                    + path.toString(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#getOutputStream(com.continuent.tungsten.common.file.FilePath)
     */
    public FileOutputStream getOutputStream(FilePath path)
            throws FileIOException
    {
        try
        {
            File f = new File(path.toString());
            return (new FileOutputStream(f));
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to write data to file: file="
                    + path.toString(), e);
        }
    }
}