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

package com.continuent.tungsten.replicator.thl.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class implements a write locking mechanism by acquiring an exclusive
 * lock on a named operating system file. The lock mechanism is used to prevent
 * multiple processes from writing to disk logs.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class WriteLock
{
    File             lockFile;
    RandomAccessFile raf;
    FileLock         lock;

    /**
     * Instantiates the write lock instance.
     * 
     * @param lockFile
     * @throws ReplicatorException
     */
    public WriteLock(File lockFile) throws ReplicatorException
    {
        this.lockFile = lockFile;
    }

    /**
     * Attempt to acquire write lock. This call is idempotent.
     * 
     * @return true if lock successfully acquired
     */
    public synchronized boolean acquire() throws ReplicatorException
    {
        if (isLocked())
            return true;

        try
        {
            raf = new RandomAccessFile(lockFile, "rw");
            FileChannel channel = raf.getChannel();
            lock = channel.tryLock();
        }
        catch (FileNotFoundException e)
        {
            throw new ReplicatorException(
                    "Unable to find or create lock file: "
                            + lockFile.getAbsolutePath());
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Error while attempting to acquire file lock: "
                            + lockFile.getAbsolutePath(), e);
        }
        
        finally
        {
            if (lock == null && raf != null)
            {
                close(raf);
            }
        }

        // Clean up and return status.  If we don't get an exclusive lock,
        // we need to clean up so that the next call will get it. 
        if (lock == null)
        {
            if (raf != null)
                close(raf);
            return false;
        }
        else if (lock.isShared())
        {
            release();
            return false;
        }
        else
            return true;
    }

    /**
     * Return true if the write lock is currently acquired exclusively.
     */
    public synchronized boolean isLocked()
    {
        return (lock != null && !lock.isShared());
    }

    /**
     * Release the write lock. This call is idempotent.
     */
    public synchronized void release()
    {
        if (lock != null)
        {
            try
            {
                lock.release();
            }
            catch (IOException e)
            {
            }
            lock = null;

            close(raf);
            raf = null;
        }
    }

    // Close file, suppressing any exception.
    private void close(RandomAccessFile raf)
    {
        try
        {
            raf.close();
        }
        catch (IOException e)
        {
        }
    }
}