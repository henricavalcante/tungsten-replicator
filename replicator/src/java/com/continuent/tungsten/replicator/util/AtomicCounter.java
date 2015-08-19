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
 * Initial developer(s): Teemu Ollakka, Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.util;

import org.apache.log4j.Logger;

/**
 * Defines a simple "atomic counter" that allows clients to increment the
 * encapsulated sequence number and wait synchronously until particular values
 * are reached. This class is thread-safe.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class AtomicCounter
{
    private static Logger logger = Logger.getLogger(AtomicCounter.class);
    private long          seqno;

    /**
     * Creates a new <code>Sequencer</code> object with a starting value.
     * 
     * @param seqno Initial sequence number
     */
    public AtomicCounter(long seqno)
    {
        this.seqno = seqno;
    }

    /**
     * Get value of current seqno.
     */
    public synchronized long getSeqno()
    {
        return seqno;
    }

    /**
     * Sets values of current seqno. Value can only be set upward.
     */
    public synchronized void setSeqno(long seqno)
    {
        if (this.seqno < seqno)
        {
            this.seqno = seqno;
            notifyAll();
        }
    }

    /**
     * Increment seqno and notify waiters, then return value.
     */
    public synchronized long incrAndGetSeqno()
    {
        seqno++;
        notifyAll();
        return seqno;
    }

    /**
     * Decrement seqno and notify waiters, then return value.
     */
    public synchronized long decrAndGetSeqno()
    {
        seqno--;
        notifyAll();
        return seqno;
    }

    /**
     * Wait until seqno is greater than or equal to the desired value.
     * 
     * @param waitSeqno Sequence number to wait for
     * @throws InterruptedException if somebody cancels the wait
     */
    public synchronized void waitSeqnoGreaterEqual(long waitSeqno)
            throws InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("Waiting for sequence number: " + waitSeqno);
        while (waitSeqno > seqno)
            this.wait();
    }

    /**
     * Wait until seqno is greater than or equal to the desired value *or* we
     * exceed the timeout.
     * 
     * @param waitSeqno Sequence number to wait for
     * @param millis Number of milliseconds to wait
     * @return True if wait was successful, otherwise false
     * @throws InterruptedException if somebody cancels the wait
     */
    public synchronized boolean waitSeqnoGreaterEqual(long waitSeqno,
            long millis) throws InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("Waiting for sequence number: " + waitSeqno);

        // Compute end time.
        long startMillis = System.currentTimeMillis();
        long endMillis = startMillis + millis;

        // Loop until the end time is met or exceeded.
        while (waitSeqno > seqno)
        {
            this.wait(millis);
            long currentMillis = System.currentTimeMillis();
            millis = endMillis - currentMillis;
            if (millis <= 0)
                break;
        }

        // Return true if we achieved the desired sequence number.
        return (waitSeqno <= seqno);
    }

    /**
     * Wait until seqno is less than or equal to the desired value.
     * 
     * @param waitSeqno Sequence number to wait for
     * @throws InterruptedException if somebody cancels the wait
     */
    public synchronized void waitSeqnoLessEqual(long waitSeqno)
            throws InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("Waiting for sequence number: " + waitSeqno);
        while (waitSeqno < seqno)
            this.wait();
    }

    /**
     * Print a string representation of the value.
     */
    public synchronized String toString()
    {
        return this.getClass().toString() + " [" + seqno + "]";
    }
}
