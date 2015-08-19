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
 * Initial developer(s):
 * Contributor(s):
 */

package com.continuent.tungsten.common.patterns.order;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class Sequence implements Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private long              wrapAround       = 0L;
    private long              generation       = 0L;
    private boolean           isWrapped        = false;

    private UUID              identity         = UUID.randomUUID();

    private AtomicLong        currentValue     = new AtomicLong(0);
    private AtomicLong        lastValue        = new AtomicLong(0);

    public Sequence(long wrapAround)
    {
        this.wrapAround = wrapAround;
    }

    public Sequence()
    {
        this.wrapAround = Long.MAX_VALUE;
    }

    public synchronized boolean isBefore(Sequence sequence)
            throws SequenceException
    {
        assertSameParent(sequence);

        if (isEqual(sequence))
        {
            return false;
        }

        // We can't be before if our generation is later...
        if (this.generation > sequence.generation)
        {
            return false;
        }

        // If our generation is earlier than the sequence generation
        // it means that the sequence generation has wrapped
        // beyond the current range and, therefore, we must be
        // before that wrapping point.
        if (this.generation < sequence.generation)
        {
            return true;
        }

        // If both are not wrapped and are of the same generation
        if (!this.isWrapped && !sequence.isWrapped
                && (this.generation == sequence.generation))
        {
            // Just returned whether or not our value is before the sequence
            // value
            return (currentValue.get() < sequence.currentValue.get());

        }

        if (this.isWrapped && !sequence.isWrapped
                && this.generation == sequence.generation)
        {
            return true;
        }

        return false;
    }

    public synchronized boolean isBeforeOrEqual(Sequence sequence)
            throws SequenceException
    {
        assertSameParent(sequence);

        return (isBefore(sequence) || isEqual(sequence));
    }

    public synchronized boolean isAfter(Sequence sequence)
            throws SequenceException
    {
        assertSameParent(sequence);

        if (isEqual(sequence) || isBefore(sequence))
        {
            return false;
        }

        return true;

    }

    public boolean isEqual(Sequence sequence) throws SequenceException
    {
        assertSameParent(sequence);

        if (currentValue.get() == sequence.currentValue.get()
                && this.generation == sequence.generation)
        {
            return true;
        }

        return false;
    }

    public synchronized void next()
    {
        lastValue.set(currentValue.get());

        if (currentValue.get() == wrapAround)
        {
            currentValue.set(0);
            generation++;
            isWrapped = true;
        }
        else
        {
            currentValue.incrementAndGet();
            isWrapped = false;
        }
    }

    @Override
    public synchronized Sequence clone()
    {
        Sequence clone = new Sequence(wrapAround);
        clone.identity = identity;
        clone.currentValue.set(currentValue.get());
        clone.lastValue.set(lastValue.get());

        return clone;
    }

    public String toString()
    {
        return "Sequence(" + generation + ":" + currentValue + ")";
    }

    /**
     * Returns the wrapAround value.
     * 
     * @return Returns the wrapAround.
     */
    public long getWrapAround()
    {
        return wrapAround;
    }

    /**
     * Sets the wrapAround value.
     * 
     * @param wrapAround The wrapAround to set.
     */
    public void setWrapAround(long wrapAround)
    {
        this.wrapAround = wrapAround;
    }

    /**
     * Returns the generation value.
     * 
     * @return Returns the generation.
     */
    public long getGeneration()
    {
        return generation;
    }

    /**
     * Sets the generation value.
     * 
     * @param generation The generation to set.
     */
    public void setGeneration(long generation)
    {
        this.generation = generation;
    }

    /**
     * Returns the isWrapped value.
     * 
     * @return Returns the isWrapped.
     */
    public boolean isWrapped()
    {
        return isWrapped;
    }

    /**
     * Sets the isWrapped value.
     * 
     * @param isWrapped The isWrapped to set.
     */
    public void setWrapped(boolean isWrapped)
    {
        this.isWrapped = isWrapped;
    }

    /**
     * Returns the identity value.
     * 
     * @return Returns the identity.
     */
    public UUID getIdentity()
    {
        return identity;
    }

    /**
     * Sets the identity value.
     * 
     * @param identity The identity to set.
     */
    public void setIdentity(UUID identity)
    {
        this.identity = identity;
    }

    /**
     * Returns the currentValue value.
     * 
     * @return Returns the currentValue.
     */
    public AtomicLong getCurrentValue()
    {
        return currentValue;
    }

    /**
     * Sets the currentValue value.
     * 
     * @param currentValue The currentValue to set.
     */
    public void setCurrentValue(AtomicLong currentValue)
    {
        this.currentValue = currentValue;
    }

    /**
     * Returns the lastValue value.
     * 
     * @return Returns the lastValue.
     */
    public AtomicLong getLastValue()
    {
        return lastValue;
    }

    /**
     * Sets the lastValue value.
     * 
     * @param lastValue The lastValue to set.
     */
    public void setLastValue(AtomicLong lastValue)
    {
        this.lastValue = lastValue;
    }

    public void assertSameParent(Sequence sequence) throws SequenceException
    {
        if (!identity.toString().equals(sequence.identity.toString()))
        {
            throw new SequenceException("Parents differ, this=" + identity
                    + ", sequence=" + sequence.getIdentity());
        }
    }

    public void reset()
    {
        currentValue.set(0);
        generation = 0;
        isWrapped = false;
    }
}
