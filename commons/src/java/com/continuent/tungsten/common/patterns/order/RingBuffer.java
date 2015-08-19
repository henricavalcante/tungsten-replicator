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
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

/**
 * This class defines a simple RingBuffer.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class RingBuffer<T> implements Iterable<T>, Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 4591478646508939995L;

    private static Logger     logger           = Logger.getLogger(RingBuffer.class);
    private int               capacity;
    private ArrayList<T>      elements;
    private int               nextElementIndex;

    public RingBuffer(int capacity)
    {
        if (capacity <= 0)
        {
            logger.warn(String.format("Capacity changed from %d to %d",
                    capacity, 20));
            capacity = 20;
        }

        elements = new ArrayList<T>(capacity);
        this.capacity = capacity;
        nextElementIndex = 0;
    }

    /**
     * Add an element to the buffer.
     * 
     * @param newElement
     */
    public synchronized void add(T newElement)
    {
        if (elements.size() - 1 > nextElementIndex)
        {
            elements.set(nextElementIndex++, newElement);
        }
        else
        {
            elements.add(nextElementIndex++, newElement);
        }

        if (nextElementIndex == capacity)
        {
            nextElementIndex = 0;
        }
    }

    public synchronized void setNextElementIndex(int nextElementIndex)
    {
        this.nextElementIndex = nextElementIndex;
    }

    public synchronized void addAll(RingBuffer<T> ringBuffer)
    {
        this.elements.addAll(ringBuffer.getElements());
    }

    public synchronized ArrayList<T> getElements()
    {
        return elements;
    }

    /**
     * Formats a string with elements counted and delimited by a simple text
     * header. {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        int elementCount = 0;
        for (T element : this)
        {
            builder.append(String.format(
                    "============== %d ===============\n%s\n", ++elementCount,
                    (element != null ? element : "EMPTY")));

        }

        return builder.toString();
    }

    /**
     * Get the total number of elements in the buffer.
     */
    public synchronized int getElementCount()
    {
        return elements.size();
    }

    /**
     * Get the maximum capacity of the buffer.
     */
    public synchronized int getCapacity()
    {
        return capacity;
    }

    /**
     * Get a synchronized copy of the elements in this buffer.
     */
    public synchronized RingBuffer<T> clone()
    {
        RingBuffer<T> clonedBuffer = new RingBuffer<T>(this.capacity);
        clonedBuffer.addAll(this);
        clonedBuffer.setNextElementIndex(this.getnextElementIndex());
        return clonedBuffer;
    }

    /**
     * Returns the nextElementIndex value.
     * 
     * @return Returns the nextElementIndex.
     */
    public int getnextElementIndex()
    {
        return nextElementIndex;
    }

    public int getLastElement()
    {
        if (nextElementIndex == 0)
        {
            return capacity - 1;
        }

        return nextElementIndex - 1;
    }

    /**
     * Quick and dirty unit test for this class.
     * 
     * @param argv
     */
    public static void main(String argv[])
    {

        RingBuffer<String> ringBuffer = new RingBuffer<String>(6);

        for (int i = 0; i < 4; i++)
        {
            String element = String.format("ELEMENT %d", i);
            ringBuffer.add(element);
        }

        System.out.println("SHOULD SHOW ONLY 4 ELEMENTS");
        System.out.println(ringBuffer);

        ringBuffer = new RingBuffer<String>(1);

        for (int i = 0; i < 50; i++)
        {
            String element = String.format("ELEMENT %d", i);
            ringBuffer.add(element);
        }

        System.out.println("SHOULD SHOW ONLY 1 ELEMENT, NUMBERED 49");
        System.out.println(ringBuffer);

        ringBuffer = new RingBuffer<String>(10);

        System.out.println("SHOULD NOT SHOW ANY ELEMENTS");
        System.out.println(ringBuffer.toString());

        for (int i = 0; i < 20; i++)
        {
            String element = String.format("ELEMENT %d", i);
            ringBuffer.add(element);
        }

        System.out.println("SHOULD SHOW ONLY 10 ELEMENTS, HIGHEST IS 19");
        System.out.println(ringBuffer);

        System.out.println("SHOULD PRINT A WARNING AND RESET CAPACITY TO 20");
        ringBuffer = new RingBuffer<String>(0);
        ringBuffer.add("FOO");
        ringBuffer.add("BAR");
        System.out.println(ringBuffer);

    }
    
    /**
     * Let's be iterable! {@inheritDoc}
     * 
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<T> iterator()
    {
        return new RingBufferIterator<T>(this);
    }

    /**
     * This class defines a RingBufferIterator
     * 
     * @author <a href="mailto:edward.archibald@continuent.com">Edward
     *         Archibald</a>
     * @version 1.0
     */
    public class RingBufferIterator<E> implements Iterator<E>
    {
        private ArrayList<E> elements;
        private int          lastElementIndex;
        private int          elementCursor;
        boolean              rolledOver     = false;

        /**
         * Creates a new <code>RingBufferIterator</code> object
         * 
         * @param ringBuffer
         */
        public RingBufferIterator(RingBuffer<E> ringBuffer)
        {
            RingBuffer<E> clonedBuffer = ringBuffer.clone();
            this.elements = clonedBuffer.getElements();
            this.lastElementIndex = clonedBuffer.getLastElement();
            this.elementCursor = this.lastElementIndex;
        }

        /**
         * {@inheritDoc}
         * 
         * @see java.util.Iterator#hasNext()
         */
        public boolean hasNext()
        {
            if (elements.size() == 0)
            {
                return false;
            }

            if (!rolledOver)
            {
                return true;
            }
            else if (elementCursor <= lastElementIndex)
            {
                return false;
            }

            return true;
        }
        
        /**
         * {@inheritDoc}
         * 
         * @see java.util.Iterator#next()
         */
        public E next()
        {

            E element = null;

            element = this.elements.get(elementCursor--);

            if (!rolledOver)
            {
                if (elementCursor == -1)
                {
                    this.elementCursor = elements.size() - 2;
                    rolledOver = true;
                }

                return element;
            }
            else
            {
                return element;
            }

        }

        /**
         * {@inheritDoc}
         * 
         * @see java.util.Iterator#remove()
         */
        public void remove()
        {
            

        }

    }
    
    
}
