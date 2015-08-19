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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl;

/**
 * This class defines a BinaryEvent
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class THLBinaryEvent
{

    private long seqno;
    private short fragno;
    private boolean lastFrag;
    private byte[] data;
    /**
     * Creates a new <code>THLBinaryEvent</code> object
     * 
     * @param seqno
     * @param fragno
     * @param lastFrag
     * @param data
     */
    public THLBinaryEvent(long seqno, short fragno, boolean lastFrag,
            byte[] data)
    {
        this.seqno = seqno;
        this.fragno = fragno;
        this.lastFrag = lastFrag;
        this.data = data;
    }
    /**
     * Returns the seqno value.
     * 
     * @return Returns the seqno.
     */
    public long getSeqno()
    {
        return seqno;
    }
    /**
     * Returns the fragno value.
     * 
     * @return Returns the fragno.
     */
    public short getFragno()
    {
        return fragno;
    }
    /**
     * Returns the lastFrag value.
     * 
     * @return Returns the lastFrag.
     */
    public boolean isLastFrag()
    {
        return lastFrag;
    }
    /**
     * Returns the data value.
     * 
     * @return Returns the data.
     */
    public byte[] getData()
    {
        return data;
    }
}
