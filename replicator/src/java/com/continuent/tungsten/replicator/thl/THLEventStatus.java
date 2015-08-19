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
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class THLEventStatus
{

    private long      seqno;
    private Exception eventException;
    private short     status;

    public THLEventStatus(long seqno, Exception eventException, short status)
    {
        this.seqno = seqno;
        this.eventException = eventException;
        this.status = status;
    }

    public long getSeqno()
    {
        return seqno;
    }

    public Exception getException()
    {
        return eventException;
    }

    public short getStatus()
    {
        return status;
    }
}
