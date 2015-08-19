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
 * Initial developer(s): Scott Martin
 * Contributor(s): 
 */
package com.continuent.tungsten.replicator.conf;

import java.io.Serializable;
import org.apache.log4j.Logger;

/**
 * This class implements a storage location for thread information
 * relevant to performance
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class MonitorThreadBucket implements Serializable
{
    @SuppressWarnings("unused")
    private static Logger      logger          = Logger.getLogger(MonitorThreadBucket.class);
    private static final long   serialVersionUID = 1L;
    private int                 count = 0 ;
    private long                value = 0L;

    public MonitorThreadBucket()
    {
       this.count = 0;
       this.value = 0L;
    }

    public void setCount(int count)
    {
       this.count = count;
    }
    public int getCount()
    {
       return this.count;
    }
    public void setValue(long value)
    {
       this.value = value;
    }
    public long getValue()
    {
       return this.value;
    }
    public void clear()
    {
        this.count = 0;
        this.value = 0L;
    }
    public void increment(long amount)
    {
        this.count++;
        this.value += amount;
        //logger.info("count = " + count + " amount = " + amount + " value = " + value);
    }
}





