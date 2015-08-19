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

package com.continuent.tungsten.common.utils;

/**
 * This class defines a DoubleStatistic
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class LongStatistic implements Statistic<Long>
{
    String label;
    Long   value = new Long(0);
    long   count;

    public LongStatistic(String label)
    {
        this.label = label;
    }

    public Long decrement()
    {
        return value -= 1;
    }

    public Long getAverage()
    {
        if (count > 0)
        {
            return value / count;
        }

        return value;
    }

    public String getLabel()
    {
        return label;
    }

    public Long getValue()
    {
        return value;
    }

    public Long increment()
    {
        return value += 1;
    }

    public void setValue(Number value)
    {
        this.value = value.longValue();
    }

    public Long add(Number value)
    {
        return this.value += value.longValue();
    }

    public Long subtract(Number value)
    {
        return this.value -= value.longValue();
    }

    public String toString()
    {
        return value.toString();
    }

    public void clear()
    {
        value = new Long(0);
        count = 0;
    }
}
