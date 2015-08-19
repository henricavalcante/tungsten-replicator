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
 * Initial developer(s): Edward Archibald
 * Contributor(s): 
 */

package com.continuent.tungsten.common.utils;

/**
 * This class defines a DoubleStatistic
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class DoubleStatistic implements Statistic<Double>
{
    String label;
    Double value = new Double(0);
    long   count;

    public DoubleStatistic(String label)
    {
        this.label = label;
    }

    public Double decrement()
    {
        return value -= 1;
    }

    public Double getAverage()
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

    public Double getValue()
    {
        return value;
    }

    public Double increment()
    {
        return value += 1;
    }

    public void setValue(Number value)
    {
        this.value = value.doubleValue();
    }

    public Double add(Number value)
    {
        count++;
        return this.value += value.longValue();
    }

    public Double subtract(Number value)
    {
        return this.value -= value.doubleValue();
    }

    public String toString()
    {
        return value.toString();
    }

    public void clear()
    {
        value = new Double(0);
        count = 0;
    }
}
