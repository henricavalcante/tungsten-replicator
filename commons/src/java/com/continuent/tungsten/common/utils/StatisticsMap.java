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

import java.util.Map;
import java.util.TreeMap;

public class StatisticsMap extends TreeMap<String, Statistic<?>>
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private String            name             = null;

    public StatisticsMap(String name)
    {
        this.name = name;
    }

    public void addDoubleStatistic(String label)
    {
        put(label, new DoubleStatistic(label));
    }

    public void addLongStatistic(String label)
    {
        put(label, new LongStatistic(label));
    }

    @SuppressWarnings("unchecked")
    public Number increment(String label)
    {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null)
        {
            return 0L;
        }
        return stat.increment();
    }

    @SuppressWarnings("unchecked")
    public Number decrement(String label)
    {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null)
        {
            return 0L;
        }
        return stat.decrement();
    }

    @SuppressWarnings("unchecked")
    public Number add(String label, Number value)
    {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null)
        {
            return 0L;
        }
        return stat.add(value);
    }

    @SuppressWarnings("unchecked")
    public Number subtract(String label, Number value)
    {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null)
        {
            return 0L;
        }
        return stat.subtract(value);
    }

    @SuppressWarnings("unchecked")
    public Number getAverage(String label)
    {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null)
        {
            return 0L;
        }
        return stat.getAverage();
    }

    @SuppressWarnings("unchecked")
    public Number getValue(String label)
    {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null)
        {
            return 0L;
        }
        return stat.getValue();
    }

    public void clear(String label)
    {
        Statistic<?> stats = get(label);
        if (stats != null)
            stats.clear();
    }

    public Map<String, ?> getMap()
    {
        return this;
    }

    public String toString()
    {
        return new ResultFormatter(this).format();
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

}
