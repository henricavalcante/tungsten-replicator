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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class TraceVectorInstances
{
    TraceVectorComponent                   component;
    Map<String, Map<Integer, TraceVector>> vectorsByCategory = new TreeMap<String, Map<Integer, TraceVector>>();

    public TraceVectorInstances(TraceVectorComponent component)
    {
        this.component = component;
    }

    public void reset()
    {
        for (Map<Integer, TraceVector> vectorMap : vectorsByCategory.values())
        {
            for (TraceVector vector : vectorMap.values())
            {
                vector.enable(false);
            }
        }
    }

    public TraceVector add(String category, int target, String description)
            throws Exception
    {
        Map<Integer, TraceVector> vectorsForCategory = vectorsByCategory
                .get(category);

        if (vectorsForCategory == null)
        {
            vectorsForCategory = new TreeMap<Integer, TraceVector>();
            vectorsByCategory.put(category, vectorsForCategory);
        }

        TraceVector vector = vectorsForCategory.get(target);
        if (vector == null)
        {
            vector = new TraceVector(component, category, target, description);
            vectorsForCategory.put(target, vector);
        }
        else
        {
            throw new Exception(String.format(
                    "attempt to add duplicate vector %s", vector));
        }

        return vector;
    }

    public TraceVector enable(String category, int target, boolean enableFlag)
    {
        Map<Integer, TraceVector> vectorsForCategory = vectorsByCategory
                .get(category);

        if (vectorsForCategory == null)
        {
            return new TraceVector();
        }

        TraceVector vector = vectorsForCategory.get(target);
        if (vector == null)
        {
            return new TraceVector();
        }

        vector.enable(enableFlag);

        return vector;
    }

    public boolean isEnabled(String category, int target)
    {
        Map<Integer, TraceVector> vectorsForCategory = vectorsByCategory
                .get(category);

        if (vectorsForCategory == null)
        {
            return false;
        }

        TraceVector vector = vectorsForCategory.get(target);
        if (vector == null)
        {
            return false;
        }

        return vector.isEnabled();

    }

    public TraceVector getTrace(String category, int target)
    {
        Map<Integer, TraceVector> vectorsForCategory = vectorsByCategory
                .get(category);

        if (vectorsForCategory == null)
        {
            return new TraceVector();
        }

        return vectorsForCategory.get(target);
    }

    public Set<TraceVector> getVectorsInState(String category,
            boolean enableFlag)
    {
        Set<TraceVector> results = new TreeSet<TraceVector>();

        Map<Integer, TraceVector> vectorsForCategory = vectorsByCategory
                .get(category);

        if (vectorsForCategory == null)
        {
            return results;
        }

        for (TraceVector vector : vectorsForCategory.values())
        {
            if (vector.isEnabled() == enableFlag)
            {
                results.add(vector);
            }
        }

        return results;

    }

    public String list(boolean listEnabled, boolean listDisabled)
    {
        StringBuilder builder = new StringBuilder();

        for (String category : vectorsByCategory.keySet())
        {
            if (listEnabled)
            {
                Set<TraceVector> vectors = getVectorsInState(category, true);
                for (TraceVector vector : vectors)
                {
                    builder.append(vector).append("\n");
                }
            }
            if (listDisabled)
            {
                Set<TraceVector> vectors = getVectorsInState(category, false);
                for (TraceVector vector : vectors)
                {
                    builder.append(vector).append("\n");
                }
            }
        }

        return builder.toString();
    }

    public String toString()
    {
        return (list(true, true));
    }

}
