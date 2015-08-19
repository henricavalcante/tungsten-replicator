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

@SuppressWarnings("rawtypes")
public class TraceVector implements Comparable
{
    private TraceVectorComponent component;

    private String               category    = null;
    private int                  target      = -1;
    private String               description = "";
    private boolean              enabled     = false;

    public TraceVector()
    {
        this(TraceVectorComponent.cluster, "cluster", -1, "");
    }

    public TraceVector(TraceVectorComponent component, String category,
            int target, String description)
    {
        this.component = component;
        this.category = category;
        this.target = target;
        this.description = description;
    }

    public synchronized void enable(boolean enableFlag)
    {
        this.enabled = enableFlag;
    }

    public synchronized boolean isEnabled()
    {
        return enabled;
    }

    public String toString()
    {
        return String.format("%s/%s/%s=%s\n  [%s]", component, category,
                target, enabled, description);
    }

    public TraceVectorComponent getComponent()
    {
        return component;
    }

    public void setComponent(TraceVectorComponent component)
    {
        this.component = component;
    }

    public String getCategory()
    {
        return category;
    }

    public void setCategory(String category)
    {
        this.category = category;
    }

    public int getTarget()
    {
        return target;
    }

    public void setTarget(int target)
    {
        this.target = target;
    }

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public int compareTo(Object o)
    {
        TraceVector compareToVector = (TraceVector) o;

        if (!compareToVector.getCategory().equals(category))
        {
            return getCategory().compareTo(compareToVector.getCategory());
        }

        if (compareToVector.getTarget() != target)
        {
            return target - compareToVector.getTarget();
        }

        return 0;
    }

}
