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

public class TraceVectorArgs
{
    private TraceVectorComponent component;
    private String               category = null;
    private int                  target   = -1;

    public TraceVectorArgs(TraceVectorComponent component, String category,
            int target)
    {
        this.component = component;
        this.category = category;
        this.target = target;
    }

    static TraceVectorArgs getArgs(String vectorPath) throws Exception
    {
        String pathElements[] = vectorPath.split("/");

        if (pathElements.length != 3)
        {
            throw new Exception(String.format("malformed vector path '%s'",
                    vectorPath));
        }

        TraceVectorComponent component = TraceVectorComponent
                .valueOf(pathElements[0]);
        String category = pathElements[1];
        int target = Integer.parseInt(pathElements[2]);

        return new TraceVectorArgs(component, category, target);
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

    /**
     * Provides a path representation of this instance
     * 
     * @return the string "<component>/<category>/<target>"
     */
    public String toVectorPath()
    {
        return toVectorPath(component, category, target);
    }

    /**
     * Provides a path representation of the given trace vector elements
     * 
     * @return the string "<component>/<category>/<target>"
     */
    public static String toVectorPath(TraceVectorComponent component,
            String category, int target)
    {
        return String
                .format("%s/%s/%d", component.toString(), category, target);
    }
}
