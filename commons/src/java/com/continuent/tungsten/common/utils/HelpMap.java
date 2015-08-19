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

import java.util.TreeMap;

public class HelpMap extends TreeMap<String, HelpItem>
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private String            category;

    public HelpMap(String category)
    {
        this.category = category;
    }

    public void put(String command, String usage, String description)
    {
        this.put(command, new HelpItem(command, usage, description));
    }

    public HelpItem getItem(String command)
    {
        return this.getItem(command);
    }

    public String getCategory()
    {
        return category;
    }

    public void setCategory(String category)
    {
        this.category = category;
    }
}
