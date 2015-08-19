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

package com.continuent.tungsten.common.cluster.resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.continuent.tungsten.common.config.TungstenProperties;

public class Queue<T> extends Resource
{
    private static final long      serialVersionUID = 8153881753668230575L;

    private LinkedBlockingQueue<T> items            = new LinkedBlockingQueue<T>();
    private T                      lastItem         = null;

    public Queue(TungstenProperties props)
    {
        super(ResourceType.QUEUE, props.getString("name", "queue", true));
        props.applyProperties(this, true);
    }

    public Queue(String name)
    {
        super(ResourceType.QUEUE, name);
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void put(T item)
    {
        synchronized (items)
        {
            try
            {
                items.put(item);
                lastItem = item;
            }
            catch (InterruptedException i)
            {
                // ignored
            }

        }
    }

    public T take()
    {
        try
        {
            return items.take();
        }
        catch (InterruptedException i)
        {
            // ignored
        }

        return null;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        List<T> list = new ArrayList<T>();
        list.addAll(items);

        builder.append(name).append("\n");
        builder.append("{").append("\n");
        for (T item : list)
        {
            builder.append(String.format("  %s\n", item));
        }
        builder.append("}").append("\n");

        return builder.toString();
    }

    public Collection<T> getItems()
    {
        return items;
    }

    public T getLastItem()
    {
        return lastItem;
    }

}
