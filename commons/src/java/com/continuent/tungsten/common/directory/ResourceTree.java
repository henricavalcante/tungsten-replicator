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
 * Initial developer(s): Ed Archibald
 * Contributor(s): 
 */

package com.continuent.tungsten.common.directory;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import com.continuent.tungsten.common.cluster.resource.Resource;

/**
 * @author <a href="mailto:edward.archibald@continuent.com">Ed Archibald</a>
 * @version 1.0
 */
public class ResourceTree extends Resource implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    protected ResourceNode    rootNode;

    /**
     * Default constructor.
     */
    public ResourceTree()
    {
        super();
    }

    /**
     * Return the root Node of the tree.
     * 
     * @return the root element.
     */
    public ResourceNode getRootNode()
    {
        return this.rootNode;
    }

    /**
     * Set the root Element for the tree.
     * 
     * @param rootNode the root element to set.
     */
    public void setRootNode(ResourceNode rootNode)
    {
        this.rootNode = rootNode;
    }

    /**
     * @return A map of all nodes represented by this resource tree
     */
    public Map<String, ResourceNode> toMap()
    {
        Map<String, ResourceNode> map = new LinkedHashMap<String, ResourceNode>();
        traverse(rootNode, map);
        return map;
    }

    /**
     * Return a representation of this tree in string form.
     */
    @Override
    public String toString()
    {
        return toMap().toString();
    }

    /**
     * @param element
     * @param map
     */
    private void traverse(ResourceNode element, Map<String, ResourceNode> map)
    {
        map.put(element.getKey(), element);
        for (ResourceNode data : element.getChildren().values())
        {
            traverse(data, map);
        }
    }
}
