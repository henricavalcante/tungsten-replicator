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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.continuent.tungsten.common.cluster.resource.Resource;
import com.continuent.tungsten.common.cluster.resource.ResourceType;

/**
 * @author <a href="mailto:edward.archibald@continuent.com">Ed Archibald</a>
 * @version 1.0
 */
@SuppressWarnings("rawtypes")
public class ResourceNode implements Comparable, Serializable
{
//    static private Logger            logger           = Logger.getLogger(ResourceNode.class);
    /**
     * 
     */
    private static final long        serialVersionUID = 1L;

    private Resource                 resource;

    public Map<String, ResourceNode> children;
    public ResourceNode              parent;

    /**
     * @param resource
     */
    public ResourceNode(Resource resource)
    {
        this.resource = resource;
    }

    /**
     * @return the resource held by this node
     */
    public Resource getResource()
    {
        return resource;
    }

    /**
     * @return the type of this node
     */
    public ResourceType getType()
    {
        return resource.getType();
    }

    /**
     * @return the allowed type for children of this node
     */
    public ResourceType getChildType()
    {
        return resource.getChildType();
    }

    /**
     * @return the key for this node
     */
    public String getKey()
    {
        return resource.getKey();
    }

    /**
     * @return true if this node can contain other resources, otherwise false
     */
    public boolean isContainer()
    {
        return resource.isContainer();
    }

    /**
     * @return true if the resource for this node is executable, otherwise false
     */
    public boolean isExecutable()
    {
        return resource.isExecutable();
    }

    /**
     * Return a map of the current children of this node or an empty map.
     * 
     * @return the children of Node<String,RouterResource>
     */
    public Map<String, ResourceNode> getChildren()
    {
        if (this.children == null)
        {
            return new LinkedHashMap<String, ResourceNode>();
        }
        return this.children;
    }

    /**
     * @param children
     */
    public void setChildren(Map<String, ResourceNode> children)
    {
        this.children = children;
    }

    /**
     * Returns the number of immediate children of this
     * Node<String,RouterResource>.
     * 
     * @return the number of immediate children.
     */
    public int getNumberOfChildren()
    {
        if (children == null)
        {
            return 0;
        }
        return children.size();
    }

    /**
     * Adds a child node to this node
     * 
     * @param childNode
     */
    public void addChild(ResourceNode childNode)
    {
        if (children == null)
        {
            children = new HashMap<String, ResourceNode>();
        }
        childNode.parent = this;
        children.put(childNode.getKey(), childNode);
    }

    /**
     * Utility method that creates a new resource node from the resource passed
     * in and then adds it as a child.
     * 
     * @param child
     * @return the new node created/added as a result of this method
     */
    public ResourceNode addChild(Resource child)
    {
        if (children == null)
        {
            children = new HashMap<String, ResourceNode>();
        }

        ResourceNode childNode = new ResourceNode(child);

        childNode.parent = this;
        children.put(child.getKey(), childNode);

        return childNode;
    }

    /**
     * Remove a child by key
     * 
     * @param key
     */
    public void removeChild(String key)
    {
        children.remove(key);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getKey());

        sb.append(" {").append(getResource().toString()).append(",[");
        int i = 0;
        for (ResourceNode e : getChildren().values())
        {
            if (i > 0)
            {
                sb.append(",");
            }
            sb.append(e.getResource().toString());
            i++;
        }
        sb.append("]").append("} ");
        return sb.toString();
    }

    public ResourceNode getParent()
    {
        return parent;
    }

    public void setParent(ResourceNode parent)
    {
        this.parent = parent;
    }

    public ResourceNode getRoot()
    {
        if (parent == null)
            return this;
        else
            return parent.getRoot();
    }

    public void setResource(Resource resource)
    {
        this.resource = resource;
    }

    public int compareTo(Object comp)
    {
        ResourceNode compRes = (ResourceNode) comp;
        return resource.getName().compareTo(compRes.getResource().getName());
    }

}
