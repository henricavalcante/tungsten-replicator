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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.manager.resource.physical;

import com.continuent.tungsten.common.cluster.resource.Resource;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.jmx.DynamicMBeanOperation;

public class Operation extends Resource
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    DynamicMBeanOperation     operation        = null;

    public Operation()
    {
        super(ResourceType.OPERATION, "UNKNOWN");
    }

    public Operation(String name)
    {
        super(ResourceType.OPERATION, name);
        init();
    }

    private void init()
    {
        this.childType = ResourceType.NONE;
        this.isContainer = false;
    }

    public void setOperation(DynamicMBeanOperation operation)
    {
        this.operation = operation;
    }

    /**
     * @return the operation
     */
    public DynamicMBeanOperation getOperation()
    {
        return operation;
    }

    @Override
    public String toString()
    {
        if (operation != null)
            return operation.toString();
        else
            return name;

    }

    public String describe(boolean detailed)
    {
        StringBuilder builder = new StringBuilder();

        if (operation != null)
        {
            builder.append(String.format("name=%s, type=%s\n", getName(),
                    getType()));
            builder.append(("{\n"));
            builder.append(String.format("  description=%s\n",
                    operation.getDescription()));
            builder.append(String.format("  usage=%s\n", operation.getUsage()));
            String paramDesc = operation.getParamDescription(true, "    ");
            if (paramDesc != null && paramDesc.length() > 0)
            {
                builder.append(paramDesc);
                builder.append("\n");
            }
            builder.append(("}"));

            return builder.toString();
        }

        return super.describe(detailed);
    }

}
