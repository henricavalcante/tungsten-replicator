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
 * Initial developer(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.filter;

/**
 * This class defines a more raw Filter by the fact that its properties are not
 * automatically set by using setter methods. Implementations must configure
 * their properties manually.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public interface FilterManualProperties extends Filter
{
    /**
     * Set filter's configuration prefix. This is important in order for the
     * filter to be able to know where its properties are in the configuration
     * file.<br/>
     * Eg. of how filter's properties could be read:<br/>
     * <code>
     * TungstenProperties filterProperties = properties.subset(configPrefix
                + ".", true);
     * </code>
     * 
     * @param configPrefix Configuration prefix.
     * @see com.continuent.tungsten.common.config.TungstenProperties#subset(String, boolean)
     */
    public void setConfigPrefix(String configPrefix);
}
