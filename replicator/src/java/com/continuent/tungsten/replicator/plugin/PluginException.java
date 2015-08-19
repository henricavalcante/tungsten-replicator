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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.plugin;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * 
 * This class defines a PluginException
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class PluginException extends ReplicatorException
{

    private static final long serialVersionUID = 1L;

    /**
     * 
     * Creates a new <code>PluginException</code> object
     * 
     * @param msg
     */
    public PluginException(String msg)
    {
        super(msg);
    }

    /**
     * 
     * Creates a new <code>PluginException</code> object
     * 
     * @param throwable
     */
    public PluginException(Throwable throwable)
    {
        super(throwable);
    }
    
    /**
     * 
     * Creates a new <code>PluginException</code> object
     * 
     * @param msg
     * @param throwable
     */
    public PluginException(String msg, Throwable throwable)
    {
        super(msg, throwable);
    }
    
}
