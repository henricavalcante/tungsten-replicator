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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.event;

import java.io.Serializable;

/**
 * This class stores generic name/value pairs in an easily serializable
 * format.  It provides an standard way to represent metadata and 
 * session variables. 
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ReplOption implements Serializable
{
    private static final long serialVersionUID = 1L;
    
    private String name ="";
    private String value = "";
    
    /**
     * Creates a new <code>StatementDataOption</code> object
     * 
     * @param option
     * @param value 
     */
    public ReplOption(String option, String value)
    {
        this.name = option;
        this.value  = value;
    }

    /**
     * Returns the name value.
     * 
     * @return Returns the name.
     */
    public String getOptionName()
    {
        return name;
    }

    /**
     * Returns the value value.
     * 
     * @return Returns the value.
     */
    public String getOptionValue()
    {
        return value;
    }

    /**
     * {@inheritDoc}
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return name + " = " + value;
    }
}
