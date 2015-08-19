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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.dbms;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import com.continuent.tungsten.replicator.event.ReplOption;

/**
 * Implements the core class for row and SQL statement updates. All update types
 * derive from this class.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class DBMSData implements Serializable
{
    static final long          serialVersionUID = -1;

    protected List<ReplOption> options          = null;

    /**
     * Creates a new <code>DBMSData</code> object
     */
    public DBMSData()
    {
    }

    /**
     * Add an option value. This can create duplicates the option exists.
     */
    public void addOption(String name, String value)
    {
        if (options == null)
            options = new LinkedList<ReplOption>();
        options.add(new ReplOption(name, value));
    }

    /**
     * Set the value, adding a new option if it does not exist or changing the
     * existing value.
     */
    public void setOption(String name, String value)
    {
        removeOption(name);
        addOption(name, value);
    }

    /**
     * Remove an option and return its value if it exists.
     */
    public String removeOption(String name)
    {
        // Remove previous value, if any.
        if (options != null)
        {
            ReplOption existingOption = null;
            for (ReplOption replOption : options)
            {
                if (name.equals(replOption.getOptionName()))
                    existingOption = replOption;
            }
            if (existingOption != null)
            {
                options.remove(existingOption);
                return existingOption.getOptionValue();
            }
        }
        return null;
    }

    /**
     * Return all options.
     */
    public List<ReplOption> getOptions()
    {
        return options;
    }

    /**
     * Returns an option value or null if not found.
     * 
     * @param name Option name
     */
    public String getOption(String name)
    {
        if (options == null)
            return null;
        else
        {
            for (ReplOption replOption : options)
            {
                if (name.equals(replOption.getOptionName()))
                    return replOption.getOptionValue();
            }
            return null;
        }
    }
}
