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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */
package com.continuent.tungsten.replicator.backup;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Contains index to storage files.  The index is used to generate new
 * file numbers. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class StorageIndex
{
    // Property serialization information. 
    private static final String VERSION_NO = "1.0";
    private static final String VERSION = "version";
    private static final String INDEX = "index";
    
    // Specification values. 
    private String version;
    private long index;
    
    /**
     * Creates an index specification from existing properties. 
     */
    public StorageIndex(TungstenProperties props)
    {
        this.version = props.getString(VERSION);
        this.index = props.getLong(INDEX);
    }
 
    /**
     * Creates a new specification whose values must be filled in. 
     */
    public StorageIndex()
    {
        this.version = VERSION_NO;
    }

    public void incrementIndex()
    {
        index++;
    }

    public long getIndex()
    {
        return index;
    }

    public void setIndex(long index)
    {
        this.index = index;
    }

    public String getVersion()
    {
        return version;
    }
    
    public TungstenProperties toProperties()
    {
        TungstenProperties props = new TungstenProperties();
        props.setString(VERSION, version);
        props.setLong(INDEX, index);
        return props;
    }
}