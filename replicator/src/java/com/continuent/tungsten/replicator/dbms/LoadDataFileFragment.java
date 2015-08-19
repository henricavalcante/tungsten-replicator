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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.dbms;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class LoadDataFileFragment extends DBMSData
{

    private int fileId;
    private byte[] data;
    private String defaultSchema;

    public LoadDataFileFragment(int fileID, byte[] data)
    {
        this.fileId = fileID;
        this.data = data;
    }

    public LoadDataFileFragment(int fileId, byte[] data, String schema)
    {
        super();
        this.fileId = fileId;
        this.data = data;
        this.defaultSchema = schema;
    }

    private static final long serialVersionUID = 1L;

    public int getFileID()
    {
        return fileId;
    }
    
    public byte[] getData()
    {
        return data;
    }

    public String getDefaultSchema()
    {
        return defaultSchema;
    }
}
