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
 * Initial developer(s): Scott Martin
 * Contributor(s):
 */
package com.continuent.tungsten.replicator.database;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;

import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;

/**
 * Implements helper methods for database operations.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DatabaseHelper
{
    /**
     * Create a serializable blob from a byte array.
     * 
     * @param bytes Array from which to read
     * @throws SQLException Thrown if the safe blob cannot be instantiated.
     */
    public static SerialBlob getSafeBlob(byte[] bytes) throws SQLException
    {
        return getSafeBlob(bytes, 0, bytes.length);
    }

    /**
     * Create a serializable blob from a byte array.
     * 
     * @param bytes Array from which to read
     * @param off Offset into the array
     * @param len Length to read from offset
     * @throws SQLException Thrown if the safe blob cannot be instantiated.
     */
    public static SerialBlob getSafeBlob(byte[] bytes, int off, int len)
            throws SQLException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(bytes, off, len);
        byte[] newBytes = baos.toByteArray();
        return new SerialBlob(newBytes);
    }
}
