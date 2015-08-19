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
package com.continuent.tungsten.replicator.extractor.mysql;

import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialException;

/**
 * This class defines a SerialBlob
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
@SuppressWarnings("serial")
public class SerialBlob extends javax.sql.rowset.serial.SerialBlob
{

    public SerialBlob(byte[] b) throws SerialException, SQLException
    {
        super(b);
    }

    public SerialBlob(Blob blob) throws SerialException, SQLException
    {
        super(blob);
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SerialException
    {
        if (length <= 0)
            return new byte[0];

        return super.getBytes(pos, length);
    }
}
