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

package com.continuent.tungsten.replicator.thl.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * Implements serialization using default Java serialization.
 */
public class JavaSerializer implements Serializer
{
    /**
     * Deserializes THLEvent off the stream. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.serializer.Serializer#deserializeEvent(java.io.InputStream)
     */
    public THLEvent deserializeEvent(InputStream inStream) throws ReplicatorException,
            IOException
    {
        ObjectInputStream oIS = new ObjectInputStream(inStream);
        Object revent;
        try
        {
            revent = oIS.readObject();
        }
        catch (ClassNotFoundException e)
        {
            throw new THLException(
                    "Class not found while deserializing THLEvent", e);
        }
        if (revent instanceof THLEvent)
            return (THLEvent) revent;
        else
        {
            throw new THLException(
                    "Unexpected class found when deserializing: "
                            + revent.getClass().getName());
        }
    }

    /**
     * Serialize the THL event onto the stream. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.serializer.Serializer#serializeEvent(com.continuent.tungsten.replicator.thl.THLEvent,
     *      java.io.OutputStream)
     */
    public void serializeEvent(THLEvent event, OutputStream outStream)
            throws IOException
    {
        ObjectOutputStream oOS = new ObjectOutputStream(outStream);
        oOS.writeObject(event);
        oOS.flush();
    }
}