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
 * Initial developer(s):
 * Contributor(s): 
 */

package com.continuent.tungsten.common.utils;

/**
 * Kudos to Bob Lee for coming up with this nice utility for debugging
 * serialization issues.  The original post was found here:
 * 
 * http://crazybob.org/2007/02/debugging-serialization.html
 * 
 * Also, try:
 *   -Dsun.io.serialization.extendedDebugInfo=true 
 */
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class SerializationDebuggingUtils extends ObjectOutputStream
{

    private static final Field DEPTH_FIELD;
    static
    {
        try
        {
            DEPTH_FIELD = ObjectOutputStream.class.getDeclaredField("depth");
            DEPTH_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e)
        {
            throw new AssertionError(e);
        }
    }

    final List<Class<?>>       stack  = new ArrayList<Class<?>>();

    /**
     * Indicates whether or not OOS has tried to write an IOException
     * (presumably as the result of a serialization error) to the stream.
     */
    boolean                    broken = false;

    public SerializationDebuggingUtils(OutputStream out) throws IOException
    {
        super(out);
        enableReplaceObject(true);
    }

    /**
     * Abuse {@code replaceObject()} as a hook to maintain our stack.
     */
    protected Object replaceObject(Object o)
    {
        // ObjectOutputStream writes serialization
        // exceptions to the stream. Ignore
        // everything after that so we don't lose
        // the path to a non-serializable object. So
        // long as the user doesn't write an
        // IOException as the root object, we're OK.
        int currentDepth = currentDepth();
        if (o instanceof IOException && currentDepth == 0)
        {
            broken = true;
        }
        if (!broken)
        {
            truncate(currentDepth);
            stack.add(o.getClass());
        }
        return o;
    }

    private void truncate(int depth)
    {
        while (stack.size() > depth)
        {
            pop();
        }
    }

    private Object pop()
    {
        return stack.remove(stack.size() - 1);
    }

    /**
     * Returns a 0-based depth within the object graph of the current object
     * being serialized.
     */
    private int currentDepth()
    {
        try
        {
            Integer oneBased = ((Integer) DEPTH_FIELD.get(this));
            return oneBased - 1;
        }
        catch (IllegalAccessException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Returns the path to the last object serialized. If an exception occurred,
     * this should be the path to the non-serializable object.
     */
    public List<Class<?>> getStack()
    {
        return stack;
    }

    /*
     * Returns a nicely formatted stack.
     */
    public String printStack()
    {
        StringBuilder builder = new StringBuilder();
        for (Class<?> clazz : getStack())
        {
            builder.append(clazz.getName()).append("\n");
        }
        return builder.toString();
    }

    public static Object testForSerialization(Object objToValidate)
            throws RuntimeException
    {
        SerializationDebuggingUtils obj_out = null;
        ObjectInputStream obj_in = null;

        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            obj_out = new SerializationDebuggingUtils(baos);

            obj_out.writeObject(objToValidate);
            byte[] buf = baos.toByteArray();
            obj_out.close();

            ByteArrayInputStream bais = new ByteArrayInputStream(buf);
            obj_in = new ObjectInputStream(bais);

            return obj_in.readObject();

        }
        catch (Exception e)
        {
            String message = String.format(
                    "Exception during serialization/deserialization of %s. Exeption=%s\n"
                            + "Path to bad object:\n%s\n", objToValidate
                            .getClass().getName(), e, obj_out.printStack());
            System.out.println(message);
            throw new RuntimeException(message);

        }
    }

    public static boolean canBeSerialized(Object objToValidate)
    {
        try
        {
            testForSerialization(objToValidate);
        }
        catch (RuntimeException e)
        {
            return false;
        }

        return true;
    }
}
