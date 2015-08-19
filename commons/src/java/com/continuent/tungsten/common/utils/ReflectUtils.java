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
 * Initial developer(s): Edward Archibald
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.common.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class ReflectUtils
{

    final static String NEWLINE = "\n";

    public static String describe(Object object)
    {
        StringBuilder builder = new StringBuilder();
        Class<?> clazz = object.getClass();

        Constructor<?>[] constructors = clazz.getDeclaredConstructors();
        Field[] fields = clazz.getDeclaredFields();
        Method[] methods = clazz.getDeclaredMethods();

        builder.append("Description for class: " + clazz.getName()).append(
                NEWLINE);
        builder.append(NEWLINE).append(NEWLINE);
        builder.append("Summary").append(NEWLINE);
        builder.append("-----------------------------------------").append(
                NEWLINE);
        builder.append("Constructors: " + (constructors.length))
                .append(NEWLINE);
        builder.append("Fields: " + (fields.length)).append(NEWLINE);
        builder.append("Methods: " + (methods.length)).append(NEWLINE);

        builder.append(NEWLINE).append(NEWLINE);
        builder.append(NEWLINE).append(NEWLINE);
        builder.append("Details").append(NEWLINE);
        builder.append("-----------------------------------------").append(
                NEWLINE);

        if (constructors.length > 0)
        {
            builder.append(NEWLINE);
            builder.append("Constructors:").append(NEWLINE);
            for (Constructor<?> constructor : constructors)
            {
                builder.append(constructor).append(NEWLINE);
            }
        }

        if (fields.length > 0)
        {
            builder.append(NEWLINE);
            builder.append("Fields:").append(NEWLINE);
            for (Field field : fields)
            {
                builder.append(field).append(NEWLINE);
            }
        }

        if (methods.length > 0)
        {
            builder.append(NEWLINE);
            builder.append("Methods:").append(NEWLINE);
            for (Method method : methods)
            {
                builder.append(method).append(NEWLINE);
            }
        }

        return builder.toString();
    }

    public static String describeValues(Object object)
    {
        StringBuilder builder = new StringBuilder();

        Class<?> clazz = object.getClass();

        Field[] fields = clazz.getDeclaredFields();

        if (fields.length > 0)
        {
            builder.append(NEWLINE)
                    .append("-----------------------------------------")
                    .append(NEWLINE);
            for (Field field : fields)
            {
                builder.append(field.getName());
                builder.append(" = ");
                try
                {
                    field.setAccessible(true);
                    builder.append(field.get(object)).append(NEWLINE);
                }
                catch (IllegalAccessException e)
                {
                    builder.append("(Exception Thrown: " + e + ")");
                }
            }
        }

        return builder.toString();
    }

    public static Object clone(Object o)
    {
        Object clone = null;

        try
        {
            clone = o.getClass().newInstance();
        }
        catch (InstantiationException e)
        {
            e.printStackTrace();
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
        }

        // Walk up the superclass hierarchy
        for (Class<?> obj = o.getClass(); !obj.equals(Object.class); obj = obj
                .getSuperclass())
        {
            Field[] fields = obj.getDeclaredFields();
            for (int i = 0; i < fields.length; i++)
            {
                fields[i].setAccessible(true);
                try
                {
                    // for each class/superclass, copy all fields
                    // from this object to the clone
                    fields[i].set(clone, fields[i].get(o));
                }
                catch (IllegalArgumentException e)
                {
                }
                catch (IllegalAccessException e)
                {
                }
            }
        }
        return clone;
    }

    public static Object copy(Object source, Object destination)
    {
        // Walk up the superclass hierarchy
        for (Class<?> obj = source.getClass(); !obj.equals(Object.class); obj = obj
                .getSuperclass())
        {
            Field[] fields = obj.getDeclaredFields();
            for (int i = 0; i < fields.length; i++)
            {
                fields[i].setAccessible(true);
                try
                {
                    // for each class/superclass, copy all fields
                    // from this object to the clone
                    fields[i].set(destination, fields[i].get(source));
                }
                catch (IllegalArgumentException e)
                {
                }
                catch (IllegalAccessException e)
                {
                }
            }
        }
        return destination;
    }

    /**
     * Maps primitive types to their corresponding wrapper classes.
     */
    @SuppressWarnings("rawtypes")
    private static Map primitiveWrapperMap = new HashMap();
    static
    {
        primitiveWrapperMap.put(Boolean.TYPE, Boolean.class);
        primitiveWrapperMap.put(Byte.TYPE, Byte.class);
        primitiveWrapperMap.put(Character.TYPE, Character.class);
        primitiveWrapperMap.put(Short.TYPE, Short.class);
        primitiveWrapperMap.put(Integer.TYPE, Integer.class);
        primitiveWrapperMap.put(Long.TYPE, Long.class);
        primitiveWrapperMap.put(Double.TYPE, Double.class);
        primitiveWrapperMap.put(Float.TYPE, Float.class);
    }

    /**
     * <p>
     * Converts the specified primitive type to its corresponding wrapper class.
     * </p>
     * 
     * @param clazz the class to convert, may be null
     * @return the wrapper class for <code>cls</code> or <code>cls</code> if
     *         <code>cls</code> is not a primitive. <code>null</code> if null
     *         input.
     */
    @SuppressWarnings("rawtypes")
    public static Class primitiveToWrapper(Class clazz)
    {
        Class convertedClass = clazz;
        if (clazz != null && clazz.isPrimitive())
        {
            convertedClass = (Class) primitiveWrapperMap.get(clazz);
        }
        return convertedClass;
    }
}
