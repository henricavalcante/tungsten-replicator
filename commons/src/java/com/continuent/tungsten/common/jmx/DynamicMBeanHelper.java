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

package com.continuent.tungsten.common.jmx;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ObjectName;

/**
 * Dynamic access to MBean enabled class.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class DynamicMBeanHelper implements Serializable
{
    private static final long                  serialVersionUID = 1L;

    private ObjectName                         objName;
    private String                             className        = null;
    private MBeanInfo                          info             = null;
    private Map<String, DynamicMBeanOperation> methods          = new TreeMap<String, DynamicMBeanOperation>();

    /**
     * @param mbeanClass
     * @param name
     * @param info
     */
    public DynamicMBeanHelper(Class<?> mbeanClass, ObjectName name,
            MBeanInfo info)
    {
        this.objName = name;
        this.className = mbeanClass.getName();
        this.info = info;
        setMethods(mbeanClass, info);

    }

    /**
     * @param mbeanClass
     * @param info
     */
    private void setMethods(Class<?> mbeanClass, MBeanInfo info)
    {
        Map<String, Method> classMethods = getClassMethods(mbeanClass);

        for (MBeanOperationInfo operation : info.getOperations())
        {
            Method classMethod = classMethods.get(operation.getName());

            if (classMethod == null)
            {
                continue;
            }

            DynamicMBeanOperation method = new DynamicMBeanOperation(
                    classMethod, operation);
            methods.put(
                    String.format("%s(%d)", method.getName(),
                            method.getSignature().length), method);
        }
    }

    /**
     * @param mbeanClass
     */
    private Map<String, Method> getClassMethods(Class<?> mbeanClass)
    {
        Map<String, Method> classMethods = new HashMap<String, Method>();

        for (Method method : mbeanClass.getMethods())
        {
            classMethods.put(method.getName(), method);
        }

        return classMethods;
    }

    /**
     * Returns the name value.
     * 
     * @return Returns the name.
     */
    public ObjectName getName()
    {
        return objName;
    }

    /**
     * Sets the name value.
     * 
     * @param name The name to set.
     */
    public void setName(ObjectName name)
    {
        this.objName = name;
    }

    /**
     * Returns the info value.
     * 
     * @return Returns the info.DynamicMBeanHelper
     */
    public MBeanInfo getInfo()
    {
        return info;
    }

    /**
     * Sets the info value.
     * 
     * @param info The info to set.
     */
    public void setInfo(MBeanInfo info)
    {
        this.info = info;
    }

    /**
     * Returns the methods value.
     * 
     * @return Returns the methods.
     */
    public Map<String, DynamicMBeanOperation> getMethods()
    {
        return methods;
    }

    public DynamicMBeanOperation getMethod(String methodName, int paramCount)
    {
        return methods.get(String.format("%s(%d)", methodName, paramCount));
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        builder.append(String.format("ObjectName=%s\n%s\n", objName,
                classNameOnly(className)));

        return builder.toString();
    }

    public String usage()
    {
        StringBuilder builder = new StringBuilder();

        builder.append(String.format("%s\n", classNameOnly(className)));

        for (DynamicMBeanOperation method : methods.values())
        {
            builder.append(String.format("\t%s\n", method.getUsage()));
        }

        return builder.toString();
    }

    /**
     * Returns the className value.
     * 
     * @return Returns the className.
     */
    public String getClassName()
    {
        return className;
    }

    /**
     * Returns the className value.
     * 
     * @return Returns the className.
     */
    public String getShortName()
    {
        return classNameOnly(className);
    }

    /**
     * Sets the className value.
     * 
     * @param className The className to set.
     */
    public void setClassName(String className)
    {
        this.className = className;
    }

    private String classNameOnly(String className)
    {
        return className.substring(className.lastIndexOf('.') + 1);
    }
}
