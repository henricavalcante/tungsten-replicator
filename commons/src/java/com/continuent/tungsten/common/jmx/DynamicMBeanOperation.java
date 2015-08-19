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
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

import javax.management.MBeanOperationInfo;

import com.continuent.tungsten.common.utils.CLLogLevel;
import com.continuent.tungsten.common.utils.CLUtils;
import com.continuent.tungsten.common.utils.ReflectUtils;

/**
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class DynamicMBeanOperation implements Serializable
{
    /**
     * 
     */
    private static final long              serialVersionUID = 1L;
    private String                         name             = null;
    private String                         description      = null;
    private String                         usage            = null;
    private Vector<String>                 signature        = new Vector<String>();
    private Map<String, DynamicMBeanParam> params           = new LinkedHashMap<String, DynamicMBeanParam>();

    /**
     * @param method
     * @param info
     */
    public DynamicMBeanOperation(Method method, MBeanOperationInfo info)
    {
        name = method.getName();
        description = name;

        setParamsAndSignature(method, info);
        setUsage(defaultUsage());

        MethodDesc mdesc = (MethodDesc) method.getAnnotation(MethodDesc.class);
        if (mdesc != null)
        {
            if (!mdesc.description().equals(""))
                description = mdesc.description();
            else
                description = name;

            if (mdesc.usage() != null || mdesc.usage().length() > 0)
            {
                setUsage(mdesc.usage());
            }
        }

    }

    /**
     * @param method
     * @param info
     */
    private void setParamsAndSignature(Method method, MBeanOperationInfo info)
    {
        Class<?>[] paramTypes = method.getParameterTypes();
        Annotation[][] paramAnnotations = method.getParameterAnnotations();

        for (int i = 0; i < paramTypes.length; i++)
        {
            String name = String.format("param%d", i + 1);
            String description = "";
            String displayName = name;

            Class<?> type = paramTypes[i];

            for (Annotation annotation : paramAnnotations[i])
            {
                if (annotation instanceof ParamDesc)
                {
                    ParamDesc desc = (ParamDesc) annotation;
                    name = desc.name();
                    if (!desc.description().equals(""))
                    {
                        description = desc.description();
                    }
                    else
                    {
                        description = name;
                    }
                    if (!desc.displayName().equals(""))
                    {
                        displayName = desc.displayName();
                    }
                    else
                    {
                        displayName = name;
                    }
                }
            }

            DynamicMBeanParam param = new DynamicMBeanParam(name, i,
                    displayName, description, type);

            // We put them in the map by display name
            params.put(displayName, param);
            signature.add(type.getName());

        }
    }

    /**
     * @return the signature of this operation
     */
    public String[] getSignature()
    {
        String[] signatureArray = new String[signature.size()];

        for (int i = 0; i < signature.size(); i++)
            signatureArray[i] = signature.get(i);

        return signatureArray;

    }

    /**
     * This method converts a map of named parameters into a position-ordered
     * set of parameters. At the same time that it is doing this, it verifies
     * that all parameters have been supplied, and that the types are correct.
     * 
     * @param paramMap
     * @return an array of objects in param order
     */
    public Object[] validateAndGetNamedParams(Map<String, Object> paramMap)
            throws Exception
    {
        if (paramMap == null)
        {
            if (params.size() > 0)
            {
                throw new Exception(
                        String.format(
                                "No parameters passed  to validateAndGetNamedParams but %d were required",
                                params.size()));
            }
        }

        if (params.size() == 0)
        {
            return null;
        }

        Object[] mbeanParams = new Object[params.size()];

        LinkedHashMap<String, DynamicMBeanParam> copyParams = new LinkedHashMap<String, DynamicMBeanParam>(
                params);

        for (String paramName : paramMap.keySet())
        {
            Object paramValue = paramMap.get(paramName);

            DynamicMBeanParam mbeanParam = copyParams.get(paramName);

            if (mbeanParam == null)
            {
                throw new Exception(
                        String.format("No parameter %s found for method %s.",
                                paramName, name));
            }

            // We have a parameter match, now check the datatype
            // match.
            if (paramValue != null)
            {
                CLUtils.println(
                        String.format(
                                "Checking datatype for param %s: value type=%s, param type=%s",
                                paramName, paramValue.getClass()
                                        .getSimpleName(), mbeanParam.getType()
                                        .getSimpleName()), CLLogLevel.debug);

                if (paramValue.getClass() != mbeanParam.getType())
                {
                    try
                    {
                        // Determine whether we can do some parsing, so the
                        // later JMX call would not throw an invalid cast
                        // exception.
                        if (mbeanParam.getType().isPrimitive())
                        {
                            CLUtils.println(
                                    String.format(
                                            "Testing for castability of param %s value %s to String",
                                            paramName, paramValue),
                                    CLLogLevel.debug);
                            Class<?> wrapperClass = ReflectUtils
                                    .primitiveToWrapper(mbeanParam.getType());
                            paramValue = wrapperClass.getConstructor(
                                    String.class).newInstance(paramValue);
                        }
                    }
                    catch (Exception e)
                    {
                        if (CLUtils.getLogLevel().ordinal() >= CLLogLevel.debug
                                .ordinal())
                        {
                            CLUtils.println(String
                                    .format("EXCEPTION: Validating params for operation %s\n%s",
                                            getUsage(), e.toString()));
                            e.printStackTrace();
                        }
                        // Ignore any exception as this is a usability
                        // increasing step. We pass the argument as is in case
                        // of exception.
                    }
                }
            }

            mbeanParams[mbeanParam.getOrder()] = paramValue;
            copyParams.remove(paramName);
        }

        // Finally, check to make sure all required params were supplied.
        if (!copyParams.isEmpty())
        {
            throw new Exception(
                    String.format(
                            "Parameters passed in were missing required parameters. Usage: %s\nMissing params:\n%s",
                            defaultUsage(), CLUtils
                                    .iterableToCommaSeparatedList(copyParams
                                            .keySet())));
        }

        return mbeanParams;

    }

    public Object[] validateAndGetPositionalParams(Map<String, Object> paramMap)
            throws Exception
    {
        if (paramMap == null)
        {
            if (params.size() > 0)
            {
                throw new Exception(String.format(
                        "No parameters passed but %d were required",
                        params.size()));
            }
        }

        if (params.size() == 0)
        {
            return null;
        }

        Object[] mbeanParams = new Object[paramMap.size()];
        // Object[] copyMBeanParams = (Object[]) (params.values().toArray());
        int i = 0;

        boolean inString = false;
        @SuppressWarnings("unused")
        String aggregateString = "";

        for (Object param : paramMap.values())
        {
            // Determine whether we can do some parsing, so the
            // later JMX call would not throw an invalid cast
            // exception.
            try
            {
                // DynamicMBeanParam mbeanParam = (DynamicMBeanParam)
                // copyMBeanParams[i];
                if (param != null)
                {
                    // Parse null.
                    if (param.getClass() == String.class)
                    {
                        if (!inString)
                        {
                            if (param.toString().compareTo("null") == 0)
                            {
                                param = null;
                            }
                            else if (param.toString().compareTo("true") == 0)
                            {
                                param = true;
                            }
                            else if (param.toString().compareTo("false") == 0)
                            {
                                param = false;
                            }
                            else if (param.toString().startsWith("\""))
                            {
                                inString = true;
                                aggregateString = param.toString();
                            }
                        }
                        else
                        {
                            aggregateString += " " + param.toString();
                            if (param.toString().endsWith("\""))
                            {
                                inString = false;
                            }
                        }

                    }
                }

            }
            catch (Exception e)
            {
                CLUtils.println(e.toString());
            }

            mbeanParams[i++] = param;
        }

        return mbeanParams;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return getUsage();
    }

    /**
     * Single line representation of parameters.
     */
    public String formatParams(Map<String, DynamicMBeanParam> params,
            boolean includeDescription)
    {
        return formatParams(params, includeDescription, false, null);
    }

    /**
     * @param multiLine Format parameters to multiple lines? More tidy but
     *            consumes more text space.
     * @param linePrefix String to use before every line. Used with
     *            multiLine=true.
     */
    private String formatParams(Map<String, DynamicMBeanParam> params,
            boolean includeDescription, boolean multiLine, String linePrefix)
    {
        StringBuilder builder = new StringBuilder();

        for (DynamicMBeanParam param : params.values())
        {
            if (builder.length() > 0)
            {
                if (multiLine)
                    builder.append("\n");
                else
                    builder.append(", ");
            }

            if (includeDescription)
            {
                if (multiLine)
                {
                    if (linePrefix != null)
                        builder.append(linePrefix);
                    builder.append(String.format("%-25s : %s", param,
                            param.getDescription()));
                }
                else
                    builder.append(String.format("%20s : %s", param,
                            param.getDescription()));
            }
            else
            {
                builder.append(String.format("%s", param));
            }
        }

        return builder.toString();
    }

    public String getParamDescription(boolean multiLine, String linePrefix)
    {
        return formatParams(params, true, multiLine, linePrefix);
    }

    /**
     * Returns the name value.
     * 
     * @return Returns the name.
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name value.
     * 
     * @param name The name to set.
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Returns the description value.
     * 
     * @return Returns the description.
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * Sets the description value.
     * 
     * @param description The description to set.
     */
    public void setDescription(String description)
    {
        this.description = description;
    }

    /**
     * Returns the usage value.
     * 
     * @return Returns the usage.
     */
    public String getUsage()
    {
        return usage;
    }

    /**
     * Sets the usage value.
     * 
     * @param usage The usage to set.
     */
    public void setUsage(String usage)
    {
        this.usage = usage;
    }

    /**
     * Sets the signature value.
     * 
     * @param signature The signature to set.
     */
    public void setSignature(Vector<String> signature)
    {
        this.signature = signature;
    }

    public String defaultUsage()
    {
        StringBuilder builder = new StringBuilder();

        builder.append(String.format("%s", getName()));

        if (params.size() == 0)
        {
            builder.append("()");
            return builder.toString();
        }

        builder.append("(");
        int paramCount = 0;
        for (DynamicMBeanParam param : params.values())
        {
            builder.append(String.format("%s %s", param.getName(), param
                    .getType().getSimpleName()));
            if (++paramCount < params.size())
            {
                builder.append(", ");
            }
        }

        builder.append(")");

        return builder.toString();

    }

}
