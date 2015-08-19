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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TraceVectorManager
{
    static private Logger                                   logger                        = Logger.getLogger(TraceVectorManager.class);

    private AtomicBoolean                                   enabled                    = new AtomicBoolean(
                                                                                               false);
    private AtomicBoolean                                   autoDisable                = new AtomicBoolean(
                                                                                               true);

    private Map<TraceVectorComponent, TraceVectorInstances> vectorsByComponent         = new TreeMap<TraceVectorComponent, TraceVectorInstances>();

    /**
     * Trace commands
     */
    public static final String                              TRACE_METHOD_TRACE            = "trace";
    public static final String                              TRACE_METHOD_TRACE_IS_ENABLED = "traceIsEnabled";
    public static final String                              TRACE_METHOD_GLOBAL_ENABLE    = "traceEnable";
    public static final String                              TRACE_METHOD_LIST             = "traceList";
    public static final String                              TRACE_METHOD_RESET            = "traceReset";
    public static final String                              TRACE_METHOD_AUTO_DISABLE     = "traceAutoDisable";

    /**
     * Trace command args
     */
    public static final String                              TRACE_ARG_VECTOR_PATH      = "vectorPath";
    public static final String                              TRACE_ARG_ENABLE_FLAG      = "enableFlag";

    public void clear()
    {
        vectorsByComponent.clear();
    }

    public String reset()
    {
        traceEnable(false);
        autoDisable.set(true);
        for (TraceVectorInstances instances : vectorsByComponent.values())
        {
            instances.reset();
        }

        return list();
    }

    private void register(TraceVectorComponent component)
    {
        TraceVectorInstances instances = vectorsByComponent.get(component);
        if (instances == null)
        {
            vectorsByComponent.put(component, new TraceVectorInstances(
                    component));
        }
    }

    public void add(TraceVectorComponent component, String category,
            int target, String description) throws Exception
    {
        TraceVectorInstances instances = vectorsByComponent.get(component);
        if (instances == null)
        {
            register(component);
            instances = vectorsByComponent.get(component);
        }

        TraceVector vector = instances.add(category, target, description);

        if (logger.isDebugEnabled())
            logger.debug("Added new vector: " + vector);

    }

    public void add(TraceVectorComponent component, Class<?> clazz, int target,
            String description) throws Exception
    {
        add(component, clazz.getSimpleName(), target, description);
    }

    public void traceEnable(boolean enableFlag)
    {
        enabled.set(enableFlag);
        logger.info("Global tracing is now "
                + (enableFlag ? "enabled" : "disabled"));
    }

    public void traceAutoDisable(boolean enableFlag)
    {
        autoDisable.set(enableFlag);
        logger.info("Automatic disable of trace vectors is now "
                + (enableFlag ? "enabled" : "disabled"));
    }

    public boolean isEnabled(TraceVectorComponent component, Class<?> clazz,
            int target)
    {
        return isEnabled(component, clazz.getSimpleName(), target);
    }

    public boolean isEnabled(TraceVectorComponent component, String category,
            int target)
    {
        if (enabled.get() == false)
            return false;

        TraceVectorInstances instances = vectorsByComponent.get(component);
        if (instances == null)
        {
            return false;
        }

        boolean isEnabled = instances.isEnabled(category, target);

        if (isEnabled)
        {
            CLUtils.println(String.format("VECTOR FIRING %s: %s",
                    autoDisable.get() ? "ONCE" : "ALWAYS",
                    instances.getTrace(category, target)));

            if (autoDisable.get() == true)
            {
                instances.enable(category, target, false);
            }

        }

        return isEnabled;
    }

    public boolean isEnabled(String vectorPath) throws Exception
    {
        return isEnabled(TraceVectorArgs.getArgs(vectorPath));
    }

    private boolean isEnabled(TraceVectorArgs args) throws Exception
    {
        return isEnabled(args.getComponent(), args.getCategory(),
                args.getTarget());
    }

    private TraceVector enable(TraceVectorComponent component, String category,
            int target, boolean enableFlag)
    {
        TraceVectorInstances instances = vectorsByComponent.get(component);
        if (instances == null)
        {
            return new TraceVector();
        }

        TraceVector vector = instances.enable(category, target, enableFlag);
        logger.info(String.format("SET %s", vector));

        return vector;
    }

    public String enable(String vectorPath, boolean enableFlag)
            throws Exception
    {
        TraceVector vector = enable(TraceVectorArgs.getArgs(vectorPath),
                enableFlag);

        return String.format("%s\n%s", describe(), vector.toString());
    }

    private TraceVector enable(TraceVectorArgs args, boolean enableFlag)
            throws Exception
    {
        return enable(args.getComponent(), args.getCategory(),
                args.getTarget(), enableFlag);
    }

    public void set(TraceVectorComponent component, String category, int target)
            throws Exception
    {
        enable(component, category, target, true);
    }

    public void set(String vectorPath) throws Exception
    {
        enable(vectorPath, true);
    }

    public void clear(String vectorPath) throws Exception
    {
        enable(vectorPath, false);
    }

    public void clear(TraceVectorComponent component, String category,
            int target) throws Exception
    {
        enable(component, category, target, false);
    }

    public TraceVector getTrace(TraceVectorComponent component,
            String category, int target)
    {
        TraceVectorInstances instances = vectorsByComponent.get(component);
        if (instances == null)
        {
            return new TraceVector();
        }

        return instances.getTrace(category, target);

    }

    public String describe()
    {
        return (String.format("VECTOR MANAGER(%s)\nAUTO DISABLE(%s)",
                enabled.get() ? "ON" : "OFF", autoDisable.get() ? "ON" : "OFF"));
    }

    public String list()
    {
        return list(true, true);
    }

    public String list(boolean listEnabled, boolean listDisabled)
    {
        StringBuilder builder = new StringBuilder();

        builder.append(describe()).append("\n");

        for (TraceVectorComponent component : TraceVectorComponent.values())
        {
            TraceVectorInstances instances = vectorsByComponent.get(component);
            if (instances == null)
            {
                continue;
            }

            builder.append(instances.list(listEnabled, listDisabled));
        }
        return builder.toString();
    }

    public void setLogLevel(Level level)
    {
        logger.setLevel(level);
    }

    public boolean parseBoolean(String boolVal)
    {
        if (boolVal.equalsIgnoreCase("on") || boolVal.equalsIgnoreCase("true"))
            return true;

        return false;
    }

    /**
     * This method populates the local trace vector manager by using the
     * TraceVectorArgDesc annotations that it finds on a given class
     * 
     * @param clazz annotated class
     */
    public void importVectors(Class<?> clazz)
    {
        Field fields[] = clazz.getDeclaredFields();

        for (int i = 0; i < fields.length; i++)
        {
            Field field = fields[i];

            Annotation annotation = field
                    .getAnnotation(TraceVectorArgDesc.class);

            if (annotation == null)
                continue;

            TraceVectorArgDesc argDesc = (TraceVectorArgDesc) annotation;

            try
            {
                int target = field.getInt(field);
                add(argDesc.component(), argDesc.category(), target,
                        argDesc.description());
            }
            catch (Exception e)
            {
                logger.warn(String.format(
                        "Failed to add vector for field '%s'", field.getName()));
            }

        }
    }

    public String getVectorDescription(Class<?> clazz,
            TraceVectorComponent component, String category, int target)
    {
        Field fields[] = clazz.getDeclaredFields();

        for (int i = 0; i < fields.length; i++)
        {
            Field field = fields[i];

            Annotation annotation = field
                    .getAnnotation(TraceVectorArgDesc.class);

            if (annotation == null)
                continue;

            TraceVectorArgDesc argDesc = (TraceVectorArgDesc) annotation;

            int annotationTarget = -1;
            try
            {
                target = field.getInt(field);
            }
            catch (Exception e)
            {
                return String.format(String.format("TRACE VECTOR: %s/%s/%s",
                        component, category, target));
            }

            if (argDesc.component() == component
                    && argDesc.category().equalsIgnoreCase(category)
                    && annotationTarget == target)
            {
                return argDesc.description();
            }
        }

        return String.format(String.format("TRACE VECTOR: %s/%s/%s", component,
                category, target));
    }

}
