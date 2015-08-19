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
 * Initial developer(s): Linas Virbalas, Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.scripting;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.EvaluatorException;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.Undefined;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a generic script executor for Javascript and that encapsulates
 * Rhino integration with the replicator. It supplies a standard environment and
 * is designed to be reusable across multiple use cases, such as batch merge
 * scripts or filters.
 * 
 * @see com.continuent.tungsten.replicator.filter.JavaScriptFilter
 */
public class JavascriptExecutor implements ScriptExecutor
{
    private static Logger         logger                = Logger.getLogger(JavascriptExecutor.class);

    // Location of script.
    private String                scriptFile            = null;

    // Optional default data source name.
    private String                defaultDataSourceName = null;

    // Map of objects to be inserted into the script context.
    private Map<String, Object>   contextMap            = null;

    // Compiled user's script.
    private Script                script                = null;

    // JavaScript scope containing all objects including functions of the user's
    // script and our exported objects.
    private Scriptable            scope                 = null;

    // Pointer to the script function map.
    private Map<String, Function> functions             = new HashMap<String, Function>();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.scripting.ScriptExecutor#setScript(java.lang.String)
     */
    @Override
    public void setScript(String script)
    {
        this.scriptFile = script;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.scripting.ScriptExecutor#getScript()
     */
    public String getScript()
    {
        return this.scriptFile;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.scripting.ScriptExecutor#setContextMap(java.util.Map)
     */
    public void setContextMap(Map<String, Object> contextMap)
    {
        this.contextMap = contextMap;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.scripting.ScriptExecutor#setDefaultDataSourceName(java.lang.String)
     */
    public void setDefaultDataSourceName(String defaultDataSourceName)
    {
        this.defaultDataSourceName = defaultDataSourceName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Determine which JS script to use.
        if (scriptFile == null)
            throw new ReplicatorException(
                    "Missing name of Javascript file to execute");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Create JavaScript context which will be used for preparing script.
        Context jsContext = ContextFactory.getGlobal().enterContext();

        // Create script's scope.
        scope = jsContext.initStandardObjects();

        // Compile user's JavaScript file for future usage.
        try
        {
            // Read and compile the script.
            BufferedReader in = new BufferedReader(new FileReader(scriptFile));
            script = jsContext.compileReader(in, scriptFile, 0, null);
            in.close();

            // Execute script to get functions into scope.
            script.exec(jsContext, scope);

            // Provide access to the logger object.
            ScriptableObject.putProperty(scope, "logger", logger);

            // Provide access to a runtime to help run processes and other
            // useful things.
            ScriptableObject.putProperty(scope, "runtime",
                    new JavascriptRuntime(context, defaultDataSourceName));

            // If we have a context map available, load the objects contained
            // therein.
            if (contextMap != null)
            {
                for (String key : contextMap.keySet())
                {
                    Object value = contextMap.get(key);
                    ScriptableObject.putProperty(scope, key, value);
                }
            }

            // Get a pointer to function "prepare()" and call it.
            getFunctionAndCall(jsContext, "prepare");
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Script file not found: "
                    + scriptFile, e);
        }
        catch (EvaluatorException e)
        {
            throw new ReplicatorException(e);
        }
        finally
        {
            // Exit JavaScript context.
            Context.exit();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context)
    {
        try
        {
            // We are in a method which might be called from a different thread
            // than the one that called previous methods. Thus we need to
            // enter JavaScript context.
            Context jsContext = ContextFactory.getGlobal().enterContext();

            // Get a pointer to function "release()" and call it.
            getFunctionAndCall(jsContext, "release");
        }
        finally
        {
            // Exit JavaScript context.
            Context.exit();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.scripting.ScriptExecutor#register(java.lang.String)
     */
    public boolean register(String method)
    {
        Object filterObj = scope.get(method, scope);
        if (filterObj instanceof Function)
        {
            functions.put(method, (Function) filterObj);
            return true;
        }
        else
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Method not found: script=" + script + " method="
                        + method);
            }
            return false;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.scripting.ScriptExecutor#execute(java.lang.String,
     *      java.lang.Object)
     */
    public Object execute(String method, Object value)
            throws ReplicatorException
    {
        // Find our method. If not present, try to register it and look again.
        Function jsFunction = functions.get(method);
        if (jsFunction == null)
        {
            register(method);
            jsFunction = functions.get(method);
        }

        // Execute the method if we found it.
        if (jsFunction == null)
        {
            throw new ReplicatorException(
                    "Attempt to call unregistered or non-existent Javascript function: script="
                            + script + " function=" + method);
        }
        else
        {
            // We are in a method which might be called from a different thread
            // than the one that called the prepare() method. Thus we need to
            // enter JavaScript context.
            Context jsContext = ContextFactory.getGlobal().enterContext();

            // Provide access to current thread object.
            ScriptableObject.putProperty(scope, "thread",
                    Thread.currentThread());

            // Call function "filter(event)" and log its result if one was
            // returned.
            Object functionArgs[] = {value};
            Object returnValue = jsFunction.call(jsContext, scope, scope,
                    functionArgs);

            // Exit JavaScript context.
            Context.exit();

            // Return the value to caller. If it is a native Java object we must
            // unwrap it to return a Java value. Otherwise, we can safely return
            // the value itself.
            if (returnValue instanceof NativeJavaObject)
                return ((NativeJavaObject) returnValue).unwrap();
            else
                return returnValue;
        }
    }

    /**
     * Tries to get a pointer to a function in the script and call it. If
     * unsuccessful, logs a message into DEBUG stream about failure. If
     * successful and function returns a value, this method logs string
     * representation of the return value into INFO stream.
     * 
     * @param functionName Function name to get and call.
     * @return true, if function called, false, otherwise.
     */
    private boolean getFunctionAndCall(Context jsContext, String functionName)
    {
        Object fObj = scope.get(functionName, scope);
        if (!(fObj instanceof Function))
        {
            logger.debug(functionName + "() is undefined in " + scriptFile);
            return false;
        }
        else
        {
            Function f = (Function) fObj;
            Object result = f.call(jsContext, scope, scope, null);
            logIfDefined(result);
            return true;
        }
    }

    /**
     * Logs object's Context.toString(...) representation to INFO stream if it's
     * defined.
     * 
     * @param objToLog Object which string representation to log.
     * @return true, if logged, false, if object was undefined or null.
     */
    private boolean logIfDefined(Object objToLog)
    {
        if (objToLog != null && !(objToLog instanceof Undefined))
        {
            logger.info(Context.toString(objToLog));
            return true;
        }
        else
            return false;
    }
}