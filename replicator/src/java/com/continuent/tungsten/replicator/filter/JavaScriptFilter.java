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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import java.io.*;

import org.apache.log4j.Logger;
import org.mozilla.javascript.*;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This filter allows to develop JavaScript filters without coding any Java at
 * all.<br/>
 * <br/>
 * User's script will be precompiled once and then called on every filtered
 * event.<br/>
 * <br/>
 * Script must define the following function:<br/>
 * function filter(event) - called on every filtered
 * {@link com.continuent.tungsten.replicator.event.ReplDBMSEvent}.<br/>
 * <br/>
 * filter(event) function's return value is handled as follows:<br/>
 * a. null and ReplDBMSEvent - passed through to the JavaScriptFilter's caller,<br/>
 * b. Undefined (no return statement in the JS) - ignored,<br/>
 * c. Everything else - logged into INFO stream.<br/>
 * <br/>
 * Further functions may be defined, but are optional:<br/>
 * function prepare() - called when this filter is being prepared.<br/>
 * function release() - called when this filter is being released.<br/>
 * <br/>
 * Any of these functions are free to return a value. If they do, value is
 * logged into trep.log INFO stream.<br/>
 * <br/>
 * Also there are the following variables exported via JavaScript reflection for
 * user's usage:<br/>
 * properties -
 * {@link com.continuent.tungsten.common.config.TungstenProperties} of the
 * current replicator instance;<br/>
 * filterProperties -
 * {@link com.continuent.tungsten.common.config.TungstenProperties} subset with
 * the current filter's properties (eg. "script");<br/>
 * logger - this classes {@link org.apache.log4j.Logger}. Eg. of usage:
 * <code>logger.info("I'm a script!");</code><br/>
 * thread - current {@link java.lang.Thread}. Eg. of usage: thread.sleep(1000);<br/>
 * <br/>
 * Note: if you wish to call more than one JS file, use multiple instances of
 * this filter with different names defined in replicator.properties
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */
public class JavaScriptFilter implements FilterManualProperties
{
    private static Logger      logger           = Logger
                                                        .getLogger(JavaScriptFilter.class);

    /**
     * Compiled user's script.
     */
    private Script             script           = null;

    /**
     * JavaScript scope containing all objects including functions of the user's
     * script and our exported objects.
     */
    private Scriptable         scope            = null;

    /**
     * Pointer to the script's filter function.
     */
    private Function           filterFunction   = null;

    /**
     * Path to a JS script file that this filter will be working on.
     */
    private String             scriptFile       = null;

    private String             configPrefix     = null;
    private TungstenProperties properties       = null;
    private TungstenProperties filterProperties = null;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.FilterManualProperties#setConfigPrefix(java.lang.String)
     */
    public void setConfigPrefix(String configPrefix)
    {
        this.configPrefix = configPrefix;
    }

    /**
     * Calls filter(event) function in user's script. Blocks until it returns.
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        // Call script if it was successfully prepared.
        if (filterFunction != null)
        {
            // We are in a method which might be called from a different thread
            // than the one that called the prepare() method. Thus we need to
            // enter JavaScript context.
            Context jsContext = ContextFactory.getGlobal().enterContext();

            // Provide access to current thread object.
            ScriptableObject.putProperty(scope, "thread", Thread
                    .currentThread());

            // Call function "filter(event)" and log its result if one was
            // returned.
            Object functionArgs[] = {event};
            Object result = filterFunction.call(jsContext, scope, scope,
                    functionArgs);

            // Exit JavaScript context.
            Context.exit();
            
            // Handle the return value.
            if (result == null)
                return null;
            else if (result instanceof ReplDBMSEvent)
                return (ReplDBMSEvent) result;
            else
                logIfDefined(result);
        }

        return event;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // Get all properties and a subset of only with this instance's
        // properties.
        properties = context.getReplicatorProperties();
        filterProperties = properties.subset(configPrefix + ".", true);

        // Determine which JS script to use.
        scriptFile = filterProperties.getString("script");
        if (scriptFile == null)
            throw new ReplicatorException(
                    "scriptFile property must be set for JavaScript filter to work");
    }

    /**
     * Reads and compiles user's script. Initializes function reflection
     * objects. Calls script's prepare() function if it exists. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Create JavaScript context which will be used for preparing script.
        Context jsContext = ContextFactory.getGlobal().enterContext();

        // Create script's scope.
        scope = jsContext.initStandardObjects();

        // Compile user's JavaScript files for future usage, so they wouldn't
        // require compilation on every filtered event.
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

            // Provide access to replicator's properties.
            ScriptableObject.putProperty(scope, "properties", properties);
            ScriptableObject.putProperty(scope, "filterProperties",
                    filterProperties);

            // Get a pointer to function "filter(event)".
            Object filterObj = scope.get("filter", scope);
            if (!(filterObj instanceof Function))
                logger.error("filter(event) is undefined in " + scriptFile);
            else
                filterFunction = (Function) filterObj;

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

    /**
     * Calls script's release() function if it exists. Closes JavaScript
     * context. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        if (scope != null)
        {
            try
            {
                // Enter JavaScript context.
                Context jsContext = ContextFactory.getGlobal().enterContext();

                // Get a pointer to function "release()" and call it.
                getFunctionAndCall(jsContext, "release");
            }
            catch (IllegalStateException e)
            {
                logger.debug("Exception while releasing: " + e);
            }
            finally
            {
                // Exit JavaScript context.
                Context.exit();
            }
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
}
