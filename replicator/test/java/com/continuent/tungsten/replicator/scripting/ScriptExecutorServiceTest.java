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

package com.continuent.tungsten.replicator.scripting;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * Tests background script method invocation using the ScriptExecutorService.
 */
public class ScriptExecutorServiceTest
{
    private static Logger logger = Logger.getLogger(ScriptExecutorServiceTest.class);

    /**
     * Verify that we can invoke a single method call using the script executor
     * service and receive the output. This case checks all types output to
     * ensure we catch any loose ends in basic processing and reporting.
     */
    @Test
    public void testScriptExecSimple() throws Exception
    {
        // Put together a script executor and a method invocation request.
        String name = "testSimpleServiceInvocation";
        String script = "function echo(argument) { return argument}";
        ScriptExecutor exec = prepareExecutor(name, script);
        ScriptMethodRequest request = new ScriptMethodRequest("echo", "hello!");

        // Create the execution service and send the request to it.
        List<ScriptExecutor> execList = new ArrayList<ScriptExecutor>();
        execList.add(exec);
        ScriptExecutorService svc = new ScriptExecutorService(name, execList, 1);
        svc.addRequest(request);
        boolean success = svc.process();

        // Ensure that all is well and we got the expected output.
        Assert.assertTrue("Checking service processing status", success);
        Assert.assertEquals("Checking invocation count", 1,
                svc.getMethodInvocationCount());
        List<ScriptExecutorTaskStatus> statusList = svc.getTaskStatusList();

        // Status should be success with no exception.
        Assert.assertEquals("Checking task status count", 1, statusList.size());
        ScriptExecutorTaskStatus status = statusList.get(0);
        Assert.assertEquals("Ensure task success", true, status.isSuccessful());
        Assert.assertNull("Ensuring no failed response",
                status.getFailedResponse());

        // Get the responses and ensure we have a single answer that is correct.
        List<ScriptMethodResponse> responses = svc.getMethodResponses();
        Assert.assertEquals("Checking response list count", 1, responses.size());
        ScriptMethodResponse response = responses.get(0);
        Assert.assertNotNull("Ensure request is present", response.getRequest());
        Assert.assertEquals("Ensure request argument", "hello!", response
                .getRequest().getArgument());
        Assert.assertEquals("Ensure response success", true,
                response.isSuccessful());
        Assert.assertEquals("Ensure correct output", "hello!",
                response.getValue());
        Assert.assertNull("Ensuring no exception", response.getThrowable());
    }

    /**
     * Verify that if the script fails we get an error properly reported back
     * both in the task executor status as well as a corresponding request to
     * the method.
     */
    @Test
    public void testScriptExecFailure() throws Exception
    {
        // Put together a script executor and a method invocation request.
        String name = "testScriptExecFailure";
        String script = "function echo1(argument) { runtime.exec('Bad command!!!'); }";
        ScriptExecutor exec = prepareExecutor(name, script);
        ScriptMethodRequest request = new ScriptMethodRequest("echo1",
                "should not get this");

        // Create the execution service and send the request to it.
        List<ScriptExecutor> execList = new ArrayList<ScriptExecutor>();
        execList.add(exec);
        ScriptExecutorService svc = new ScriptExecutorService(name, execList, 1);
        svc.addRequest(request);
        logger.info("Expecting a stack trace due to the exec failure; it's part of the test");
        boolean success = svc.process();

        // Ensure that all is well and we got the expected output.
        Assert.assertFalse("Checking service processing status", success);
        Assert.assertEquals("Checking invocation count", 0,
                svc.getMethodInvocationCount());
        List<ScriptExecutorTaskStatus> statusList = svc.getTaskStatusList();

        // Status should be failure with exception.
        Assert.assertEquals("Checking task status count", 1, statusList.size());
        ScriptExecutorTaskStatus status = statusList.get(0);
        Assert.assertEquals("Ensure task failure", false, status.isSuccessful());
        Assert.assertNotNull("Ensuring no response", status.getFailedResponse());

        // Get the responses and ensure we have 1 failed response.
        List<ScriptMethodResponse> responses = svc.getMethodResponses();
        Assert.assertEquals("Checking response list count", 1, responses.size());
        ScriptMethodResponse response = responses.get(0);
        Assert.assertNotNull("Ensure request is present", response.getRequest());
        Assert.assertEquals("Ensure response failure", false,
                response.isSuccessful());
        Assert.assertNull("Ensure correct no output", response.getValue());
        Assert.assertNotNull("Ensuring exception", response.getThrowable());
    }

    /**
     * Verify that we can invoke script calls in parallel using multiple script
     * executors running in the background. This case requires multiple cores to
     * execute efficiently.
     */
    @Test
    public void testScriptExecParallel() throws Exception
    {
        // Put together a st of script executors.
        String name = "testScriptExecParallel";
        String script = "function echo2(argument) { return argument}";
        List<ScriptExecutor> executors = new ArrayList<ScriptExecutor>(4);
        for (int i = 0; i < 4; i++)
        {
            ScriptExecutor exec = prepareExecutor(name, script);
            executors.add(exec);
        }

        // Create the execution service and send 20 requests to it.
        ScriptExecutorService svc = new ScriptExecutorService(name, executors,
                20);
        for (int i = 0; i < 20; i++)
        {
            ScriptMethodRequest request = new ScriptMethodRequest("echo2",
                    "arg-" + i);
            svc.addRequest(request);
        }
        boolean success = svc.process();

        // Ensure that all is well and we got the expected output.
        Assert.assertTrue("Checking service processing status", success);
        Assert.assertEquals("Checking invocation count", 20,
                svc.getMethodInvocationCount());
        List<ScriptExecutorTaskStatus> statusList = svc.getTaskStatusList();

        // Status should be success with no exception.
        Assert.assertEquals("Checking task status count", 4, statusList.size());
        for (ScriptExecutorTaskStatus status : statusList)
        {
            Assert.assertEquals("Ensure task success", true,
                    status.isSuccessful());
            Assert.assertNull("Ensuring no failed response",
                    status.getFailedResponse());
        }

        // Get the responses and ensure we have 20 different output values.
        List<ScriptMethodResponse> responses = svc.getMethodResponses();
        Map<String, Boolean> outputs = new HashMap<String, Boolean>();
        for (ScriptMethodResponse response : responses)
        {
            outputs.put(response.getValue().toString(), true);
        }
        Assert.assertEquals("Ensure correct number of distinct responses", 20,
                outputs.size());
    }

    // Create a context and execute script using default data source name.
    private ScriptExecutor prepareExecutor(String name, String script)
            throws Exception
    {
        return prepareExecutor(name, script, null);
    }

    // Create and return an executor for a script.
    private ScriptExecutor prepareExecutor(String name, String script,
            String dsName) throws Exception
    {
        File scriptFile = writeScript(name, script);
        JavascriptExecutor exec = new JavascriptExecutor();
        exec.setScript(scriptFile.getAbsolutePath());
        exec.setDefaultDataSourceName(dsName);
        exec.prepare(null);
        return exec;
    }

    // Write script.
    public File writeScript(String name, String script) throws IOException
    {
        File testDir = new File("testJavascriptExecutorService");
        testDir.mkdirs();
        File scriptFile = new File(testDir, name);
        FileWriter fw = new FileWriter(scriptFile);
        fw.write(script);
        fw.flush();
        fw.close();
        return scriptFile;
    }
}