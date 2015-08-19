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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.common.exec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

/**
 * Wrapper class to encapsulate all aspects of executing native operating system
 * commands which are typically characterized . The wrapper handles up-front
 * collection of inputs such as setting the working directory, environment, and
 * stdin. It also manages the collection output from stdout and stderr.
 * <p>
 * Here is an example of typical usage:
 * <p>
 * <code><pre>
 * ProcessExecutor pe = new ProcessExecutor();
 * pe.setCommands(new String[] {"echo", "hi!"});
 * pe.setTimeout(1000);
 * pe.setEnv("myenvvar", "myvalue");
 * pe.run();
 * </pre></code> After the run() method completes it is safe for clients to
 * examine output values. The class is implemented as runnable to permit callers
 * to run it easily in another thread.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ProcessExecutor implements Runnable
{
    private static final Logger       logger             = Logger
                                                                 .getLogger(ProcessExecutor.class);

    /**
     * Maximum milliseconds to wait for output threads to complete after process
     * completion.
     */
    protected static final int        THREAD_WAIT_MILLIS = 30 * 1000;

    // Input values.
    protected HashMap<String, String> env                = new HashMap<String, String>();
    protected InputStream             stdin;
    protected File                    workDirectory;
    protected String[]                commands;
    protected int                     timeout;

    // Output values.
    protected boolean                 stdOutAppend;
    protected boolean                 stdErrAppend;
    protected File                    stdOutFile;
    protected File                    stdErrFile;
    protected String                  stdout;
    protected String                  stderr;
    protected Logger                  stdOutLogger;
    protected Logger                  stdErrLogger;
    protected Throwable               error;
    protected int                     exitValue;
    protected boolean                 timedout;
    protected boolean                 succeeded;
    protected Process                 process            = null;
    protected boolean                 redirectStdErr     = false;

    /**
     * Creates a new instance.
     */
    public ProcessExecutor()
    {
    }

    /**
     * Creates a new instance.
     */
    public ProcessExecutor(boolean redirectStdErr)
    {
        this.redirectStdErr = true;
    }

    /** Returns the program and command line argumetns. */
    public String[] getCommands()
    {
        return commands;
    }

    /** Sets the program and command line arguments. */
    public void setCommands(String[] commands)
    {
        this.commands = commands;
    }

    /** Sets an environmental variable. */
    public void setEnv(String name, String value)
    {
        env.put(name, value);
    }

    /** Returns the table of environmental variables. */
    public HashMap<String, String> getEnv()
    {
        return env;
    }

    /** Sets environmental variables to be used by this command. */
    public void setEnv(HashMap<String, String> env)
    {
        this.env = env;
    }

    /** Returns the inputstream fed to the process. */
    public InputStream getStdin()
    {
        return stdin;
    }

    /**
     * Sets the input stream fed to the process. The input stream is
     * automatically closed at the end of the command. Null is the default value
     * and means there is no stdin for this process.
     */
    public void setStdin(InputStream stdin)
    {
        this.stdin = stdin;
    }

    /**
     * Send stdout to a file
     * 
     * @param stdOutFile a file for stdout to be stored to
     */
    public void setStdOut(File stdOutFile)
    {
        this.stdOutFile = stdOutFile;
    }
    
    /**
     * Send stdout to a logger.
     * 
     * @param stdOutLogger an initialized Logger for stdout to be appended to.
     */
    public void setStdOut(Logger stdOutLogger)
    {
        this.stdOutLogger = stdOutLogger;
    }

    /**
     * Send stderr to a file
     * 
     * @param stdErrFile a file for stderr to be stored to
     */
    public void setStdErr(File stdErrFile)
    {
        this.stdErrFile = stdErrFile;
    }

    /**
     * Send stderr to a logger.
     * 
     * @param stdErrLogger an initialized Logger for stderr to be appended to.
     */
    public void setStdErr(Logger stdErrLogger)
    {
        this.stdErrLogger = stdErrLogger;
    }
    
    /**
     * Returns true if stdout should append to existing file. 
     */
    public boolean isStdOutAppend()
    {
        return stdOutAppend;
    }

    /**
     * If true, append stdout to existing file.  Otherwise, 
     * stdout overwrites the file if it exists. 
     */
    public void setStdOutAppend(boolean appendStdout)
    {
        this.stdOutAppend = appendStdout;
    }

    /**
     * Returns true if stderr should append to existing file. 
     */
    public boolean isStdErrAppend()
    {
        return stdErrAppend;
    }

    /**
     * If true, append stderr to existing file.  Otherwise, 
     * stderr overwrites the file if it exists. 
     */
    public void setStdErrAppend(boolean appendStderr)
    {
        this.stdErrAppend = appendStderr;
    }

    /**
     * Returns the process timeout in milliseconds.
     */
    public int getTimeout()
    {
        return timeout;
    }

    /**
     * Sets the process timeout in milliseconds. We consider the process failed
     * if it does not terminate within this time. A value of 0 is the default
     * and means to wait indefinitely.
     */
    public void setTimeout(int timeout)
    {
        this.timeout = timeout;
    }

    /**
     * Returns the process working directory.
     */
    public File getWorkDirectory()
    {
        return workDirectory;
    }

    /**
     * Sets the process working direcgtory. Null is the default and means to run
     * in the current directory of the Java process that launches this command.
     */
    public void setWorkDirectory(File workDirectory)
    {
        this.workDirectory = workDirectory;
    }

    /**
     * Returns the exception, if any, generated while executing the command.
     */
    public Throwable getError()
    {
        return error;
    }

    /**
     * Returns the exit value of the command or -1 if the command failed to
     * execute.
     */
    public int getExitValue()
    {
        return exitValue;
    }

    /**
     * Returns a String containing stderr output. An empty string means there is
     * no output.
     */
    public String getStderr()
    {
        return stderr;
    }

    /**
     * Returns stderr in the form of a String array where each string contains
     * one line of output.
     */
    public List<String> getStderrByLine()
    {
        return toStringList(stderr);
    }

    /**
     * Returns a String containing stdout. An empty string means there is no
     * output.
     */
    public String getStdout()
    {
        return stdout;
    }

    /**
     * Returns the stdout in the form of a String array where each string
     * contains one line of output.
     */
    public List<String> getStdoutByLine()
    {
        return toStringList(stdout);
    }

    /**
     * Returns true if this processed exceeded its timeout and was killed.
     */
    public boolean isTimedout()
    {
        return timedout;
    }

    /**
     * Returns true if we think the process succeeded based on the return code
     * and lack of exceptions or timeout during execution.
     */
    public boolean isSuccessful()
    {
        return succeeded;
    }

    /**
     * Execute the command. When this method returns it is safe for callers to
     * read output values.
     */
    public void run()
    {
        // Clear output values.
        stdout = "";
        stderr = "";
        exitValue = -1;
        error = null;
        timedout = false;
        succeeded = false;

        // Construct and run the process.
        try
        {
            // Create a process instance.
            ProcessBuilder pb = new ProcessBuilder(commands);
            Map<String, String> localEnv = pb.environment();
            for (String key : env.keySet())
            {
                localEnv.put(key, env.get(key));
            }
            pb.directory(workDirectory);
            pb.redirectErrorStream(redirectStdErr);
            process = pb.start();

            exitValue = handleProcessIO(process);
        }
        catch (InterruptedException e)
        {
            logger.warn("Command timed out: command="
                    + commandsToString(commands) + " timeout=" + timeout);
            timedout = true;
        }
        catch (IOException e)
        {
            logger.warn("Command failed with I/O error: command="
                    + commandsToString(commands));
            logger.debug("Command I/O exception: " + e);
            error = e;
        }
        catch (Throwable e)
        {
            logger.warn("Command failed with unexpected exception: command="
                    + commandsToString(commands), e);
            error = e;
        }

        // Figure out whether we succeeded or failed.
        if (this.error != null)
            succeeded = false;
        else if (this.exitValue != 0)
            succeeded = false;
        else if (this.timedout == true)
            succeeded = false;
        else
            succeeded = true;
    }

    // Concatenates an array of strings into a single space-separated string.
    private String commandsToString(String[] commands)
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < commands.length; i++)
        {
            if (i > 0)
                sb.append(" ");
            sb.append(commands[i]);
        }
        return sb.toString();
    }

    /**
     * Manage process input and output. This is messy so we put it in a separate
     * routine.
     */
    protected int handleProcessIO(Process process) throws InterruptedException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Starting execution of \""
                    + commandsToString(commands) + "\"");
        }

        // Manage command execution.
        InputStreamSink stdoutProcessor = null;
        InputStreamSink stderrProcessor = null;
        TimerTask task = null;
        try
        {
            // Use threads to capture process output.
            stdoutProcessor = getInputSink("stdout", process.getInputStream(),
                    stdOutFile, stdOutAppend, stdOutLogger);
            stderrProcessor = getInputSink("stderr", process.getErrorStream(),
                    stdErrFile, stdErrAppend, stdErrLogger);
            Thread stdoutThread = new Thread(stdoutProcessor);
            Thread stderrThread = new Thread(stderrProcessor);
            stdoutThread.start();
            stderrThread.start();

            // Schedule the timer if we need a timeout.
            if (timeout > 0)
            {
                final Thread t = Thread.currentThread();
                task = new TimerTask()
                {
                    public void run()
                    {
                        t.interrupt();
                    }
                };
                Timer timer = new Timer();
                timer.schedule(task, timeout);
            }

            // Copy in standard input if present.
            if (stdin != null)
            {
                OutputStream processStdin = process.getOutputStream();
                try
                {
                    // Write output from stream.
                    byte[] buff = new byte[1024];
                    int len = 0;
                    while ((len = stdin.read(buff)) != -1)
                    {
                        processStdin.write(buff, 0, len);
                    }
                }
                catch (IOException e)
                {
                    logger.warn("Writing of data to stdin halted by exception",
                            e);
                }
                finally
                {
                    try
                    {
                        stdin.close();
                    }
                    catch (IOException e)
                    {
                        logger
                                .warn(
                                        "Input stdin close operation generated exception",
                                        e);
                    }
                    try
                    {
                        processStdin.close();
                    }
                    catch (IOException e)
                    {
                        logger
                                .warn(
                                        "Process stdin close operation generated exception",
                                        e);
                    }
                }
            }

            // Wait for process to complete.
            process.waitFor();

            // Wait for threads to complete. Not strictly necessary
            // but makes it more likely we will read output properly.
            stdoutThread.join(THREAD_WAIT_MILLIS);
            stderrThread.join(THREAD_WAIT_MILLIS);
        }
        catch (FileNotFoundException e)
        {
            logger.debug("Unable to open file: " + e.getMessage(), e);
            throw new ProcessRuntimeException("Unable to open file " + e.getMessage(), e);
        }
        catch (InterruptedException e)
        {
            logger.warn("Command exceeded timeout: "
                    + commandsToString(commands));
            process.destroy();
            throw e;
        }
        finally
        {
            // Cancel the timer and cleanup process stdin.
            if (task != null)
                task.cancel();

            // Collect output--this needs to happen no matter what.
            stderr = stderrProcessor.getOutput();
            stdout = stdoutProcessor.getOutput();
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Command \"" + commandsToString(commands) + "\" "
                    + "terminated with exitcode " + process.exitValue());
        }

        return process.exitValue();
    }
    
    // Utility routine to allocate a proper input stream sink based on whether
    // input is capture in memory of within a file.
    private InputStreamSink getInputSink(String tag, InputStream inputStream,
            File outFile, boolean append, Logger outLogger)
            throws FileNotFoundException
    {
        if (outFile != null)
            return new FileInputStreamSink(tag, inputStream, outFile, append);
        else if (outLogger != null)
            return new LoggerInputStreamSink(tag, inputStream, outLogger);
        else
            return new StringInputStreamSink(tag, inputStream, 0);
    }


    // Utility routine to turn output in string form into a list where each
    // string represents a single line.
    public static List<String> toStringList(String output)
    {
        BufferedReader br = new BufferedReader(new StringReader(output));
        ArrayList<String> list = new ArrayList<String>();

        String line;
        try
        {
            while ((line = br.readLine()) != null)
            {
                list.add(line);
            }
        }
        catch (IOException e)
        {
            // This should not happen if we are reading from a String.
            logger
                    .warn("Converting string output to list resulted in error",
                            e);
        }
        return list;
    }

    /**
     * Returns the process value.
     * 
     * @return Returns the process.
     */
    public Process getProcess()
    {
        return process;
    }
}