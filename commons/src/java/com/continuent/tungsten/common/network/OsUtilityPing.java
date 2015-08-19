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

package com.continuent.tungsten.common.network;

import java.net.InetAddress;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.exec.ProcessExecutor;

/**
 * Ping host using OS ping utility.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class OsUtilityPing implements PingMethod
{
    private static Logger          logger = Logger.getLogger(OsUtilityPing.class);
    private String                 notes;
    private static String          MAC    = "mac";
    private static String          LINUX  = "linux";
    private static String          OTHER  = "other";
    private static volatile String operatingSystem;

    /**
     * Tests a host for reachability.
     * 
     * @param address Host name
     * @param timeout Timeout in milliseconds
     * @return True if host is reachable, otherwise false.
     */
    public boolean ping(HostAddress address, int timeout) throws HostException
    {
        return doPing(address.getInetAddress(), timeout);
    }

    // Shared routine to issue ping call.
    private boolean doPing(InetAddress inetAddress, int timeout)
    {
        // Determine OS type if necessary.
        if (operatingSystem == null)
        {
            String os = System.getProperty("os.name").toLowerCase();
            if (os.indexOf("mac") > -1)
                operatingSystem = MAC;
            else if (os.indexOf("linux") > -1)
                operatingSystem = LINUX;
            else
                operatingSystem = OTHER;
        }

        // Use different ping form depending on operating system type.
        String[] pingArray;
        if (MAC.equals(operatingSystem))
        {
            // Mac OS X uses -W with milliseconds for timeouts.
            pingArray = new String[]{"ping", "-c", "1", "-W",
                    new Integer(timeout).toString(),
                    inetAddress.getHostAddress()};
        }
        else
        {
            // Linux uses -w with seconds for timeouts.
            pingArray = new String[]{"ping", "-c", "1", "-w",
                    new Integer(timeout / 1000).toString(),
                    inetAddress.getHostAddress()};
        }

        // Configure process executor. The command timeout is extra long to
        // let ping timeout by itself.
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(pingArray);
        pe.setTimeout(timeout + 3000);

        // Record command in printable form.
        StringBuffer sb = new StringBuffer();
        for (String part : pe.getCommands())
        {
            if (sb.length() > 0)
                sb.append(" ");
            sb.append(part);
        }
        String command = sb.toString();

        // Put the command in the notes.
        notes = command;

        // Run the said command.
        pe.run();

        // Print debug output.
        if (logger.isDebugEnabled())
        {
            logger.debug("Ping command: " + command);
            logger.debug("Ping command exit value: " + pe.getExitValue());
            String stdout = pe.getStdout();
            if (stdout.length() > 0)
                logger.debug("Ping command stdout: " + stdout);
            String stderr = pe.getStderr();
            if (stderr.length() > 0)
                logger.debug("Ping command stderr: " + stderr);
        }

        // See if we succeeded.
        if (pe.isSuccessful())
        {
            return true;
        }
        else if (pe.isTimedout())
        {
            logger.debug("Ping timed out: cmd=" + command + " timeout="
                    + timeout);
            return false;
        }
        else
        {
            logger.debug("Ping failed: cmd=" + command + " exit code="
                    + pe.getExitValue());
            return false;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.network.PingMethod#getNotes()
     */
    public String getNotes()
    {
        return notes;
    }
}