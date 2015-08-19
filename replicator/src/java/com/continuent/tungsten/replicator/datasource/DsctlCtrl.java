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

package com.continuent.tungsten.replicator.datasource;

import java.util.List;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.exec.ArgvIterator;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.ddlscan.DDLScanCtrl;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * This class defines a DsctlCtrl that implements a utility to access datasource
 * methods. See the printHelp() command for a description of current commands.
 * 
 * @author <a href="mailto:linas.virbalasn@vmware.com">Linas Virbalas</a>
 * @version 1.0
 */
public class DsctlCtrl
{
    private static Logger                    logger       = Logger.getLogger(DsctlCtrl.class);

    protected static ArgvIterator            argvIterator = null;
    protected static DataSourceAdministrator admin        = null;
    protected static String                  service      = null;
    protected static String                  datasource   = "global";

    public void prepare() throws ReplicatorException, InterruptedException
    {
    }

    public void release()
    {
    }

    /**
     * Main method to run utility.
     * 
     * @param argv optional command string
     */
    public static void main(String argv[])
    {
        int exitCode = 0;
        try
        {
            // Command line parameters and options.
            String configFile = null;
            String command = null;
            // Options for set command.
            Long seqno = null;
            Long epoch = null;
            String eventId = null;
            String sourceId = null;

            // Parse command line arguments.
            ArgvIterator argvIterator = new ArgvIterator(argv);
            if (!argvIterator.hasNext())
            {
                printHelp();
                succeed();
            }

            String curArg = null;
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-conf".equals(curArg))
                {
                    configFile = argvIterator.next();
                }
                else if ("-service".equals(curArg))
                {
                    service = argvIterator.next();
                }
                else if ("-ds".equals(curArg))
                {
                    datasource = argvIterator.next();
                }
                else if ("-seqno".equals(curArg))
                {
                    seqno = Long.parseLong(argvIterator.next());
                }
                else if ("-epoch".equals(curArg))
                {
                    epoch = Long.parseLong(argvIterator.next());
                }
                else if ("-event-id".equals(curArg))
                {
                    eventId = argvIterator.next();
                }
                else if ("-source-id".equals(curArg))
                {
                    sourceId = argvIterator.next();
                }
                else if (curArg.startsWith("-"))
                {
                    fatal("Unrecognized option: " + curArg, null, 1);
                }
                else
                {
                    command = curArg;
                }
            }

            if (command == null)
                fatal("No command entered: try 'help' to get a list", null, 1);
            else if (command.equals(Commands.HELP))
            {
                printHelp();
                succeed();
            }

            // Retrieve configuration file if user didn't specify one.
            if (configFile == null)
                configFile = getConfigFile();

            // Read configuration file.
            TungstenProperties properties = DDLScanCtrl.readConfig(configFile);
            if (properties == null)
            {
                fatal("Unable to read configuration file " + configFile, null,
                        2);
            }
            else
            {
                try
                {
                    // Initialize datasource administrator.
                    admin = new DataSourceAdministrator(properties);
                    admin.prepare();

                    // Parse user's command.
                    if (command.equals(Commands.RESET))
                        exitCode = doReset();
                    else if (command.equals(Commands.GET))
                        exitCode = doGet();
                    else if (command.equals(Commands.SET))
                    {
                        if (seqno == null || epoch == null || eventId == null
                                || sourceId == null)
                        {
                            fatal("Command 'set' requires options -seqno, -epoch, -event-id and -source-id",
                                    null, 1);
                        }
                        exitCode = doSet(seqno, epoch, eventId, sourceId);
                    }
                    else
                        fatal("Unrecognized command, try 'help' to get a list: "
                                + command, null, 1);
                }
                catch (ReplicatorException e)
                {
                    printlnerr(e.getMessage());
                    exitCode = 3;
                }
                finally
                {
                    if (admin != null)
                    {
                        admin.release();
                    }
                }
            }

        }
        catch (Exception e)
        {
            printlnerr(e.getMessage());
            e.printStackTrace();
            exitCode = 4;
        }
        System.exit(exitCode);
    }

    /**
     * Clear datasource state.
     */
    protected static int doReset() throws ReplicatorException,
            InterruptedException
    {
        boolean cleared = admin.reset(datasource);
        if (cleared)
        {
            String msg = "Service \"" + service + "\" datasource \""
                    + datasource + "\" catalog information cleared";
            logger.info(msg);
            println(msg);
            return 0;
        }
        else
        {
            printlnerr("FAILED to clear datasource \"" + datasource
                    + "\" information for service \"" + service + "\"");
            return 5;
        }
    }

    /**
     * Get datasource position.
     */
    @SuppressWarnings("unchecked")
    protected static int doGet() throws ReplicatorException,
            InterruptedException
    {
        List<ReplDBMSHeader> headers = admin.get(datasource);
        if (headers == null)
        {
            printlnerr("Position headers have not been retrieved");
            return 6;
        }
        else
        {
            JSONArray jsonArray = new JSONArray();
            for (ReplDBMSHeader header : headers)
            {

                JSONObject jsonHeader = new JSONObject();
                jsonHeader.put("applied_latency", header.getAppliedLatency());
                jsonHeader.put("epoch_number", header.getEpochNumber());
                jsonHeader.put("eventid", header.getEventId());
                jsonHeader.put("extract_timestamp",
                        toString(header.getExtractedTstamp()));
                jsonHeader.put("fragno", header.getFragno());
                jsonHeader.put("last_frag", header.getLastFrag());
                jsonHeader.put("seqno", header.getSeqno());
                jsonHeader.put("shard_id", header.getShardId());
                jsonHeader.put("source_id", header.getSourceId());
                jsonHeader.put("task_id", header.getTaskId());
                jsonHeader.put("update_timestamp",
                        toString(header.getUpdateTstamp()));
                jsonArray.add(jsonHeader);
            }
            String json = jsonArray.toJSONString();
            logger.info("Service \"" + service + "\" datasource \""
                    + datasource + "\" position retrieved:");
            logger.info(json);
            DsctlCtrl.println(json);
            return 0;
        }
    }

    /**
     * Transforms object to String, if it is not null.
     */
    protected static String toString(Object value)
    {
        if (value == null)
            return null;
        else
            return value.toString();
    }

    /**
     * Set datasource position.
     */
    protected static int doSet(long seqno, long epoch, String eventId,
            String sourceId) throws ReplicatorException, InterruptedException
    {
        if (admin.set(datasource, seqno, epoch, eventId, sourceId))
        {
            String msg = "Service \"" + service + "\" datasource \""
                    + datasource + "\" position was set to: seqno=" + seqno
                    + " epoch_number=" + epoch + " eventid=" + eventId
                    + " source_id=" + sourceId;
            logger.info(msg);
            println(msg);
            return 0;
        }
        else
        {
            String msg = "FAILED to set position of service \"" + service
                    + "\" datasource \"" + datasource + "\" to: seqno=" + seqno
                    + " epoch_number=" + epoch + " eventid=" + eventId
                    + " source_id=" + sourceId;
            printlnerr(msg);
            return 7;
        }
    }

    protected static String getConfigFile()
    {
        String configFile = null;
        if (service == null)
        {
            // Nothing is given. Retrieve configuration file of
            // default service.
            configFile = DDLScanCtrl.lookForConfigFile();
            if (configFile == null)
            {
                fatal("Replicator configuration file not found or there is more than one, check -service parameter",
                        null, 1);
            }
            else
            {
                // Parse service out of filename.
                service = DDLScanCtrl.serviceFromConfigFileName(configFile);
            }
        }
        else
        {
            // Retrieve configuration file of a given service.
            ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf
                    .getConfiguration(service);
            configFile = runtimeConf.getReplicatorProperties()
                    .getAbsolutePath();
        }
        return configFile;
    }

    protected static void printHelp()
    {
        println("Datasource Utility");
        println("Syntax: dsctl [conf|service] [-ds name] [operation]");
        println("Configuration (required if there's more than one service):");
        println("  -conf path       - Path to a static-<svc>.properties file");
        println("     OR            ");
        println("  -service name    - Name of a replication service to get datasource configuration from");
        println("Options:");
        println(" [-ds name]        - Name of the datasource (default: global)");
        // println(" [-log]            - If set, operations will be logged to a file");
        println("Operations:");
        println("  get              - Return all available position information");
        println("  set -seqno ###   - Set position (all four parameters required)");
        println("      -epoch ###");
        println("      -event-id AAAAAAAAA.######:#######");
        println("      -source-id AAA.AAA.AAA");
        println("  reset            - Clear datasource position information");
        println("  help             - Print this help display");
    }

    /**
     * Print a message to stdout with trailing new line character.
     * 
     * @param msg
     */
    protected static void println(String msg)
    {
        System.out.println(msg);
    }

    /**
     * Print a message to stderr with trailing new line character.
     * 
     * @param msg
     */
    protected static void printlnerr(String msg)
    {
        System.err.println(msg);
    }

    /**
     * Print a message to stdout without trailing new line character.
     * 
     * @param msg
     */
    protected static void print(String msg)
    {
        System.out.print(msg);
    }

    /**
     * Abort following a fatal error.
     * 
     * @param msg
     * @param t
     */
    protected static void fatal(String msg, Throwable t, int exitCode)
    {
        printlnerr(msg);
        if (t != null)
            t.printStackTrace();
        fail(exitCode);
    }

    /**
     * Exit with a process failure code.
     */
    protected static void fail(int exitCode)
    {
        System.exit(exitCode);
    }

    /**
     * Exit with a process success code.
     */
    protected static void succeed()
    {
        System.exit(0);
    }

    class Commands
    {
        public static final String GET   = "get";
        public static final String SET   = "set";
        public static final String HELP  = "help";
        public static final String RESET = "reset";
    }
}
