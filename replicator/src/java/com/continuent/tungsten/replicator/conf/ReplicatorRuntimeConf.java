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

package com.continuent.tungsten.replicator.conf;

import java.io.File;

import com.continuent.tungsten.common.jmx.ServerRuntimeException;

/**
 * This class defines configuration values that are set at runtime through
 * system properties and provides convenient access to the same.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ReplicatorRuntimeConf
{
    /** Path to replicator release */
    static public final String HOME_DIR                 = "replicator.home.dir";
    /** Path to replicator log directory */
    static public final String LOG_DIR                  = "replicator.log.dir";
    /** Path to replicator conf directory */
    static public final String CONF_DIR                 = "replicator.conf.dir";
    /** Option to clear dynamic properties */
    static public final String CLEAR_DYNAMIC_PROPERTIES = "replicator.clear.dynamic.proeprties";

    // Static variables.
    private static File        replicatorHomeDir;
    private static File        replicatorLogDir;
    private static File        replicatorConfDir;

    private final File         replicatorProperties;
    private final File         replicatorDynamicProperties;
    private final File         replicatorDynamicRole;
    private final boolean      clearDynamicProperties;

    /** Creates a new instance. */
    private ReplicatorRuntimeConf(String serviceName)
    {
        // Configure directory locations.
        replicatorHomeDir = locateReplicatorHomeDir();
        replicatorLogDir = locateReplicatorLogDir();
        replicatorConfDir = locateReplicatorConfDir();

        // Configure location of replicator.properties file.
        replicatorProperties = new File(locateReplicatorConfDir(), "static-"
                + serviceName + ".properties");

        if (!replicatorProperties.isFile() || !replicatorProperties.canRead())
        {
            throw new ServerRuntimeException(
                    "Replicator static properties does not exist or is invalid: "
                            + replicatorProperties);
        }

        // Configure location of replicator dynamic properties file.
        replicatorDynamicProperties = new File(replicatorConfDir, "dynamic-"
                + serviceName + ".properties");

        // Configure location of replicator online role file.
        replicatorDynamicRole = new File(replicatorConfDir, "dynamic-"
                + serviceName + ".role");

        // Determine whether we want to clear dynamic properties at start-up.
        this.clearDynamicProperties = Boolean.parseBoolean(System
                .getProperty(CLEAR_DYNAMIC_PROPERTIES));
    }

    /**
     * Returns a configured replication runtime or throws an exception if
     * configuration fails.
     */
    public static ReplicatorRuntimeConf getConfiguration(String serviceName)
    {
        return new ReplicatorRuntimeConf(serviceName);
    }

    public File getReplicatorHomeDir()
    {
        return replicatorHomeDir;
    }

    public File getReplicatorConfDir()
    {
        return replicatorConfDir;
    }

    public File getReplicatorLogDir()
    {
        return replicatorLogDir;
    }

    public File getReplicatorProperties()
    {
        return replicatorProperties;
    }

    public File getReplicatorDynamicProperties()
    {
        return replicatorDynamicProperties;
    }

    public File getReplicatorDynamicRole()
    {
        return replicatorDynamicRole;
    }

    public boolean getClearDynamicProperties()
    {
        return clearDynamicProperties;
    }

    /**
     * Find and return the replicator home directory.
     */
    public static File locateReplicatorHomeDir()
    {
        if (replicatorHomeDir == null)
        {
            // Configure replicator home.
            String replicatorHome = System.getProperty(HOME_DIR);
            if (replicatorHome == null)
                replicatorHome = System.getProperty("user.dir");
            replicatorHomeDir = new File(replicatorHome);
            if (!replicatorHomeDir.isDirectory())
            {
                throw new ServerRuntimeException(
                        "Replicator home does not exist or is invalid: "
                                + replicatorHomeDir);
            }
        }
        return replicatorHomeDir;
    }

    /**
     * Find and return the replicator log directory.
     */
    public static File locateReplicatorLogDir()
    {
        if (replicatorLogDir == null)
        {
            // Configure replicator log directory.
            String replicatorLog = System.getProperty(LOG_DIR);
            if (replicatorLog == null)
                replicatorLogDir = new File(locateReplicatorHomeDir(), "log");
            else
                replicatorLogDir = new File(replicatorLog);
            if (!replicatorLogDir.isDirectory())
            {
                throw new ServerRuntimeException(
                        "Replicator log directory does not exist or is invalid: "
                                + replicatorLogDir);
            }
        }
        return replicatorLogDir;
    }

    /**
     * Locate and return the replicator conf director.
     */
    public static File locateReplicatorConfDir()
    {
        if (replicatorConfDir == null)
        {
            // Configure replicator conf directory.
            String replicatorConf = System.getProperty(CONF_DIR);
            if (replicatorConf == null)
                replicatorConfDir = new File(locateReplicatorHomeDir(), "conf");
            else
                replicatorConfDir = new File(replicatorConf);
            if (!replicatorConfDir.isDirectory())
            {
                throw new ServerRuntimeException(
                        "Replicator conf directory does not exist or is invalid: "
                                + replicatorConfDir);
            }
        }
        return replicatorConfDir;
    }

    /**
     * Locate and return the replicator role file, returning null if the file
     * configuration file does not exist. This call avoids exceptions if the
     * configuration directory location cannot be found.
     */
    public static File locateReplicatorRoleFile(String serviceName)
    {
        // First, find the replicator configuration directory.
        File confDir = null;
        if (replicatorConfDir != null)
        {
            confDir = replicatorConfDir;
        }
        else if (System.getProperty(CONF_DIR) != null)
        {
            confDir = new File(System.getProperty(CONF_DIR));
        }
        else if (System.getProperty(HOME_DIR) != null)
        {
            File homeDir = new File(System.getProperty(HOME_DIR));
            confDir = new File(homeDir, "conf");
        }

        // If we cannot find the directory specification or it
        // does not exist, return a null. Otherwise return a
        // path for the role file.
        if (confDir == null || !confDir.isDirectory())
        {
            return null;
        }
        else
        {
            return new File(confDir, "dynamic-" + serviceName + ".role");
        }
    }
}