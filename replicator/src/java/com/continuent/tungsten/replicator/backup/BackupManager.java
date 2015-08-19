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

package com.continuent.tungsten.replicator.backup;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.PropertyException;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.fsm.event.EventDispatcher;

/**
 * Implements a manager that tracks all backup agents.
 * 
 * @author <a href="mailto:jussi-pekka.kurikka@continuent.com">Jussi-Pekka
 *         Kurikka</a>
 * @version 1.0
 */
public class BackupManager
{
    private static Logger                 logger          = Logger
                                                                  .getLogger(BackupManager.class);

    // Property names
    public static String                  BACKUP_AGENTS   = "replicator.backup.agents";
    public static String                  BACKUP_AGENT    = "replicator.backup.agent";
    public static String                  BACKUP_DEFAULT  = "replicator.backup.default";
    public static String                  STORAGE_AGENTS  = "replicator.storage.agents";
    public static String                  STORAGE_AGENT   = "replicator.storage.agent";
    public static String                  STORAGE_DEFAULT = "replicator.storage.default";

    // Backup agent information
    private HashMap<String, BackupAgent>  backupAgents    = new HashMap<String, BackupAgent>();
    private HashMap<String, StorageAgent> storageAgents   = new HashMap<String, StorageAgent>();
    private String                        backupDefaultName;
    private String                        storageDefaultName;
    boolean                               enabled         = false;

    // Event dispatcher to log events.
    private final EventDispatcher         eventDispatcher;

    // Thread pool for backup/restore tasks.
    private static final ExecutorService  taskService     = Executors
                                                                  .newFixedThreadPool(10);

    public BackupManager(EventDispatcher eventDispatcher)
    {
        this.eventDispatcher = eventDispatcher;
    }

    /**
     * Initialize the backup manager. This loads and configures all agents.
     * 
     * @param properties Replicator properties
     */
    public void initialize(TungstenProperties properties)
            throws BackupException
    {
        // Find and load backup agents
        List<String> backupNames = properties.getStringList(BACKUP_AGENTS);
        for (String backupName : backupNames)
        {
            BackupAgent agent = (BackupAgent) loadAndConfigure(BACKUP_AGENT,
                    backupName, properties);
            backupAgents.put(backupName, agent);
        }

        // Ensure default agent can be found.
        backupDefaultName = properties.getString(BACKUP_DEFAULT);
        if (backupDefaultName == null)
        {
            logger
                    .warn("No default backup agent name provided; backups must explicitly select an agent");
        }
        else if (backupAgents.get(backupDefaultName) == null)
        {
            throw new BackupException(
                    "Default backup agent name does not exist: "
                            + backupDefaultName);
        }
        else
        {
            logger.info("Default backup agent set: name=" + backupDefaultName);
        }

        // Find and load storage agents
        List<String> storageNames = properties.getStringList(STORAGE_AGENTS);
        for (String storageName : storageNames)
        {
            StorageAgent agent = (StorageAgent) loadAndConfigure(STORAGE_AGENT,
                    storageName, properties);
            storageAgents.put(storageName, agent);
        }

        // Ensure default storage agent can be found.
        storageDefaultName = properties.getString(STORAGE_DEFAULT);
        if (storageDefaultName == null)
        {
            logger
                    .warn("No default storage agent name provided; storages must explicitly select an agent");
        }
        else if (storageAgents.get(storageDefaultName) == null)
        {
            throw new BackupException(
                    "Default storage agent name does not exist: "
                            + storageDefaultName);
        }
        else
        {
            logger
                    .info("Default storage agent set: name="
                            + storageDefaultName);
        }

        // Finally see if we have at least one backup and one storage agent.
        if (backupAgents.size() == 0)
        {
            logger.warn("No backup agents configured; backups are disabled");
        }
        else if (storageAgents.size() == 0)
        {
            logger.warn("No storage agents configured; backups are disabled");
        }
        else
        {
            enabled = true;
            logger.info("Backups are now enabled");
        }
    }

    /**
     * Returns true if backups are enabled.
     */
    public boolean isBackupEnabled()
    {
        return enabled;
    }

    /**
     * Run backup and store output.
     * 
     * @param backupAgentName Name of backup type or null for default
     * @param storageAgentName Name of storage type or null for default
     * @return Future that provides URI of backup when task finishes
     */
    // @SuppressWarnings("unchecked")
    public Future<String> spawnBackup(String backupAgentName,
            String storageAgentName, boolean isOnline) throws BackupException, UnsupportedCapabilityException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Processing backup request: backup=" + backupAgentName
                    + " storage=" + storageAgentName);
        }
        assertEnabled();

        // Resolve backup agent name.
        backupAgentName = resolveBackupAgentName(backupAgentName);

        // Find backup and storage agents.
        BackupAgent backupAgent = findBackupAgent(backupAgentName);
        StorageAgent storageAgent = findStorageAgent(storageAgentName);

        // If we are online, make sure the back up allows online backup.
        if (isOnline)
        {
            BackupCapabilities caps = backupAgent.capabilities();
            if (!caps.isHotBackupEnabled())
            {
                throw new UnsupportedCapabilityException(
                        "Online backup not supported for agent type "
                                + backupAgentName);
            }
        }

        // Start task.
        BackupTask callable = new BackupTask(eventDispatcher, backupAgentName,
                backupAgent, storageAgent);
        Future<String> task = taskService.submit(callable);
        return task;
    }

    /**
     * Retrieve and restore a previous backup.
     * 
     * @param uri URI to locate backup or null to get latest backup from default
     *            storage
     */
    public Future<String> spawnRestore(String uri) throws BackupException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Processing restore request: uri=" + uri);
        }
        assertEnabled();

        // Locate the corresponding backup, if it exists.
        StorageAgent storageAgent = null;
        URI realUri = null;
        StorageSpecification storageSpec = null;
        if (uri == null)
        {
            // Get the latest backup from the default storage agent.
            storageAgent = findStorageAgent(null);
            realUri = storageAgent.last();
            if (realUri == null)
            {
                throw new BackupException(
                        "Default storage agent does not have a most recent backup");
            }
            storageSpec = storageAgent.getSpecification(realUri);
        }
        else
        {
            // Convert the URI and select the first storage agent that has it.
            try
            {
                realUri = new URI(uri);
            }
            catch (URISyntaxException e)
            {
                throw new BackupException("Invalid URI format: uri=" + uri
                        + " message=" + e.getMessage(), e);
            }
            for (String agentName : this.storageAgents.keySet())
            {
                storageAgent = findStorageAgent(agentName);
                storageSpec = storageAgent.getSpecification(realUri);
                if (storageSpec != null)
                    break;
            }
        }

        // If we did not find the backup yet, the URI must be bad.
        if (storageSpec == null)
        {
            throw new BackupException("Backup not found: " + uri);
        }

        // Locate the backup agent.
        String backupAgentName = storageSpec.getAgent();
        BackupAgent backupAgent = this.findBackupAgent(backupAgentName);

        // Spawn the restore task.
        RestoreTask callable = new RestoreTask(realUri, eventDispatcher,
                backupAgent, storageAgent);
        Future<String> task = taskService.submit(callable);
        return task;
    }

    /**
     * Release all backup agents and decommission the backup manager.
     */
    public void release()
    {
    }

    // Ensure backups are enabled.
    private void assertEnabled() throws BackupException
    {
        if (!enabled)
            throw new BackupException(
                    "Backups are disabled; ensure backup agent and storage are defined");
    }

    // Load a backup plugin.
    protected BackupPlugin loadAndConfigure(String prefix, String name,
            TungstenProperties properties) throws BackupException
    {
        String pluginPrefix = prefix + "." + name.trim();

        // Find the plug-in class name.
        String rawClassName = properties.getString(pluginPrefix);
        if (rawClassName == null)
            throw new BackupException(
                    "Backup plugin class name property is missing or null:  key="
                            + pluginPrefix);
        String pluginClassName = rawClassName.trim();
        logger.info("Loading backup plugin: key=" + pluginPrefix
                + " class name=" + pluginClassName);

        // Subset backup plug-in properties.
        TungstenProperties pluginProperties = properties.subset(pluginPrefix
                + ".", true);
        if (logger.isDebugEnabled())
            logger.debug("Backup plugin properties: "
                    + pluginProperties.toString());

        // Load the plug-in class and configure its properties.
        BackupPlugin plugin;
        try
        {
            plugin = (BackupPlugin) Class.forName(pluginClassName)
                    .newInstance();
            pluginProperties.applyProperties(plugin);
        }
        catch (PropertyException e)
        {
            throw new BackupException(
                    "Unable to configure backup plugin properties: key="
                            + pluginPrefix + " class name=" + pluginClassName
                            + " : " + e.getMessage(), e);
        }
        catch (Exception e)
        {
            throw new BackupException("Unable to load plugin class: key="
                    + pluginPrefix + " class name=" + pluginClassName, e);
        }

        // Plug-in is ready to go, so call its configure method.
        try
        {
            plugin.configure();
        }
        catch (BackupException e)
        {
            logger.error("Plugin configuration failed: " + e.getMessage(), e);
            throw e;
        }
        catch (Throwable t)
        {
            String message = "Unable to configure plugin: key=" + pluginPrefix
                    + " class name=" + pluginClassName;
            logger.error(message, t);
            throw new BackupException(message, t);
        }

        // It worked. We have a configured plugin.
        logger.info("Backup plug-in configured successfully: key="
                + pluginPrefix + " class name=" + pluginClassName);
        return plugin;
    }

    // Resolve backup agent name from default value if necessary.
    private String resolveBackupAgentName(String name) throws BackupException
    {
        if (name == null)
        {
            if (backupDefaultName == null)
                throw new BackupException(
                        "No backup name specified and there is no default backup agent");
            else
                return this.backupDefaultName;
        }
        else
            return name;
    }

    // Finds a backup agent given a non-null name.
    private BackupAgent findBackupAgent(String name) throws BackupException
    {
        BackupAgent agent = backupAgents.get(name);
        if (agent == null)
            throw new BackupException("Backup agent name not found: " + name);
        return agent;
    }

    // Finds a storage agent or loads the default.
    private StorageAgent findStorageAgent(String name) throws BackupException
    {
        StorageAgent agent = null;

        if (name == null)
        {
            if (storageDefaultName == null)
                throw new BackupException(
                        "No storage name specified and there is no default storage agent");
            else
                agent = storageAgents.get(storageDefaultName);
        }
        else
        {
            agent = storageAgents.get(name);
            if (agent == null)
                throw new BackupException("Storage agent name not found: "
                        + name);
        }
        return agent;
    }
}