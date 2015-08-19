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
 * Initial developer(s): Ed Archibald
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.common.directory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.Resource;
import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.cluster.resource.notification.ClusterResourceNotification;
import com.continuent.tungsten.common.cluster.resource.notification.DirectoryNotification;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.exception.DirectoryException;
import com.continuent.tungsten.common.exception.DirectoryNotFoundException;
import com.continuent.tungsten.common.exception.ResourceException;
import com.continuent.tungsten.common.exec.ProcessExecutor;
import com.continuent.tungsten.common.patterns.notification.NotificationGroupMember;
import com.continuent.tungsten.common.patterns.notification.ResourceNotificationException;
import com.continuent.tungsten.common.patterns.notification.ResourceNotificationListener;
import com.continuent.tungsten.common.patterns.notification.ResourceNotifier;
import com.continuent.tungsten.common.utils.CLUtils;
import com.continuent.tungsten.common.utils.Command;
import com.continuent.tungsten.common.utils.CommandLineParser;
import com.continuent.tungsten.manager.resource.physical.Process;
import com.continuent.tungsten.manager.resource.physical.Operation;
import com.continuent.tungsten.manager.resource.physical.ResourceFactory;
import com.continuent.tungsten.manager.resource.shared.ResourceConfiguration;
import com.continuent.tungsten.manager.resource.shared.RootResource;

/**
 * This class provides a means to organize cluster resources in an intuitive,
 * hierarchical form. It allows us to, effectively, extend the resources that
 * can be referred to, directly, in the Tungsten ResourceManager.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Ed Archibald</a>
 * @version 1.0
 */
public class Directory extends ResourceTree
        implements
            Serializable,
            ResourceNotifier
{
    /**
     * 
     */
    private static Logger                                    logger               = Logger.getLogger(Directory.class);

    private static final long                                serialVersionUID     = 1L;

    public final static String                               DEFAULT_CLUSTER_NAME = "default";

    public final static String                               PROTOCOL             = "resource:";
    public final static String                               ROOT_ELEMENT         = "/";
    public final static String                               CURRENT_ELEMENT      = ".";
    public final static String                               PARENT_ELEMENT       = "..";
    public final static String                               ANY_ELEMENT          = "*";
    public final static String                               AMPERSAND            = "&";

    public final static String                               PATH_SEPARATOR       = "/";

    public static final String                               DIRECTORY            = "directory";

    public static final String                               EXECUTE              = "execute";

    public static final String                               LIST                 = "ls";
    public static final String                               CD                   = "cd";
    public static final String                               CP                   = "cp";
    public static final String                               RM                   = "rm";
    public static final String                               CREATE               = "create";
    public static final String                               PWD                  = "pwd";
    public static final String                               CHKEXEC              = "chkexec";
    public static final String                               WHICH                = "which";
    public static final String                               CONNECT              = "connect";
    public static final String                               DISCONNECT           = "disconnect";
    public static final String                               DISCONNECT_ALL       = "disconnectAll";
    public static final String                               MERGE                = "merge";
    public static final String                               SERVICE              = "service";
    public static final String                               EXTENSION            = "extension";
    public static final String                               LOCATE_SERVICE       = "locateServices";

    public static final char                                 FLAG_LONG            = 'l';
    public static final char                                 FLAG_RECURSIVE       = 'R';
    public static final char                                 FLAG_ABSOLUTE        = 'A';
    public static final char                                 FLAG_PARENTS         = 'p';

    public static final String                               KEY_COMMAND          = "command";

    public static final String[]                             directoryCommands    = {
            LIST, CD, CP, RM, CREATE, PWD, EXECUTE, CHKEXEC, WHICH, CONNECT,
            MERGE, SERVICE, DISCONNECT, DISCONNECT_ALL, LOCATE_SERVICE            };

    protected String                                         clusterName          = null;
    protected String                                         memberName           = null;
    protected boolean                                        recursive            = false;
    protected boolean                                        absolute             = false;
    protected boolean                                        detailed             = false;
    protected boolean                                        createParents        = false;

    protected static ArrayList<ResourceNotificationListener> listeners            = new ArrayList<ResourceNotificationListener>();

    protected Map<String, DirectorySession>                  sessionsByID         = new HashMap<String, DirectorySession>();
    // member <connectionID, list<sessions>>
    protected Map<String, Map<Long, Vector<String>>>         sessionsByDomain     = new HashMap<String, Map<Long, Vector<String>>>();

    protected String                                         systemSessionID      = null;

    // Services folders for each node, with the key being the node name.
    protected Map<String, ResourceNode>                      servicesFolders      = new TreeMap<String, ResourceNode>();

    // This gets incremented for any changes to this instance.
    protected Long                                           currentVersion       = 01L;
    protected Long                                           lastMergedVersion    = 0L;

    protected boolean                                        merging              = false;

    private static Directory                                 _instance            = null;

    private static CommandLineParser                         parser               = new CommandLineParser();

    public Directory()
    {

    }

    /**
     * Returns a global instance. This is the standard method to access a single
     * process-wide directory.
     * 
     * @param clusterName Name of the cluster
     * @param memberName Name of the cluster member
     * @return Returns the global directory
     * @throws ResourceException
     * @throws DirectoryNotFoundException
     */
    public static Directory getInstance(String clusterName, String memberName)
            throws ResourceException, DirectoryNotFoundException
    {
        if (_instance == null)
        {
            _instance = new Directory(clusterName, memberName);
        }

        return _instance;
    }

    /**
     * Creates a local directory instance that caller is responsible for
     * managing. This method allows creation of directories for unit tests.
     * 
     * @param clusterName Name of the cluster
     * @param memberName Name of the cluster member
     * @return Returns A new directory instance
     * @throws ResourceException
     * @throws DirectoryNotFoundException
     */
    public static Directory createLocalInstance(String clusterName,
            String memberName) throws ResourceException,
            DirectoryNotFoundException
    {
        return new Directory(clusterName, memberName);
    }

    /**
     * Creates an instance of Directory with some base-level resources.
     * 
     * @param memberName 
     * @throws ResourceException
     */
    private Directory(String clusterName, String memberName)
            throws ResourceException, DirectoryNotFoundException
    {
        this.clusterName = clusterName;
        this.memberName = memberName;

        rootNode = new ResourceNode(new RootResource());
        setRootNode(rootNode);

        String sessionID = UUID.randomUUID().toString();

        systemSessionID = connect(memberName, 0L, sessionID);
        systemSessionID = sessionID;

        ResourceNode defaultService = ResourceFactory.addInstance(
                ResourceType.CLUSTER, clusterName, getRootNode(), this,
                systemSessionID);

        ResourceNode host = ResourceFactory.addInstance(ResourceType.MANAGER,
                memberName, defaultService, this, systemSessionID);

        ResourceNode confFolder = ResourceFactory.addInstance(
                ResourceType.FOLDER, "conf", host, this, systemSessionID);

        // Hold config of executable services (replicator, connector, etc.)
        ResourceNode serviceFolder = ResourceFactory.addInstance(
                ResourceType.FOLDER, ResourceType.SERVICE.toString()
                        .toLowerCase(), confFolder, this, systemSessionID);

        servicesFolders.put(memberName, serviceFolder);
    }

    public synchronized String connect(String domain, long handle,
            String sessionID) throws DirectoryNotFoundException
    {
        synchronized (sessionsByDomain)
        {
            Map<Long, Vector<String>> domainSessions = sessionsByDomain
                    .get(domain);

            if (domainSessions == null)
            {
                domainSessions = new HashMap<Long, Vector<String>>();
                sessionsByDomain.put(domain, domainSessions);
            }

            Vector<String> sessions = domainSessions.get(handle);
            if (sessions == null)
            {
                sessions = new Vector<String>();
                domainSessions.put(handle, sessions);
            }

            DirectorySession session = null;

            try
            {
                session = getSession(sessionID);
            }
            catch (DirectoryNotFoundException ignored)
            {

            }

            if (session == null)
            {
                session = newSession(sessionID);
                sessions.add(session.getSessionID());
                if (logger.isDebugEnabled())
                {
                    logger.debug(String
                            .format("Created new session for domain %s %s for connectionID=%d",
                                    domain, sessionID, handle));
                }
                return session.getSessionID();
            }
            else
            {
                logger.warn(String.format(
                        "Directory session %s %s already exists", domain,
                        sessionID));
                return session.getSessionID();
            }

        }
    }

    public synchronized void disconnect(String domain, long handle,
            String sessionID)
    {
        Map<Long, Vector<String>> domainSessions = sessionsByDomain.get(domain);

        if (domainSessions == null)
        {
            return;
        }

        Vector<String> sessions = domainSessions.get(handle);

        if (sessions == null)
            return;

        disconnect(sessionID);

        if (sessions.remove(sessionID))
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(String
                        .format("Removed active session for domain %s %s for connection %d",
                                domain, sessionID, handle));
            }
        }

        if (sessions.isEmpty())
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(String.format(
                        "Clearing session storage storage for connection %d",
                        handle));
            }
            sessions.remove(handle);
        }

        if (domainSessions.isEmpty())
        {
            if (logger.isInfoEnabled())
                logger.info(String.format(
                        "Clearing session storage for domain %s", domain));
            sessionsByDomain.remove(domain);
        }
    }

    public synchronized void disconnectAll(String domain, long handle)
    {
        Map<Long, Vector<String>> domainSessions = sessionsByDomain.get(domain);

        if (domainSessions == null)
        {
            return;
        }
        Vector<String> sessions = domainSessions.get(handle);

        if (sessions == null)
            return;

        Vector<String> sessionsToRemove = new Vector<String>(sessions);
        for (String sessionID : sessionsToRemove)
        {
            disconnect(domain, handle, sessionID);
        }
    }

    public synchronized void disconnectAll(String domain)
    {
        Map<Long, Vector<String>> domainSessions = sessionsByDomain.get(domain);

        if (domainSessions == null)
        {
            return;
        }

        logger.info(String.format("Removing all sessions for domain '%s'",
                domain));

        synchronized (sessionsByDomain)
        {
            Vector<Long> handlesToRemove = new Vector<Long>(
                    domainSessions.keySet());
            for (Long handle : handlesToRemove)
            {
                disconnectAll(domain, handle);
            }
        }

        if (logger.isInfoEnabled())
            logger.info(String.format("Clearing session storage for domain %s",
                    domain));

        sessionsByDomain.remove(domain);
    }

    /**
     * Creates a new Directory session. This session becomes a context for a
     * given directory user and is used, primarily, to track the current working
     * resource. In most cases, this functionality will be used for interactive
     * applications where a user uses 'cd' to set a context.
     * 
     * @param sessionID
     * @return returns a string identifier for a new session
     */
    private DirectorySession newSession(String sessionID)
    {
        DirectorySession session = null;

        synchronized (sessionsByID)
        {
            session = new DirectorySession(this, sessionID, getRootNode());
            sessionsByID.put(session.getSessionID(), session);
        }

        return session;
    }

    /**
     * Returns an existing session or throws an exception.
     * 
     * @param sessionID
     * @return a context for a given session
     * @throws DirectoryNotFoundException
     */
    public DirectorySession getSession(String sessionID)
            throws DirectoryNotFoundException
    {
        DirectorySession session = null;

        if (sessionID == null)
        {
            throw new DirectoryNotFoundException(
                    "null sessionID is not allowed");
        }

        synchronized (sessionsByID)
        {
            session = sessionsByID.get(sessionID);

            if (session == null)
            {
                throw new DirectoryNotFoundException(String.format(
                        "Session %s not found", sessionID));
            }
        }

        session.setLastTimeAccessed(System.currentTimeMillis());
        return session;
    }

    /**
     * Removes an existing Directory session.
     * 
     * @param sessionID
     * @throws DirectoryException
     */
    public synchronized void removeSession(String sessionID)
            throws DirectoryException
    {
        DirectorySession session = null;

        synchronized (sessionsByID)
        {
            session = sessionsByID.get(sessionID);

            if (session == null)
            {
                throw new DirectoryException(String.format(
                        "Directory session '%s' not found", sessionID));
            }
            sessionsByID.remove(sessionID);
        }

    }

    /**
     * This is the main interface for the directory with respect to text-based
     * operations.
     * 
     * @param sessionID
     * @return the output for a given command
     * @throws Exception
     */
    public Serializable processCommand(String sessionID, String commandLine)
            throws Exception
    {
        Command cmd = parser.parseOne(commandLine);

        if (cmd == null)
        {
            throw new Exception("Cannot execute null command");
        }

        String command = cmd.getTokens()[0];
        String[] params = getParams(cmd.getTokens(), true);

        String result = null;

        if (command.equals(CD))
        {
            String path = (params != null ? params[0] : null);
            cd(sessionID, path);
        }
        else if (command.equals(LIST))
        {
            String path = (params != null ? params[0] : null);
            ResourceNode startNode = getStartNode(sessionID, path);
            List<ResourceNode> entries = ls(sessionID, path, cmd.isRecursive());
            result = formatEntries(entries, startNode, cmd.isLong(),
                    cmd.isAbsolute());
        }
        else if (command.equals(CREATE))
        {
            String path = (params != null ? params[0] : null);
            create(sessionID, path, cmd.includeParents());
        }
        else if (command.equals(RM))
        {
            String path = (params != null ? params[0] : null);
            rm(sessionID, path);
        }
        else if (command.equals(CP))
        {
            String source = (params != null ? params[0] : null);
            String destination = (source != null && params.length == 2)
                    ? params[1]
                    : null;
            cp(sessionID, source, destination);
        }
        else if (command.equals(PWD))
        {
            result = pwd(sessionID);
        }
        else if (command.equals(CHKEXEC))
        {
            String path = (params != null ? params[0] : null);
            return String.format("%s", isExecutable(sessionID, path));
        }
        else if (command.equals(WHICH))
        {
            String path = (params != null ? params[0] : null);
            return String.format("%s", which(sessionID, path));
        }
        else if (command.equals(CONNECT))
        {
            throw new Exception(
                    "This interface to CONNECT is no longer supported");
        }
        else if (command.equals(SERVICE))
        {
            String serviceSpec = (params != null ? params[0] : null);
            String serviceCmd = (params != null && params.length > 1
                    ? params[1]
                    : null);

            return executeExtension(ResourceType.SERVICE, serviceSpec,
                    "command", serviceCmd, null);
        }
        else if (command.equals(LOCATE_SERVICE))
        {
            String serviceSpec = (params != null ? params[0] : null);
            String serviceCmd = (params != null && params.length > 1
                    ? params[1]
                    : null);

            return executeExtension(ResourceType.SERVICE, serviceSpec,
                    "command", serviceCmd, null);
        }
        else if (command.equals(EXECUTE))
        {
            // Re-parse to ignore flags etc. We pass everthing
            // along to the extension as needed.
            cmd = parser.parseOne(commandLine, false);
            params = getParams(cmd.getTokens(), false);

            // Execute a procedure
            String type = (params != null ? params[0] : null);
            String extensionName = (params != null ? params[1] : null);
            String theCommand = (params != null && params.length > 2
                    ? params[2]
                    : null);

            if (type == null || extensionName == null || theCommand == null)
            {
                throw new DirectoryException(
                        String.format(
                                "Incorrectly formed command for execute:'%s'.",
                                command));
            }
            ResourceType extensionType = ResourceType.valueOf(type
                    .toUpperCase());

            String argList[] = null;
            if (params != null && params.length > 3)
            {
                argList = new String[params.length - 3];
                for (int i = 3; i < params.length; i++)
                    argList[i - 3] = params[i];
            }

            return executeExtension(extensionType, extensionName, KEY_COMMAND,
                    theCommand, argList);
        }
        else
        {
            if (isExecutable(sessionID, command))
            {
                throw new DirectoryException(String.format(
                        "Cannot execute '%s' in this context.", command));
            }
        }

        return result;
    }

    public synchronized void disconnect(String sessionID)
    {
        try
        {
            removeSession(sessionID);
        }
        catch (DirectoryException d)
        {
            logger.warn(String.format(
                    "Attempt to remove non-existent session %s", sessionID), d);
        }
    }

    /**
     * This command returns the full path to a given resource, as long as the
     * resource exists
     * 
     * @param sessionID
     * @param path
     * @return a string representing the full, absolute path for a given path
     *         element
     * @throws DirectoryNotFoundException
     */
    public String which(String sessionID, String path)
            throws DirectoryNotFoundException
    {
        ResourceNode node = null;

        try
        {
            node = locate(sessionID, path);
        }
        catch (DirectoryNotFoundException d)
        {
            return null;
        }

        String thePath = formatPath(getAbsolutePath(getRootNode(), node, true),
                true);

        int paramStart = thePath.indexOf("(");

        if (paramStart != -1)
        {
            return thePath.substring(0, paramStart);
        }

        return thePath;
    }

    /**
     * A primitive that indicates whether or not the resource associated with a
     * node is executable.
     * 
     * @param sessionID
     * @param path
     * @return true if the path is executable, otherwise false
     * @throws DirectoryNotFoundException
     */
    public boolean isExecutable(String sessionID, String path)
            throws DirectoryNotFoundException
    {
        ResourceNode execNode = locate(sessionID, path);

        if (execNode.getResource() == null)
        {
            throw new DirectoryNotFoundException(String.format(
                    "Cannot execute '%s'", path));
        }

        if (execNode.getResource() instanceof Operation)
        {
            return true;
        }

        return false;

    }

    /**
     * Returns services folder which is created upon construction of this
     * directory and which holds services to execute commands upon.
     */
    public ResourceNode getServiceFolder(String hostName)
    {
        return servicesFolders.get(hostName);
    }

    /**
     * Returns one or more service configurations as property instances.
     * 
     * @param sessionID Session ID
     * @param hostName Name of host on which service abides
     * @param name Name of the service or null to select all available services
     */
    public List<TungstenProperties> getServiceConfig(String sessionID,
            String hostName, String name)
    {
        List<TungstenProperties> serviceList = new ArrayList<TungstenProperties>();

        // Look up service folder for member.
        String servicePath = String.format("/%s/%s/conf/%s",
                this.getClusterName(), hostName, "service");
        ResourceNode serviceFolder = null;
        try
        {
            serviceFolder = this.locate(sessionID, servicePath);
        }
        catch (DirectoryNotFoundException e)
        {
            logger.warn("Unable to find service directory: " + serviceFolder);
            if (logger.isDebugEnabled())
                logger.debug(e);
        }
        if (serviceFolder == null)
            return serviceList;

        // Construct list of services by scanning children.
        for (String serviceName : serviceFolder.getChildren().keySet())
        {
            if (name == null || name.equals(serviceName))
            {
                ResourceNode serviceNode = serviceFolder.getChildren().get(
                        serviceName);
                ResourceConfiguration config = (ResourceConfiguration) serviceNode
                        .getResource();
                TungstenProperties serviceProps = config.getProperties();
                serviceList.add(serviceProps);
            }
        }

        return serviceList;
    }

    /**
     * Traverse back the indicated number of levels, starting at the current
     * node, and return the node found there.
     * 
     * @param levels
     * @return the resource that is relative to the current node, proceeding via
     *         the parents, by the indicated levels
     */
    public ResourceNode locateRelative(String sessionID, int levels)
            throws DirectoryException, DirectoryNotFoundException
    {
        return locateRelative(sessionID, levels, getCurrentNode(sessionID));
    }

    /**
     * Traverse back the indicated number of levels, starting at the current
     * node, and return the node found there.
     * 
     * @param levels
     * @return the resource that is relative to the current node, proceeding via
     *         the parents, by the indicated levels, starting at the indicated
     *         node.
     */
    public ResourceNode locateRelative(String sessionID, int levels,
            ResourceNode startNode) throws DirectoryException,
            DirectoryNotFoundException
    {
        if (levels == 0)
            return getCurrentNode(sessionID);

        ResourceNode foundNode = null;
        ResourceNode nodeToSearch = startNode;

        for (int level = 0; level < levels; level++)
        {
            if ((foundNode = nodeToSearch.getParent()) != null)
            {
                nodeToSearch = nodeToSearch.getParent();

            }
            else
            {
                throw new DirectoryException(String.format(
                        "No parent element found for '%s'",
                        formatPath(
                                getAbsolutePath(nodeToSearch, nodeToSearch,
                                        true), true)));
            }
        }

        return foundNode;
    }

    /**
     * Locates a given resource node, given a path. This method takes into
     * account the 'current' working node.
     * 
     * @param sessionID
     * @param path
     * @return the resource specified by path
     * @throws DirectoryNotFoundException if the resource cannot be found
     */
    public ResourceNode locate(String sessionID, String path)
            throws DirectoryNotFoundException
    {
        return locate(sessionID, path, getCurrentNode(sessionID));
    }

    /**
     * Locates a given resource give a path and a starting node.
     * 
     * @param path
     * @param startNode
     * @return the resource specified by path
     * @throws DirectoryNotFoundException if the resource cannot be found
     */
    public ResourceNode locate(String sessionID, String path,
            ResourceNode startNode) throws DirectoryNotFoundException
    {

        if (path == null)
        {
            return getCurrentNode(sessionID);
        }
        else if (path.startsWith(PROTOCOL))
        {
            path = path.substring(PROTOCOL.length());
        }
        else if (path.startsWith(ROOT_ELEMENT)
                && path.length() > ROOT_ELEMENT.length())
        {
            startNode = getRootNode();
            path = path.substring(ROOT_ELEMENT.length());
        }

        if (path.equals(CURRENT_ELEMENT))
            return getCurrentNode(sessionID);
        else if (path.equals(ROOT_ELEMENT))
            return getRootNode();

        ResourceNode foundNode = null;
        ResourceNode nodeToSearch = startNode;

        String pathElements[] = path.split(PATH_SEPARATOR);

        if (pathElements.length == 0)
        {
            return getRootNode();
        }

        for (String element : pathElements)
        {
            // Just skip blanks which result from extra slashes
            if (element.length() == 0)
                continue;

            if (element.equals(PARENT_ELEMENT))
            {
                if ((foundNode = nodeToSearch.getParent()) != null)
                {
                    nodeToSearch = nodeToSearch.getParent();

                }
                else
                {
                    throw new DirectoryNotFoundException(String.format(
                            "element '%s' not found", path));
                }
            }
            else
            {

                Map<String, ResourceNode> children = nodeToSearch.getChildren();

                // For the present, we just handle the wildcard as if it
                // means to return 'any' element rather than all elements.
                if (element.equals(ANY_ELEMENT))
                {
                    if (nodeToSearch.getType() == ResourceType.CLUSTER)
                    {
                        foundNode = children.get(memberName);
                    }
                    else if (children.size() > 0)
                    {
                        foundNode = getFirst(children);
                    }
                    else
                    {
                        throw new DirectoryNotFoundException(
                                String.format(
                                        "the element '%s' of path '%s' resolves to more than one element",
                                        element, path));
                    }
                }
                else
                {

                    foundNode = children.get(element);
                }

                if (foundNode == null)
                {
                    throw new DirectoryNotFoundException(
                            String.format(
                                    "element '%s' not found in path '%s' while searching for entry '%s'",
                                    element,
                                    formatPath(
                                            getAbsolutePath(getRootNode(),
                                                    nodeToSearch, true), true),
                                    path));
                }

                nodeToSearch = foundNode;
            }
        }

        return foundNode;
    }

    /**
     * @param map
     * @throws DirectoryNotFoundException
     */
    private ResourceNode getFirst(Map<String, ResourceNode> map)
            throws DirectoryNotFoundException
    {
        for (ResourceNode node : map.values())
            return node;

        return null;
    }

    /**
     * @param map
     * @return
     * @throws DirectoryNotFoundException
     */
//    private ResourceNode getLast(Map<String, ResourceNode> map)
//            throws DirectoryNotFoundException
//    {
//        ResourceNode lastNode = null;
//        for (ResourceNode node : map.values())
//        {
//            lastNode = node;
//        }
//
//        return lastNode;
//    }

    /**
     * @param path
     * @throws DirectoryNotFoundException
     */
    private ResourceNode getStartNode(String sessionID, String path)
            throws DirectoryNotFoundException
    {
        ResourceNode startNode = null;

        startNode = locate(sessionID, path);

        return startNode;

    }

    /**
     * Returns a list of resource nodes according to the path passed in.
     * 
     * @param path
     * @return a list of resources as indicated by the path.
     * @throws DirectoryNotFoundException if the specified resources cannot be
     *             found
     */
    public List<ResourceNode> ls(String sessionID, String path,
            boolean doRecurse) throws DirectoryNotFoundException
    {
        ResourceNode startNode = null;
        startNode = getStartNode(sessionID, path);
        List<ResourceNode> entries = new LinkedList<ResourceNode>();

        if (!startNode.isContainer())
        {
            entries.add(startNode);
            return entries;
        }

        getEntries(startNode, entries, doRecurse);

        return entries;
    }

    /**
     * Makes a copy of a specific resource node in the destination.
     * 
     * @param sourcePath
     * @param destinationPath
     * @throws DirectoryException
     * @throws DirectoryNotFoundException
     */
    public synchronized void cp(String sessionID, String sourcePath,
            String destinationPath) throws DirectoryException,
            DirectoryNotFoundException
    {

        ResourceNode destination = null;
        ResourceNode source = null;

        if (sourcePath == null || destinationPath == null)
        {
            throw new DirectoryException(
                    "cp: <source> <destination>: missing operand");
        }

        try
        {
            source = locate(sessionID, sourcePath);
        }
        catch (DirectoryNotFoundException c)
        {
            throw new DirectoryException(String.format(
                    "cp: the source element '%s' does not exist", sourcePath));

        }

        try
        {
            destination = locate(sessionID, destinationPath);

            // If the destination path exists and is not a container,
            // or it is of the same type, don't allow the copy.
            if (!destination.getResource().isContainer()
                    || destination.getResource().getType() == source
                            .getResource().getType())
            {

                throw new DirectoryException(String.format(
                        "cp: cannot copy over an existing element '%s'",
                        destinationPath));
            }
        }
        catch (DirectoryNotFoundException c)
        {
            // We have more checking to do.....
        }

        if (destination == null)
        {
            // If the destination path refers only to the new name,
            // the destination is the current node.
            if (lastElement(destinationPath).equals(
                    elementPrefix(destinationPath)))
            {
                destination = getCurrentNode(sessionID);
            }
            else
            {
                destination = locate(sessionID, elementPrefix(destinationPath));
            }
        }

        Resource copy = null;

        try
        {
            copy = ResourceFactory.copyInstance(source,
                    lastElement(destinationPath), destination, this);

        }
        catch (ResourceException c)
        {
            throw new DirectoryException(String.format(
                    "unable to create a copy of '%s', reason='%s'", sourcePath,
                    c.getMessage()));
        }

        destination.addChild(copy);
        flush();
    }

    /**
     * @param nodeToSearch
     * @param entries
     * @return all of the entries in the nodes below the node to search
     */
    public List<ResourceNode> getEntries(ResourceNode nodeToSearch,
            List<ResourceNode> entries, boolean doRecurse)
    {
        for (ResourceNode entry : nodeToSearch.getChildren().values())
        {
            entries.add(entry);

            if (doRecurse)
            {
                getEntries(entry, entries, doRecurse);
            }

        }

        return entries;
    }

    /**
     * @param path
     * @throws DirectoryNotFoundException
     */
    public synchronized ResourceNode cd(String sessionID, String path)
            throws DirectoryNotFoundException, DirectoryException
    {
        ResourceNode node = null;

        if (path == null)
        {
            node = getRootNode();
        }
        else
        {
            node = locate(sessionID, path);
        }

        if (!node.isContainer())
        {
            throw new DirectoryException(String.format(
                    "the element referenced by '%s' is not a container", path));
        }

        getSession(sessionID).setCurrentNode(node);

        flush();

        return node;

    }

    /**
     * Executes a command on a service. The command must be defined in the
     * service configuration like this for eg.:<br/>
     * command.start=../../tungsten-replicator/bin/replicator start
     * 
     * @param serviceSpec Name of the service.
     * @param serviceCmd Command name to execute on a service.
     * @return Stdout of the process have been executed.
     * @throws Exception If service node not found, or service does not support
     *             the requested command.
     */
    public String service(String serviceSpec, String serviceCmd)
            throws Exception
    {
        return executeExtension(ResourceType.SERVICE, serviceSpec, KEY_COMMAND,
                serviceCmd, null);
    }

    /**
     * Executes a command on a service. The command must be defined in the
     * service configuration like this for eg.:<br/>
     * command.start=../../tungsten-replicator/bin/replicator start
     * 
     * @param procedureSpec Name of the service.
     * @param command Command name to execute on a service.
     * @return standard output of the process have been executed.
     * @throws Exception If service node not found, or service does not support
     *             the requested command.
     */
    public String procedure(String procedureSpec, String command)
            throws Exception
    {
        return executeExtension(ResourceType.EXTENSION, procedureSpec, "run",
                command, null);
    }

    /**
     * Execute an 'extension' by looking it up in the appropriate cluster
     * configuration directory. This is a generic facility that can execute any
     * previously configured command at the OS level and includes the ability to
     * execute concurrently across a set of nodes. The arguments passed in
     * determine where we will look for the 'extension' and what we will
     * execute.
     * 
     * @param extensionType - the type of the extension. This determines the
     *            directory that is searched for the extension.
     * @param extensionName - the name of the .properties file to look for in
     *            the extension directory.
     * @param commandPrefix - the string to look for, in the extension
     *            properties file as an 'introducer' to a specific operation.
     *            This may vary depending on the function of the extension.
     * @param command - the command 'key' to use to find the command to execute.
     * @param args - any args required by the command. These are treated
     *            positionally
     */
    public String executeExtension(ResourceType extensionType,
            String extensionName, String commandPrefix, String command,
            String[] args) throws Exception
    {
        logger.debug(String.format("executeExtension(%s %s %s %s %s)",
                extensionType, extensionName, commandPrefix, command,
                args != null ? CLUtils.printArgs(args) : ""));

        if (extensionName == null)
        {
            return String
                    .format("You must provide the component name for this command");
        }

        String cmdPrefix = String.format("%s.", commandPrefix);
        String cmdProp = String.format("%s.%s", commandPrefix, command);

        String extensionPath = String.format("/%s/%s/conf/%s/%s",
                getClusterName(), getMemberName(), extensionType.toString()
                        .toLowerCase(), extensionName);

        ResourceNode extensionNode = locate(getSystemSessionID(), extensionPath);

        if (extensionNode == null)
        {
            return String
                    .format("Could not find an extension of type %s named %s on member %s",
                            extensionType, extensionName, memberName);
        }

        ResourceConfiguration config = (ResourceConfiguration) extensionNode
                .getResource();

        TungstenProperties tp = config.getProperties();

        if (command == null)
        {
            return String
                    .format("%s extension for %s takes one of the following commands:\n%s",
                            extensionType, extensionName,
                            tp.subset(cmdPrefix, true).keyNames());
        }

        String execPath = tp.getString(cmdProp);

        if (execPath == null)
        {
            return String.format(
                    "The %s extension for component %s does not support the command %s\n"
                            + "It takes one of the following commands\n:%s",
                    extensionType, extensionName, command,
                    tp.subset(cmdPrefix, true).keyNames());
        }
        else
        {
            ArrayList<String> execList = new ArrayList<String>();

            for (String arg : execPath.split(" +"))
            {
                execList.add(arg);
            }

            // original string from which args were taken
            // may have included quoted strings but the
            // basic command parser in cctrl doesn't leave them
            // alone. Since I don't want to mess with that parser
            // right now and break something else, just
            // reconstruct the quoted strings here.
            if (args != null)
            {
                boolean inQuotedString = false;
                String quotedString = "";
                for (String arg : args)
                {
                    if (arg.startsWith("\""))
                    {
                        inQuotedString = true;
                        quotedString = "";
                        quotedString = quotedString + arg + " ";
                        continue;
                    }
                    else if (inQuotedString)
                    {
                        if (arg.endsWith("\""))
                        {
                            inQuotedString = false;
                            quotedString = quotedString + arg;
                            execList.add(quotedString);
                        }
                        else
                        {
                            quotedString = quotedString + arg + " ";
                        }
                        continue;
                    }
                    else
                    {
                        execList.add(arg);
                    }
                }
            }

            // Log command execution so we can trace commands later in the log.
            logger.info("Executing OS command: + " + execPath);
            ProcessExecutor processExecutor = new ProcessExecutor();
            processExecutor.setWorkDirectory(null); // Uses current working dir.
            processExecutor.setCommands(execList.toArray(new String[execList
                    .size()]));
            processExecutor.run();
            if (processExecutor.isSuccessful())
            {
                logger.debug("**************RESULTS FROM COMMAND EXECUTION****************");
                logger.debug("Exit value: " + processExecutor.getExitValue());
                logger.debug("Stdout: " + processExecutor.getStdout());
                logger.debug("Stderr: " + processExecutor.getStderr());
            }
            else
            {
                // Failed commands need to be logged for debugging purposes.
                logger.warn("Command failed: " + execPath);
                logger.info("Exit value: " + processExecutor.getExitValue());
                logger.info("Stderr: " + processExecutor.getStderr());
                logger.info("Stdout: " + processExecutor.getStdout());
            }

            String stdOut = processExecutor.getStdout();
            String stdErr = processExecutor.getStderr();

            return stdErr + stdOut;
        }
    }

    public synchronized ResourceNode create(String sessionID, String name,
            boolean createParents) throws DirectoryException,
            DirectoryNotFoundException
    {
        return create(sessionID, name, getCurrentNode(sessionID), createParents);
    }

    /**
     * @param name
     * @param parent
     * @return the resource that was created
     * @throws DirectoryException if the parent cannot be found
     */
    public synchronized ResourceNode create(String sessionID, String name,
            ResourceNode parent) throws DirectoryException
    {
        ResourceNode newElement = null;

        if (name == null)
        {

            throw new DirectoryException(
                    String.format("mkdir: missing name to create"));
        }

        try
        {

            newElement = ResourceFactory.addInstance(parent.getResource()
                    .getChildType(), name, parent, this, sessionID);

        }
        catch (ResourceException r)
        {
            throw new DirectoryException(r.getMessage());
        }

        flush();
        return newElement;
    }

    /**
     * @param path
     * @param startNode
     * @param createParents
     * @return the resource that was created
     * @throws DirectoryException if the startNode cannot be found
     */
    public synchronized ResourceNode create(String sessionID, String path,
            ResourceNode startNode, boolean createParents)
            throws DirectoryException
    {

        if (path.startsWith(PROTOCOL))
        {
            path = path.substring(PROTOCOL.length());
        }

        String pathElements[] = path.split(PATH_SEPARATOR);

        if (pathElements.length == 0)
        {
            throw new DirectoryException(
                    "missing operand: usage: create <path>");
        }

        ResourceNode foundNode = startNode;
        ResourceNode createdNode = null;

        // First look at all of the elements up to the final element.
        // If createParents is set, create missing elements
        for (int i = 0; i < pathElements.length - 1; i++)
        {
            if (pathElements[i].length() == 0)
                continue;

            try
            {
                foundNode = locate(sessionID, pathElements[i], startNode);
            }
            catch (DirectoryNotFoundException d)
            {
                if (createParents)
                {
                    foundNode = create(sessionID, pathElements[i], startNode);
                }
                else
                {
                    throw new DirectoryException(String.format(
                            "element '%s' does not exist in path '%s'",
                            pathElements[i], path));
                }
            }

            startNode = foundNode;
        }

        createdNode = create(sessionID, pathElements[pathElements.length - 1],
                startNode);

        flush();
        return createdNode;

    }

    /**
     * @param sessionID
     * @param path
     * @throws DirectoryNotFoundException
     */
    public synchronized void rm(String sessionID, String path,
            ResourceNode targetNode) throws DirectoryNotFoundException,
            DirectoryException
    {
        ResourceNode nodeToRemove = null;

        nodeToRemove = getStartNode(sessionID, path);

        if (nodeToRemove != targetNode)
        {
            throw new DirectoryException(String.format(
                    "Found node %s is not the same as the target node %s",
                    nodeToRemove, targetNode));
        }
        nodeToRemove.getParent().removeChild(nodeToRemove.getKey());
    }

    /**
     * @param sessionID
     * @param path
     * @throws DirectoryNotFoundException
     */
    public synchronized void rm(String sessionID, String path)
            throws DirectoryNotFoundException, DirectoryException
    {
        ResourceNode nodeToRemove = null;

        nodeToRemove = getStartNode(sessionID, path);
        nodeToRemove.getParent().removeChild(nodeToRemove.getKey());
    }

    /**
     * @param sessionID
     * @return a string representing the working directory/resourceNode
     * @throws DirectoryNotFoundException
     */
    public String pwd(String sessionID) throws DirectoryNotFoundException
    {
        return formatPath(getCwd(sessionID), true);
    }

    /**
     * @param pathElements
     * @param reverseOrder
     * @return a formatted representation of the path
     */
    public static String formatPath(List<ResourceNode> pathElements,
            boolean reverseOrder)
    {
        StringBuilder builder = new StringBuilder();

        int elementCount = 0;

        for (ResourceNode element : pathElements)
        {
            String elementString = null;

            if (element.getResource() instanceof Operation)
            {
                elementString = (element.getResource() != null ? element
                        .getResource().toString() : element.toString());
            }
            else
            {
                elementString = element.getKey();
                if (element.isContainer() && element.getParent() != null)
                    elementString += "/";
            }

            if (reverseOrder)
            {
                builder.insert(0, elementString);
            }
            else
            {
                if (element.getParent() == null)
                {
                    builder.append("/");
                }
                builder.append(elementString);
                if (elementCount++ > 0)
                {
                    builder.append("/");
                }
            }
        }

        return builder.toString();
    }

    /**
     * @param entries
     * @param detailed
     * @return string representation of the entries
     */
    @SuppressWarnings("unchecked")
    public static String formatEntries(List<ResourceNode> entries,
            ResourceNode startNode, boolean detailed, boolean absolute)
    {
        StringBuilder builder = new StringBuilder();

        int entryCount = 0;

        Collections.sort(entries);

        for (ResourceNode entry : entries)
        {
            // On entry we must ensure the start node is correct.
            if (entryCount++ == 0)
            {
                // Default to the root if no start node is provided.
                if (startNode == null)
                    startNode = entry.getRoot();
            }
            else
            {
                builder.append("\n");
            }

            if (detailed)
            {
                Resource res = entry.getResource();
                String detail = (res != null ? res.describe(detailed) : entry
                        .toString());

                builder.append(String.format("%s", detail));
            }
            else
            {
                boolean includeStartNode = (startNode.getParent() == null)
                        || absolute;

                if (absolute)
                {
                    startNode = startNode.getRoot();
                }
                builder.append(String.format(
                        "%s",
                        formatPath(
                                getAbsolutePath(startNode, entry,
                                        includeStartNode), true)));

            }

        }

        return builder.toString();
    }

    public static List<ResourceNode> getAbsolutePath(ResourceNode fromNode,
            ResourceNode toNode, boolean includeFromNode)
    {
        List<ResourceNode> absolutePath = new LinkedList<ResourceNode>();

        absolutePath.add(toNode);

        ResourceNode parent = toNode.getParent();

        while (parent != null)
        {
            if (parent == fromNode)
            {
                if (includeFromNode)
                    absolutePath.add(parent);

                break;
            }

            absolutePath.add(parent);

            parent = parent.getParent();
        }

        return absolutePath;
    }

    public ResourceNode getCurrentNode(String sessionID)
            throws DirectoryNotFoundException
    {
        return getSession(sessionID).getCurrentNode();
    }

    public void setCurrentNode(String sessionID, ResourceNode currentNode)
            throws DirectoryNotFoundException
    {
        getSession(sessionID).setCurrentNode(currentNode);
    }

    public List<ResourceNode> getCwd(String sessionID)
            throws DirectoryNotFoundException
    {
        return getAbsolutePath(getRootNode(), getSession(sessionID)
                .getCurrentNode(), true);
    }

    /**
     * This method merges the current directory with another directory such that
     * the current directory has all elements, by name, which exist in both
     * directories and any elements that have identical names remain in the
     * current directory.
     * 
     * @param source
     * @param destination
     * @return the count of the number of nodes merged
     * @throws DirectoryException
     */
    public synchronized Directory merge(Directory source, Directory destination)
            throws DirectoryException
    {
        merging = true;
        ResourceNode sourceNode = source.getRootNode();
        ResourceNode destinationNode = destination.getRootNode();
        Integer nodesMerged = new Integer(0);

        System.out.println("###### START DIRECTORY MERGE ########");

        _merge(sourceNode, destinationNode, nodesMerged);

        System.out.println(String.format(
                "###### END DIRECTORY MERGE, MERGED=%d ########", nodesMerged));

        // While we are at it, make sure that we add any sessionsByID we do
        // not already know about.
        destination.mergeSessions(source);

        lastMergedVersion = currentVersion;
        merging = false;
        return this;
    }

    private void _merge(ResourceNode sourceNode, ResourceNode destinationNode,
            Integer nodesMerged)
    {

        Map<String, ResourceNode> sourceChildren = sourceNode.getChildren();
        Map<String, ResourceNode> destinationChildren = destinationNode
                .getChildren();

        for (ResourceNode sourceChild : sourceChildren.values())
        {
            ResourceNode destinationChild = destinationChildren.get(sourceChild
                    .getKey());

            if (destinationChild == null)
            {
                destinationChild = destinationNode.addChild(sourceChild
                        .getResource());
                nodesMerged++;
            }

            _merge(sourceChild, destinationChild, nodesMerged);
        }
    }

    /**
     * paramStart
     * 
     * @param args
     */
    String[] getParams(String[] args, boolean parseFlags)
            throws DirectoryException
    {
        if (args == null || args.length == 1)
            return null;

        int paramIndex = 1;

        if (args[paramIndex].startsWith("-") && parseFlags)
        {
            byte chars[] = args[paramIndex].getBytes();
            // Skip the flag
            for (int i = 1; i < chars.length; i++)
            {
                if (chars[i] == FLAG_LONG)
                    detailed = true;
                else if (chars[i] == FLAG_RECURSIVE)
                    recursive = true;
                else if (chars[i] == FLAG_PARENTS)
                    createParents = true;
                else if (chars[i] == FLAG_ABSOLUTE)
                    absolute = true;
                else
                    throw new DirectoryException(String.format(
                            "Unrecognized option '%c'", chars[i]));
            }

            paramIndex++;

        }

        if (args.length == paramIndex)
            return null;

        String[] params = new String[args.length - paramIndex];
        int countParams = args.length - paramIndex;

        for (int i = 0; i < countParams; i++)
        {
            params[i] = args[paramIndex++];
        }

        return params;

    }

    public Map<String, DirectorySession> getSessionMap()
    {
        synchronized (sessionsByID)
        {
            return new HashMap<String, DirectorySession>(sessionsByID);
        }
    }

    private void mergeSessions(Directory source)
    {
        synchronized (sessionsByID)
        {
            Map<String, DirectorySession> sessionMap = source.getSessionsByID();
            for (String sessionId : sessionMap.keySet())
            {
                if (sessionsByID.get(sessionId) == null)
                {
                    sessionsByID.put(sessionId, sessionMap.get(sessionId));
                }
            }
        }

        synchronized (sessionsByDomain)
        {
            Map<Long, Vector<String>> domainSessionsLocal = sessionsByDomain
                    .get(source.getMemberName());

            // Create a place to store these remote domain sessions
            if (domainSessionsLocal == null)
            {
                domainSessionsLocal = new HashMap<Long, Vector<String>>();
                sessionsByDomain.put(source.getMemberName(),
                        domainSessionsLocal);
            }

            Map<String, Map<Long, Vector<String>>> sourceDomainMap = source
                    .getSessionsByDomain();
            Map<Long, Vector<String>> domainSessionsRemote = sourceDomainMap
                    .get(source.getMemberName());

            for (Long handle : domainSessionsRemote.keySet())
            {
                Vector<String> localSessions = domainSessionsLocal.get(handle);

                if (localSessions == null)
                {
                    localSessions = new Vector<String>();
                    domainSessionsLocal.put(handle, localSessions);
                }

                Vector<String> remoteSessions = domainSessionsRemote
                        .get(handle);
                for (String sessionID : remoteSessions)
                {
                    if (!localSessions.contains(sessionID))
                    {
                        logger.debug(String
                                .format("Merged session for domain %s %s for handle=%d",
                                        source.getMemberName(), sessionID,
                                        handle));
                        localSessions.add(sessionID);
                    }
                }
            }
        }
    }

    public Map<String, Map<Long, Vector<String>>> getSessionsByDomain()
    {
        return sessionsByDomain;
    }

    @Override
    public String toString()
    {
        String ret = null;

        try
        {
            List<ResourceNode> listResult = ls(systemSessionID, ROOT_ELEMENT,
                    true);
            Collections.sort(listResult, new ResourceNodeComparator());
            ret = formatEntries(listResult, null, detailed, false);
        }
        catch (Exception e)
        {
            ret = "NOT AVAILABLE BECAUSE OF EXCEPTION=" + e;
        }

        return ret;
    }

    class ResourceNodeComparator
            implements
                Comparator<ResourceNode>,
                Serializable
    {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        public int compare(ResourceNode o1, ResourceNode o2)
        {
            return o1.getKey().compareTo(o2.getKey());
        }
    }

    private String lastElement(String path)
    {
        return path.substring((path.lastIndexOf(PATH_SEPARATOR) + 1));
    }

    private String elementPrefix(String path)
    {
        int separatorLocation = path.lastIndexOf(PATH_SEPARATOR);

        if (separatorLocation == -1)
        {
            return null;
        }

        return path.substring(0, separatorLocation);
    }

    public boolean exists(String sessionID, String path)
    {
        ResourceNode nodeToVerify = null;

        try
        {
            nodeToVerify = locate(sessionID, path);
        }
        catch (DirectoryNotFoundException c)
        {
            return false;
        }

        return ((nodeToVerify == null ? false : true));

    }

    public static boolean isDirectoryCommand(String command)
    {
        for (String cmd : directoryCommands)
        {
            if (command.compareToIgnoreCase(cmd) == 0)
                return true;
        }

        return false;
    }

    /**
     * @return the systemSessionID
     */
    public String getSystemSessionID()
    {
        return systemSessionID;
    }

    /**
     * @param systemSessionID the systemSessionID to set
     */
    public void setSystemSessionID(String systemSessionID)
    {
        this.systemSessionID = systemSessionID;
    }

    public void addListener(ResourceNotificationListener listener)
    {
        if (listener == null)
        {
            logger.warn("Attempting to add a null listener");
            return;
        }
        listeners.add(listener);
    }

    public void notifyListeners(ClusterResourceNotification notification)
            throws ResourceNotificationException
    {
        for (ResourceNotificationListener listener : listeners)
        {
            listener.notify(notification);
        }
    }

    public void run()
    {
        
    }

    public synchronized void flush()
    {
        if (merging)
            return;

        currentVersion++;

        TungstenProperties resourceProps = new TungstenProperties();
        resourceProps.setObject("directory", this);

        DirectoryNotification notification = new DirectoryNotification(
                clusterName, memberName, getClass().getSimpleName(), getClass()
                        .getSimpleName(), ResourceState.MODIFIED, resourceProps);

        try
        {
            notifyListeners(notification);
        }
        catch (ResourceNotificationException r)
        {
            logger.error(
                    String.format(
                            "Could not send directory synchronization request, reason=%s",
                            r.getLocalizedMessage()), r);

        }
    }

    public long getVersion()
    {
        return currentVersion;
    }

    public Map<String, NotificationGroupMember> getNotificationGroupMembers()
    {
        return new LinkedHashMap<String, NotificationGroupMember>();
    }

    public Map<String, DirectorySession> getSessionsByID()
    {
        return sessionsByID;
    }

    public void setSessionsByID(Map<String, DirectorySession> sessionsByID)
    {
        this.sessionsByID = sessionsByID;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    public String getMemberName()
    {
        return memberName;
    }

    public void setMemberName(String memberName)
    {
        this.memberName = memberName;
    }

    public void prepare() throws Exception
    {
        

    }

    /**
     * @param siteName 
     * @param clusterName
     * @param host
     * @param beanServiceName
     * @param port
     * @param component
     * @param managerName
     * @throws Exception
     */
    public ResourceNode getManagerNode(String sessionID, String siteName,
            String clusterName, String host, String beanServiceName, int port,
            String component, String managerName) throws Exception
    {
        // String path = String.format("%s/%s/%s:%d/%s", dataServiceName, host,
        // beanServiceName, port, managerName);

        String path = String.format("/%s/%s/%s/%s/%s", siteName, clusterName,
                host, beanServiceName, managerName);

        ResourceNode managerNode = null;

        try
        {
            managerNode = locate(sessionID, path);
        }
        catch (DirectoryNotFoundException d)
        {
            try
            {
                managerNode = create(sessionID, path, true);
            }
            catch (DirectoryException di)
            {
                throw new Exception(String.format(
                        "Unable to create directory entry, reason=%s",
                        di.getMessage()));
            }
        }

        return managerNode;

    }

    /**
     * Processes are found at: /<site>/<cluster>/<member>/<component>/<process>
     * 
     * @param sessionID
     * @param clusterName
     * @param memberName
     */
    public ResourceNode getProcessNode(String sessionID, String clusterName,
            String memberName, String processName)
    {
        String path = String.format("/%s/%s/%s", clusterName, memberName,
                processName);

        ResourceNode processNode = null;

        try
        {
            processNode = locate(sessionID, path, getRootNode());
        }
        catch (DirectoryNotFoundException d)
        {
            return null;
        }

        return processNode;

    }

    /**
     * @see com.continuent.tungsten.common.directory.Directory#rm(java.lang.String,
     *      com.continuent.tungsten.common.directory.ResourceNode)
     */
    public synchronized void rm(String sessionID, ResourceNode node)
    {
        node.getParent().removeChild(node.getKey());
    }

    public synchronized ResourceNode createProcessNode(String sessionID,
            String siteName, String clusterName, String memberName,
            String componentName, String resourceManagerName, int port)
            throws DirectoryException
    {
        String path = String.format("/%s/%s/%s/%s", siteName, clusterName,
                memberName, resourceManagerName);

        if (memberName == null || port == 0)
        {
            String message = String
                    .format("Attempting to create an invalid process entry for component '%s'",
                            resourceManagerName);
            throw new DirectoryException(message);
        }

        try
        {
            ResourceNode processNode = create(sessionID, path, true);
            Process process = (Process) processNode.getResource();
            process.setMember(memberName);
            process.setClusterName(clusterName);
            process.setPort(port);
            return processNode;
        }
        catch (Exception di)
        {
            throw new DirectoryException(String.format(
                    "Unable to create directory entry, reason=%s",
                    di.getMessage()), di);
        }
    }

    public String formatPath(Directory directory, String sessionID,
            ResourceNode node) throws DirectoryNotFoundException
    {
        return Directory.formatPath(
                directory.getAbsolutePath(directory, sessionID, node), true);
    }

    public List<ResourceNode> getAbsolutePath(Directory directory,
            String sessionID, ResourceNode node)
            throws DirectoryNotFoundException
    {
        return getAbsolutePath(directory.getCurrentNode(sessionID), node, true);
    }

}
