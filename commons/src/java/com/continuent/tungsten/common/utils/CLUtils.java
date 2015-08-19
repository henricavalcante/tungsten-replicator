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
 * Initial developer(s): Edward Archibald
 * Contributor(s): 
 */

package com.continuent.tungsten.common.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.MultiCompletor;
import jline.NullCompletor;
import jline.SimpleCompletor;

import com.continuent.tungsten.common.cluster.resource.DataSource;
import com.continuent.tungsten.common.cluster.resource.DataSourceRole;
import com.continuent.tungsten.common.cluster.resource.Replicator;
import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.notification.ReplicatorNotification;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.exception.CLException;

public class CLUtils implements Serializable
{
    /**
     * 
     */
    private static final long     serialVersionUID = 1L;
    private static final String   COMMAND_COMMIT   = "commit";
    private static final String   COMMAND_QUIT     = "quit";
    private static final String   COMMAND_ROLLBACK = "rollback";
    private static final String   COMMAND_LIST     = "ls";

    private static final String   NEWLINE          = "\n";

    private static Vector<String> captureBuffer    = new Vector<String>();

    private static CLLogLevel     logLevel         = CLLogLevel.normal;

    public static void clearCapture()
    {
        captureBuffer.clear();
    }

    public static void getCapture(Vector<String> transferBuffer)
    {
        transferBuffer.addAll(captureBuffer);
        clearCapture();
    }

    public static CLLogLevel getLogLevel()
    {
        return logLevel;
    }

    public static void setLogLevel(CLLogLevel logLevel)
    {
        CLUtils.logLevel = logLevel;
    }

    public static String setLogLevel(String level) throws CLException
    {
        try
        {
            logLevel = CLLogLevel.valueOf(level);
            return logLevel.toString();
        }
        catch (IllegalArgumentException i)
        {
            throw new CLException(String.format(
                    "'%s' is not a valid value for the log level", level));

        }
    }

    private static SimpleDateFormat dateFormat = new SimpleDateFormat(
                                                       "[yyyy/MM/dd hh:mm:ss a z]");

    public static String[] getInputTokens(ConsoleReader cr, String prompt,
            BufferedReader in) throws IOException
    {
        String inbuf = null;

        if (cr != null)
        {
            inbuf = cr.readLine(prompt);
        }
        else
        {
            System.out.print(prompt);
            inbuf = in.readLine();
        }
        if (inbuf == null)
        {
            CLUtils.println("\nExiting...");
            System.exit(0);
        }

        Vector<String> noBlanks = new Vector<String>();
        for (String token : inbuf.split("\\b"))
        {
            if (!token.trim().equals(""))
                noBlanks.add(token);
        }

        if (noBlanks.size() > 0)
        {
            return noBlanks.toArray(new String[noBlanks.size()]);
        }
        return null;
    }

    /**
     * Does a generic format of a TungstenProperties instance.
     * 
     * @param name - a name to be associated with the properties
     * @param props - the TungstenProperties instance
     * @param header - an optional header that can be pre-pended to each
     *            property
     * @return a String with the aggregate formatted properties.
     */
    public static String formatPropertiesOld(String name,
            TungstenProperties props, String header, boolean wasModified)
    {
        String indent = "  ";
        StringBuilder builder = new StringBuilder();
        builder.append(header);

        builder.append(String.format("%s%s\n", name, modifiedSign(wasModified)));
        builder.append("{\n");
        Map<String, String> propMap = props.hashMap();
        for (String key : propMap.keySet())
        {
            builder.append(String.format("%s%s = %s\n", indent, key,
                    propMap.get(key)));
        }
        builder.append(String.format("}"));

        return builder.toString();
    }

    public static String formatProperties(String name,
            TungstenProperties props, String header, boolean wasModified)
    {
        Map<String, String> propMap = props.hashMap();
        return formatMap(name, propMap, "", header, wasModified);
    }

    /**
     * Does a generic format of a TungstenProperties instance.
     * 
     * @param name - a name to be associated with the properties
     * @param props - the TungstenProperties instance
     * @param header - an optional header that can be pre-pended to each
     *            property
     * @return a String with the aggregate formatted properties.
     */
    public static String formatMapOld(String name, Map<String, String> props,
            String header, boolean wasModified)
    {
        String indent = "  ";
        StringBuilder builder = new StringBuilder();
        builder.append(header);

        builder.append(String.format("%s%s\n", name, modifiedSign(wasModified)));
        builder.append("{\n");

        for (String key : props.keySet())
        {
            builder.append(String.format("%s%s = %s\n", indent, key,
                    props.get(key)));
        }
        builder.append(String.format("}"));
        return builder.toString();
    }

    public static String formatMap(String name, Map<String, String> props,
            String header, boolean wasModified)
    {
        return formatMap(name, props, "", header, wasModified);
    }

    public static String formatMap(String name, Map<String, String> props,
            String indent, String header, boolean wasModified)
    {
        StringBuilder builder = new StringBuilder();

        TreeMap<String, String> sorted = new TreeMap<String, String>();
        sorted.putAll(props);
        for (String key : sorted.keySet())
        {
            Object value = props.get(key);
            value = (value == null ? "" : value);

            builder.append(String.format("%s%30s:    %s\n", indent, key,
                    value.toString()));

        }

        Vector<String[]> results = new Vector<String[]>();
        results.add(new String[]{builder.toString()});

        return ResultFormatter.formatResults(name, null, results,
                ResultFormatter.DEFAULT_WIDTH, true, true);
    }

    public static String formatProperties(String name,
            TungstenProperties props, String header)
    {
        return formatProperties(name, props, header, false);
    }

    /**
     * @param header
     * @param wasModified
     * @param printDetails
     * @param includeStatistics
     * @param useRelativeLatency
     * @return a formatted string representing a datasource
     */
    public static String formatDsMap(Map<String, TungstenProperties> dsMap,
            String header, boolean wasModified, boolean printDetails,
            boolean includeStatistics, boolean useRelativeLatency)
    {
        StringBuilder builder = new StringBuilder();

        for (TungstenProperties dsProps : dsMap.values())
        {
            builder.append(
                    formatStatus(dsProps, null, null, null, true, header,
                            wasModified, printDetails, includeStatistics,
                            false, false, useRelativeLatency)).append(NEWLINE);
        }

        return builder.toString();

    }

    /**
     * @param dsProps
     * @param header
     * @param wasModified
     * @param printDetails
     * @param includeStatistics
     * @param useRelativeLatency
     * @return a formatted string representing a datasource
     */
    public static String formatDsProps(TungstenProperties dsProps,
            String header, boolean wasModified, boolean printDetails,
            boolean includeStatistics, boolean useRelativeLatency)
    {
        return formatStatus(dsProps, null, null, null, true, header,
                wasModified, printDetails, includeStatistics, true, false,
                useRelativeLatency);
    }

    /**
     * @param dsProps - datasource properties to format
     * @param replProps - formatted replicator status for the datasource
     * @param header - header to be inserted on each line
     * @param wasModified - indicates whether or not the datasource has been
     *            modified
     * @param printDetails - print details
     * @param includeStatistics - include statistics
     * @param useRelativeLatency
     * @return a formatted string representing a datasource/replicator status
     */
    public static String formatStatus(TungstenProperties dsProps,
            TungstenProperties replProps, String header, boolean wasModified,
            boolean printDetails, boolean includeStatistics,
            boolean useRelativeLatency)
    {
        return formatStatus(dsProps, replProps, null, null, true, header,
                wasModified, printDetails, includeStatistics, false, false,
                useRelativeLatency);
    }

    /**
     * Format manager status
     * 
     * @param isRawFormat
     * @param useRelativeLatency
     */
    public static String formatStatus(TungstenProperties dsProps,
            TungstenProperties replProps, TungstenProperties dbProps,
            TungstenProperties routerUsage, boolean managerIsOnline,
            String header, boolean wasModified, boolean printDetails,
            boolean includeStatistics, boolean isRawFormat,
            boolean useRelativeLatency)
    {
        return formatStatus(dsProps, replProps, dbProps, routerUsage,
                managerIsOnline, header, wasModified, printDetails,
                includeStatistics, true, isRawFormat, useRelativeLatency);
    }

    public static String formatRouterStatus(TungstenProperties dsProps,
            boolean printDetails, boolean useRelativeLatency)
    {

        String activeConnections = dsProps
                .getString(DataSource.ACTIVE_CONNECTION_COUNT);
        String connectionsCreated = dsProps
                .getString(DataSource.CONNECTIONS_CREATED_COUNT);

        String latencyDisplay = "";

        if (dsProps.getString("role").equals("slave")
                || dsProps.getString("role").equals("relay"))
        {
            String relativeLatencyInfo = useRelativeLatency ? String.format(
                    ", relative=%5.3f", dsProps.getDouble(
                            DataSource.RELATIVE_LATENCY, "-1.0", false)) : "";

            latencyDisplay = String.format(", latency=%5.3f%s",
                    dsProps.getDouble(DataSource.APPLIED_LATENCY),
                    relativeLatencyInfo);
        }
        return String.format("%s(%s:%s, created=%s, active=%s%s)",
                dsProps.getString("name"), dsProps.getString("role"),
                dsProps.getString("state"), connectionsCreated,
                activeConnections, latencyDisplay);

    }

    /**
     * @param dsProps - datasource properties to format
     * @param replProps - formatted replicator status for the datasource
     * @param dbProps - properties that represent the database server state
     * @param managerIsOnline
     * @param header - header to be inserted on each line
     * @param wasModified - indicates whether or not the datasource has been
     *            modified
     * @param printDetails - print details
     * @param includeStatistics - whether or not to include statistics
     * @param includeComponents
     * @param isRawFormat
     * @param userRelativeLatency
     * @return a formatted string representing a datasource/replicator status
     */
    public static String formatStatus(TungstenProperties dsProps,
            TungstenProperties replProps, TungstenProperties dbProps,
            boolean managerIsOnline, String header, boolean wasModified,
            boolean printDetails, boolean includeStatistics,
            boolean includeComponents, boolean isRawFormat,
            boolean userRelativeLatency)
    {
        return formatStatus(dsProps, replProps, dbProps, null, managerIsOnline,
                header, wasModified, printDetails, includeStatistics,
                includeComponents, isRawFormat, userRelativeLatency);
    }

    /**
     * @param dsProps - datasource properties to format
     * @param replProps - formatted replicator status for the datasource
     * @param dbProps - properties that represent the database server state
     * @param routerUsage
     * @param managerIsOnline
     * @param header - header to be inserted on each line
     * @param wasModified - indicates whether or not the datasource has been
     *            modified
     * @param printDetails - print details
     * @param includeStatistics - whether or not to include statistics
     * @param includeComponents
     * @param isRawFormat If true, eliminates 'pretty' formatting.
     * @param useRelativeLatency
     * @return a formatted string representing a datasource/replicator status
     */
    public static String formatStatus(TungstenProperties dsProps,
            TungstenProperties replProps, TungstenProperties dbProps,
            TungstenProperties routerUsage, boolean managerIsOnline,
            String header, boolean wasModified, boolean printDetails,
            boolean includeStatistics, boolean includeComponents,
            boolean isRawFormat, boolean useRelativeLatency)
    {
        String indent = "  ";
        StringBuilder builder = new StringBuilder();
        builder.append(header);
        String progressInformation = "";
        String additionalInfo = "";
        String replicator_useSSLConnection = ""; // true if Replicator uses SSL

        int indentToUse = dsProps.getString(DataSource.NAME).length() + 1;
        /*
         * Witness have only a header, so take care of them here and return.
         */
        if (dsProps.getString(DataSource.ROLE).equals(
                DataSourceRole.witness.toString()))
        {
            String dsHeader = String.format("%s(%s:%s)", dsProps
                    .getString(DataSource.NAME), dsProps
                    .getString(DataSource.ROLE),
                    managerIsOnline
                            ? dsProps.getString(DataSource.STATE)
                            : "FAILED");

            if (!isRawFormat)
            {
                builder.append(
                        ResultFormatter.makeSeparator(
                                ResultFormatter.DEFAULT_WIDTH, 1, true))
                        .append(NEWLINE);
                builder.append(ResultFormatter.makeRow(
                        (new String[]{dsHeader}),
                        ResultFormatter.DEFAULT_WIDTH, indentToUse, true, true));

                builder.append(
                        ResultFormatter.makeSeparator(
                                ResultFormatter.DEFAULT_WIDTH, 1, true))
                        .append(NEWLINE);

                builder.append(ResultFormatter.makeRow(
                        new String[]{indent
                                + String.format("MANAGER(state=%s)",
                                        managerIsOnline ? "ONLINE" : "STOPPED")},
                        ResultFormatter.DEFAULT_WIDTH, 0, false, true));

                builder.append(
                        ResultFormatter.makeSeparator(
                                ResultFormatter.DEFAULT_WIDTH, 1, true))
                        .append(NEWLINE);
            }
            else
            {
                builder.append(dsHeader
                        + "\n"
                        + String.format("MANAGER(state=%s)", managerIsOnline
                                ? "ONLINE"
                                : "STOPPED"));
            }

            builder.append(NEWLINE);

            return builder.toString();

        }

        // --- Replicator properties ---
        if (replProps != null)
        {

            progressInformation = String.format("progress=%s",
                    replProps.getString(Replicator.APPLIED_LAST_SEQNO));

            String relativeLatencyInfo = useRelativeLatency ? String.format(
                    ", relative=%5.3f", replProps.getDouble(
                            Replicator.RELATIVE_LATENCY, "-1.0", false)) : "";

            if (dsProps.getString(Replicator.ROLE).equals("master"))
            {

                additionalInfo = String.format(", %s, THL latency=%5.3f%s",
                        progressInformation, replProps.getDouble(
                                Replicator.APPLIED_LATENCY, "-1.0", false),
                        relativeLatencyInfo);

            }
            else
            {
                additionalInfo = String.format(", %s, latency=%5.3f%s",
                        progressInformation, replProps.getDouble(
                                Replicator.APPLIED_LATENCY, "-1.0", false),
                        relativeLatencyInfo);
            }

            // Retrieve useSSLConnection value
            Boolean _replicator_useSSLConnection = replProps
                    .getBoolean(Replicator.USE_SSL_CONNECTION);
            if (_replicator_useSSLConnection != null
                    && _replicator_useSSLConnection)
                replicator_useSSLConnection = MessageFormat.format("[{0}]",
                        "SSL");
        }

        // --- DataSource properties ---
        String vipInfo = null;

        if (dsProps.getBoolean(DataSource.VIPISBOUND)
                && dsProps.getString(DataSource.STATE).equals(
                        ResourceState.ONLINE.toString()))
        {
            vipInfo = String.format("VIP=(%s:%s)",
                    dsProps.getString(DataSource.VIPINTERFACE),
                    dsProps.getString(DataSource.VIPADDRESS));
        }

        String state = dsProps.get(DataSource.STATE);
        String failureInfo = "";
        if (state.equals(ResourceState.FAILED.toString()))
        {
            failureInfo = String.format("(%s)",
                    dsProps.getString(DataSource.LASTERROR));
        }
        else if (state.equals(ResourceState.SHUNNED.toString()))
        {
            String lastError = dsProps.getString(DataSource.LASTERROR);
            String shunReason = dsProps.getString(DataSource.LASTSHUNREASON);

            if (!shunReason.equals("") && !shunReason.equals("NONE"))
            {
                if (lastError != null
                        && (!lastError.equals("") && !lastError.equals("--")))
                    failureInfo = String.format("(%s AFTER %s)", shunReason,
                            lastError);
                else
                    failureInfo = String.format("(%s)", shunReason);
            }
            else
            {
                failureInfo = String.format("(%s)", lastError);
            }
        }

        // / --- Build String for status ---
        String connectionStats = "";
        boolean isComposite = dsProps.getBoolean(DataSource.ISCOMPOSITE,
                "false", false);

        String fullState = String
                .format("%s%s", dsProps.getString(DataSource.STATE),
                        (dsProps.getInt(DataSource.PRECEDENCE) == -1
                                ? ":ARCHIVE "
                                : ""));

        String dsHeader = String.format("%s%s(%s:%s%s%s) %s", dsProps
                .getString("name"), modifiedSign(wasModified), String.format(
                "%s%s", (isComposite ? "composite " : ""),
                dsProps.getString(DataSource.ROLE)), fullState, failureInfo,
                additionalInfo, connectionStats);

        String alertMessage = dsProps.getString(DataSource.ALERT_MESSAGE, "",
                false);
        alertMessage = (alertMessage.length() > 0 ? String.format(
                "\nREASON[%s]", alertMessage) : alertMessage);
        String dsAlert = String.format("STATUS [%s] %s%s%s", dsProps
                .getString(DataSource.ALERT_STATUS), dateFormat
                .format((new Date(dsProps.getLong(DataSource.ALERT_TIME)))),
                replicator_useSSLConnection, alertMessage);

        if (!printDetails)
        {
            if (!isRawFormat)
            {
                builder.append(
                        ResultFormatter.makeSeparator(
                                ResultFormatter.DEFAULT_WIDTH, 1, true))
                        .append(NEWLINE);
                builder.append(ResultFormatter.makeRow(
                        (new String[]{dsHeader}),
                        ResultFormatter.DEFAULT_WIDTH, indentToUse, true, true));

                if (vipInfo != null)
                {
                    builder.append(ResultFormatter.makeRow(
                            (new String[]{vipInfo}),
                            ResultFormatter.DEFAULT_WIDTH, indentToUse, true,
                            true));
                }

                builder.append(ResultFormatter.makeRow((new String[]{dsAlert}),
                        ResultFormatter.DEFAULT_WIDTH, indentToUse, true, true));
                builder.append(
                        ResultFormatter.makeSeparator(
                                ResultFormatter.DEFAULT_WIDTH, 1, true))
                        .append(NEWLINE);
            }
            else
            {
                builder.append(dsHeader).append(NEWLINE);
                if (vipInfo != null)
                {
                    builder.append(vipInfo).append(NEWLINE);
                }
                builder.append(dsAlert).append(NEWLINE);
            }

            if (!includeComponents)
            {
                return builder.toString();
            }

            if (!isComposite)
            {
                String managerStatus = String.format("MANAGER(state=%s)",
                        managerIsOnline ? "ONLINE" : "STOPPED");

                String replicatorStatus = formatReplicatorProps(replProps,
                        managerIsOnline, header, printDetails);

                String dbState = (dbProps != null
                        ? dbProps.getString("state")
                        : "UNKNOWN");

                String dbHeader = String.format("DATASERVER(state=%s)\n",
                        dbState);

                String usage = null;

                if (routerUsage != null)
                {
                    usage = String
                            .format("CONNECTIONS(created=%s, active=%s)",
                                    routerUsage
                                            .getString(DataSource.CONNECTIONS_CREATED_COUNT),
                                    routerUsage
                                            .getString(DataSource.ACTIVE_CONNECTION_COUNT));
                }

                if (!isRawFormat)
                {

                    builder.append(ResultFormatter.makeRow(new String[]{indent
                            + managerStatus}, ResultFormatter.DEFAULT_WIDTH, 0,
                            false, true));

                    builder.append(ResultFormatter.makeRow(new String[]{indent
                            + replicatorStatus}, ResultFormatter.DEFAULT_WIDTH,
                            0, false, true));

                    builder.append(ResultFormatter.makeRow((new String[]{indent
                            + dbHeader}), ResultFormatter.DEFAULT_WIDTH,
                            indentToUse, true, true));
                    if (routerUsage != null)
                    {
                        builder.append(ResultFormatter.makeRow(
                                (new String[]{indent + usage}),
                                ResultFormatter.DEFAULT_WIDTH, indentToUse,
                                true, true));

                    }

                    builder.append(
                            ResultFormatter.makeSeparator(
                                    ResultFormatter.DEFAULT_WIDTH, 1, true))
                            .append(NEWLINE);
                }
                else
                {
                    builder.append(managerStatus).append(NEWLINE);
                    builder.append(replicatorStatus).append(NEWLINE);
                    builder.append(dbHeader).append(NEWLINE);
                    if (usage != null)
                    {
                        builder.append(usage).append(NEWLINE);
                    }

                }
            }

            builder.append(NEWLINE);

            return builder.toString();

        }

        // DETAILS:
        builder.append(formatMap(dsHeader, dsProps.map(), "", "", false));

        if (!isComposite)
        {

            if (replProps != null)
            {
                String replHeader = null;

                if (replProps.getString(Replicator.STATE).equals(
                        ResourceState.STOPPED.toString()))
                {
                    replHeader = String.format("%s:REPLICATOR(state=STOPPED)",
                            replProps.getString("host"));
                }
                else
                {
                    replHeader = String.format(
                            "%s:REPLICATOR(role=%s, state=%s)",
                            replProps.getString("host"),
                            replProps.getString("role"),
                            replProps.getString("state"));
                }

                builder.append(formatMap(replHeader, replProps.map(), "", "  ",
                        false));

            }

            if (dbProps != null)
            {
                String dbHeader = String.format("%s:DATASERVER(state=%s)",
                        dsProps.get("host"), dbProps.getString("state"));

                builder.append(formatMap(dbHeader, dbProps.map(), "", "  ",
                        false));

            }
        }

        builder.append(NEWLINE);
        return builder.toString();
    }

    public static String formatHeaderRow(String header)
    {
        StringBuilder builder = new StringBuilder();

        builder.append(
                ResultFormatter.makeSeparator(ResultFormatter.DEFAULT_WIDTH, 1,
                        true)).append(NEWLINE);
        builder.append(ResultFormatter.makeRow((new String[]{header}),
                ResultFormatter.DEFAULT_WIDTH, 0, true, true));

        builder.append(
                ResultFormatter.makeSeparator(ResultFormatter.DEFAULT_WIDTH, 1,
                        true)).append(NEWLINE);

        return builder.toString();

    }

    public static String formatReplicatorProps(TungstenProperties replProps,
            boolean managerIsOnline, String header, boolean printDetails)
    {

        if (replProps == null)
        {
            if (managerIsOnline)
                return "REPLICATOR(state=STOPPED)";
            else
                return "REPLICATOR(state=STATUS NOT AVAILABLE)";
        }
        else if (replProps.getString(Replicator.STATE).equals(
                ResourceState.STOPPED.toString()))
        {
            return "REPLICATOR(state=STOPPED)";
        }
        String indent = "\t";
        StringBuilder builder = new StringBuilder();

        String role = replProps.getString(Replicator.ROLE);
        String masterReplicator = "";

        if (role.equals("slave") || role.equals("relay"))
        {
            final String prefix = "thl://";

            String masterUri = replProps
                    .getString(Replicator.MASTER_CONNECT_URI);

            if (masterUri != null)
            {
                // don't display the port
                int lastIdx = masterUri.indexOf(":", prefix.length());

                // if we don't have a ':' at the end, maybe a '/'?
                if (lastIdx == -1)
                {
                    lastIdx = masterUri.indexOf("/", prefix.length());
                }

                // If we don't have either, we just go to the end of the string
                if (lastIdx == -1)
                {
                    lastIdx = masterUri.length();
                }

                masterReplicator = ", master="
                        + masterUri.substring(masterUri.indexOf("//") + 2,
                                lastIdx);
            }
        }
        builder.append(String.format("REPLICATOR(role=%s%s, state=%s)",
                replProps.getString("role"), masterReplicator,
                ReplicatorNotification.replicatorStateToResourceState(replProps
                        .getString("state"))));

        TreeMap<String, String> sortedMap = new TreeMap<String, String>();
        sortedMap.putAll(replProps.map());

        if (!printDetails)
        {
            return builder.toString();
        }

        builder.append("\n").append(header);
        builder.append("{").append("\n");
        builder.append(header);
        builder.append(
                String.format("%shost = %s", indent,
                        replProps.getString("host"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%sminSeqNo = %s", indent,
                        replProps.getString("minSeqNo"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%smaxSeqNo = %s", indent,
                        replProps.getString("maxSeqNo"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%smasterUri = %s", indent,
                        replProps.getString("masterUri"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%suptimeSeconds = %s", indent,
                        replProps.getString("uptimeSeconds"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%spendingExceptionMessage = %s", indent,
                        replProps.getString("pendingExceptionMessage")))
                .append("\n");
        builder.append(header);
        builder.append(
                String.format("%spendingErrorCode = %s", indent,
                        replProps.getString("pendingErrorCode"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%spendingError = %s", indent,
                        replProps.getString("pendingError"))).append("\n");

        builder.append(header);
        builder.append(String.format("}"));
        return builder.toString();
    }

    public static String formatAllReplicatorProps(TungstenProperties replProps,
            String header, boolean printDetails)
    {

        if (replProps == null)
        {
            return "REPLICATOR(state=STATUS NOT AVAILABLE)";
        }
        else if (replProps.getString(Replicator.STATE).equals(
                ResourceState.STOPPED.toString()))
        {
            return "REPLICATOR(state=STOPPED)";
        }
        String indent = "\t";
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("REPLICATOR(role=%s, state=%s)",
                replProps.getString("role"), replProps.getString("state")));

        TreeMap<String, String> sortedProps = new TreeMap<String, String>();
        sortedProps.putAll(replProps.map());

        if (!printDetails)
        {
            return builder.toString();
        }

        builder.append("\n").append(header);
        builder.append("{").append("\n");

        for (String key : sortedProps.keySet())
        {

            builder.append(header);
            builder.append(
                    String.format("%s%s = %s", indent, key,
                            sortedProps.get(key))).append("\n");
        }

        builder.append(header);
        builder.append(String.format("}"));
        return builder.toString();
    }

    static public String modifiedSign(boolean wasModified)
    {
        return ((wasModified ? "*" : ""));
    }

    static public void formatStatistics(TungstenProperties dsProps,
            StringBuilder builder, String header, String indent)
    {
        builder.append(
                String.format("%sactiveConnectionCount = %s", indent,
                        dsProps.getObject("activeConnectionCount"))).append(
                "\n");
        builder.append(header);
        builder.append(
                String.format("%sconnectionsCreatedCount = %s", indent,
                        dsProps.getObject("connectionsCreatedCount"))
                        .toString()).append("\n");
        builder.append(header);
        builder.append(
                String.format("%sstatementsCreatedCount = %s", indent,
                        dsProps.getObject("statementsCreatedCount")).toString())
                .append("\n");
        builder.append(header);
        builder.append(
                String.format("%spreparedStatementsCreatedCount = %s", indent,
                        dsProps.getObject("preparedStatementsCreatedCount"))
                        .toString()).append("\n");
        builder.append(header);
        builder.append(
                String.format("%scallableStatementsCreatedCount = %s", indent,
                        dsProps.getObject("callableStatementsCreatedCount"))
                        .toString()).append("\n");
        builder.append(header);

    }

    /**
     * Temporary utility method to keep current println behavior.
     * 
     * @param msg
     */
    static public void println(String msg)
    {
        println(msg, CLLogLevel.normal);
    }

    // Print a message to stdout.
    static public void println(String msg, CLLogLevel level)
    {
        if (msg == null)
            return;

        if (level.getLevel() > logLevel.getLevel())
        {
            return;
        }

        if (msg.length() > 0)
        {
            if (level.getLevel() >= CLLogLevel.detailed.getLevel())
            {
                msg = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss ")
                        .format(new Date()) + msg;
            }
            if (level.getLevel() >= CLLogLevel.debug.getLevel())
            {
                msg = String.format("%s: %s", level.toString().toUpperCase(),
                        msg);
            }

            System.out.println(msg);
        }
    }

    // Print a formatted message to stdout.
    static public void printf(String fmt, Object... args)
    {
        System.out.printf(fmt, args);
    }

    // Print a message to stdout without a newline
    static public void print(String msg)
    {
        if (msg.length() > 0)
            System.out.print(msg);
    }

    // Print an error.
    static public void error(String msg, Throwable t)
    {
        println("ERROR: " + msg);
        if (t != null)
            t.printStackTrace();
    }

    // Abort following a fatal error.
    static public void fatal(String msg, Throwable t)
    {
        println(msg);
        if (t != null)
            t.printStackTrace();
        System.exit(1);
    }

    public static void printDataService(
            Map<String, TungstenProperties> dataSourceProps, String[] args,
            boolean useRelativeLatency)
    {
        boolean printDetail = false;

        if (dataSourceProps == null)
        {
            System.out.println("CLUSTER UNAVAILABLE\n");
        }

        if (args.length > 1 && args[1].equals("-l"))
        {
            printDetail = true;
        }

        for (String dsName : dataSourceProps.keySet())
        {
            if (args.length >= 3)
            {
                if (!dsName.equals(args[2]))
                    continue;
            }

            printDataSource(dataSourceProps.get(dsName), "", printDetail,
                    useRelativeLatency);
        }

    }

    public static void printDataSource(TungstenProperties dsProperties,
            String header, boolean printDetails, boolean useRelativeLatency)
    {
        println(formatStatus(dsProperties, null, null, null, true, header,
                false, printDetails, printDetails, false, false,
                useRelativeLatency));
    }

    public static String printArgs(String args[])
    {
        return printArgs(args, 0);

    }

    public static String printArgs(String args[], int startElement)
    {

        StringBuffer buf = new StringBuffer();

        for (int i = startElement; i < args.length; i++)
        {
            if (buf.length() > 0)
            {
                buf.append(" ");
            }
            buf.append(args[i]);
        }

        return buf.toString();
    }

    public static TungstenProperties editProperties(TungstenProperties props,
            boolean isNew, BufferedReader in) throws IOException
    {
        boolean wasModified = false;

        ConsoleReader newDSReader = new ConsoleReader();

        List<Completor> comps = new LinkedList<Completor>();

        // Add a choice for each property setting.
        // Complete with currently set value so user can edit it easily
        for (String key : props.hashMap().keySet())
        {
            List<Completor> completor = new LinkedList<Completor>();
            completor.add(new SimpleCompletor(key));
            completor.add(new SimpleCompletor(props.getString(key)));
            completor.add(new NullCompletor());
            comps.add(new ArgumentCompletor(completor));
        }

        // Add commit and rollback keywords
        comps.add(new SimpleCompletor(new String[]{COMMAND_COMMIT,
                COMMAND_ROLLBACK}));

        newDSReader.addCompletor(new MultiCompletor(comps));

        String[] args = null;

        while ((args = getInputTokens(
                newDSReader,
                String.format("edit %s> ", props.getString("name"),
                        CLUtils.modifiedSign(wasModified || isNew)), in)) != null)
        {
            if (COMMAND_QUIT.equals(args[0]))
            {
                if (wasModified)
                {
                    CLUtils.println("Please either commit or rollback changes before quitting");
                    continue;
                }
                break;
            }
            else if (COMMAND_COMMIT.equals(args[0]))
            {
                break;
            }
            else if (COMMAND_ROLLBACK.equals(args[0]))
            {
                props = null;
                break;
            }
            // list current properties
            else if (COMMAND_LIST.equals(args[0]))
            {
                // for (String key : dsProps.keySet())
                println(formatProperties(props.getString("name"), props, "",
                        wasModified));
            }
            // not enough args or key not present in the predefined settings
            else if (args.length != 2 || props.getString(args[0]) == null)
            {
                CLUtils.println("Usage: <attribute> <new value> (example: \"role master\")");
                CLUtils.println("       or use 'rollback' or 'commit' to complete your work");
            }
            // set a property
            else
            {
                props.setString(args[0], args[1]);
                wasModified = true;
                CLUtils.println(CLUtils.formatProperties(
                        props.getString("name"), props, "", wasModified));
            }
        }

        if (!wasModified && !isNew)
            return null;

        return props;
    }

    public static String[] appendArg(String args[], String newArg)
    {
        ArrayList<String> newArgs = new ArrayList<String>();
        for (String arg : args)
            newArgs.add(arg);

        newArgs.add(newArg);

        return newArgs.toArray(new String[newArgs.size()]);
    }

    public static String[] prependArg(String args[], String newArg)
    {
        ArrayList<String> newArgs = new ArrayList<String>();
        newArgs.add(newArg);
        for (String arg : args)
            newArgs.add(arg);

        return newArgs.toArray(new String[newArgs.size()]);
    }

    public static String listToString(List<Object> list)
    {
        StringBuilder builder = new StringBuilder();

        for (Object obj : list)
        {
            builder.append(String.format("%s\n", obj.toString()));
        }

        return builder.toString();
    }

    /**
     * This method will format any iterable class into a comma-separated list.
     * 
     * @param iterable An iterable value.
     * @return formatted string
     */
    public static String iterableToCommaSeparatedList(Iterable<?> iterable)
    {
        StringBuilder builder = new StringBuilder();
        boolean first = true;

        for (Object obj : iterable)
        {
            if (first)
            {
                builder.append(String.format("%s", obj.toString()));
                first = false;
            }
            else
            {
                builder.append(String.format(", %s", obj.toString()));
            }
        }
        return builder.toString();
    }

    /**
     * This method will format any iterable class into a simple newline
     * delimited list.
     * 
     * @param iterable
     * @return formatted string
     */
    public static String iterableToString(Iterable<?> iterable)
    {
        StringBuilder builder = new StringBuilder();

        for (Object obj : iterable)
        {
            builder.append(String.format("%s\n", obj.toString()));
        }

        return builder.toString();

    }

    public static String stringListToString(List<String> list)
    {
        StringBuilder builder = new StringBuilder();

        for (String obj : list)
        {
            builder.append(String.format("%s\n", obj.toString()));
        }

        return builder.toString();
    }

    public static String stringCollectionToString(Collection<String> list)
    {
        StringBuilder builder = new StringBuilder();

        for (String obj : list)
        {
            builder.append(String.format("%s\n", obj.toString()));
        }

        return builder.toString();
    }

    public static String collectionToString(Collection<Object> collection)
    {
        StringBuilder builder = new StringBuilder();

        for (Object obj : collection)
        {
            builder.append(String.format("%s\n", obj.toString()));
        }

        return builder.toString();
    }
}
