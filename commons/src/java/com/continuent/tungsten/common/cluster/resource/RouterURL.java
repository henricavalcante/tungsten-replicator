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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.common.cluster.resource;

import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.log4j.Logger;

/**
 * Implements a simple parser for SQLRouter URLs. It identifies and strips out
 * t-router properties
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class RouterURL implements Cloneable
{
    private static final String URL_OPTIONS_DELIMITERS       = "&=?";

    private static Logger       logger                       = Logger.getLogger(RouterURL.class);

    private static final String URL_ELMT_JDBC                = "jdbc";
    private static final String URL_ELMT_TROUTER             = "t-router";
    public static final String  URL_FULL_HEADER              = URL_ELMT_JDBC
                                                                     + ':'
                                                                     + URL_ELMT_TROUTER
                                                                     + "://";
    // Keys for specific, internal use, connection properties
    public static final String  KEY_MAX_APPLIED_LATENCY      = "maxAppliedLatency";
    public static final String  KEY_QOS                      = "qos";
    public static final String  KEY_SESSION_ID               = "sessionId";
    public static final String  KEY_AFFINITY                 = "affinity";
    public static final String  KEY_USER                     = "user";
    public static final String  KEY_PASSWORD                 = "password";

    /** How the password will be displayed when hiding it */
    private static final String OBFUSCATED_PASSWORD          = "<obfuscated>";

    // various predefined session ids
    public static final String  SESSIONID_CONNECTION         = "CONNECTION";
    public static final String  SESSIONID_DATABASE           = "DATABASE";
    public static final String  SESSIONID_USER               = "USER";
    public static final String  SESSIONID_PROVIDED_IN_DBNAME = "PROVIDED_IN_DBNAME";

    /** Special tag to be replaced by a database name dynamically */
    public static final String  DBNAME_TOKEN                 = "${DBNAME}";

    // Parsed URL data
    private String              dataServiceName              = "UNDEFINED";
    private String              dbname                       = "";
    private QualityOfService    qos                          = QualityOfService.RW_STRICT;
    public static double        MAX_APPLIED_LATENCY_UNDEF    = -1;
    private double              maxAppliedLatency            = MAX_APPLIED_LATENCY_UNDEF;
    private String              sessionId                    = null;
    private String              affinity                     = null;

    private boolean             autoSession                  = false;
    /**
     * These properties hold only the non-RouterURL settings, ie. the JDBC
     * driver specific ones
     */
    private Properties          props                        = new Properties();

    // Parsing information.
    // TUC-1065: don't store the original URL, this would double the information
    /**
     * After parsing, this will store the last position character position of
     * the URL base ([jdbc:t-router://<service>/<db>]<opts>)
     */
    private int                 urlBaseEndIndex              = 0;

    /**
     * Creates a parsed URL object.<br>
     * Valid driver URLs are: jdbc:t-router://service/<database-name>
     * [?][qos={RW_STRICT|RW_RELAXED|RO_STRICT|RO_RELAXED}]
     * [?|&][otheroptions=value...]<br>
     * The default qos (Quality of service) is RW_STRICT unless specified or
     * unless overridden by the service configuration.<br>
     * Properties passed via the URL supersede the ones in the properties. Last
     * property defined in the URL wins.
     * 
     * @param url SQL router URL
     * @param info Properties for URL
     * @throws SQLException if the URL cannot be parsed or misses the data
     *             service name or database name
     */
    public RouterURL(String url, Properties info) throws SQLException
    {
        parseUrl(url, info);
    }

    /**
     * Parses the driver URL and extracts the properties.
     * 
     * @param url the URL to parse
     * @param info any existing properties already loaded in a
     *            <code>Properties</code> object
     * @throws SQLException if the URL is not correctly formed or if the data
     *             service name or database name are missing
     */
    private void parseUrl(String url, Properties info) throws SQLException
    {
        // TUC-1065: don't store the original URL, this would double the
        // information

        // Record the parsing position
        this.urlBaseEndIndex = 0;

        // Add input properties if supplied.
        if (info != null)
            props.putAll(info);

        // Skip jdbc protocol.
        if (!"jdbc".equalsIgnoreCase(nextUrlBaseToken(url)))
        {
            throw new SQLException("URL lacks 'jdbc' protocol: " + url);
        }

        // Skip sub-protocol.
        if (!"t-router".equalsIgnoreCase(nextUrlBaseToken(url)))
        {
            throw new SQLException("URL lacks 't-router' sub-protocol: " + url);
        }

        // Get the service name.
        dataServiceName = nextUrlBaseToken(url);
        if (dataServiceName == null)
        {
            throw new SQLException("Missing data service name in URL: " + url);
        }
        // Get the database name.
        dbname = nextUrlBaseToken(url);
        if (dbname == null)
        {
            dbname = "";
        }
        parseURLOptions(url.substring(urlBaseEndIndex));

        if (logger.isDebugEnabled())
        {
            logger.debug("Parsed t-router URL: " + toString());
        }
    }

    public void parseURLOptions(String substring) throws SQLException
    {
        urlOptionsToProperties(substring, props);
        transferRouterPropertiesToMemberVariables();
    }

    /**
     * Given a string of URL options (eg. affinity=blah&maxAppliedLatency=2),
     * extracts each option and add them to the given Properties parameter.
     * 
     * @param urlOptions string to parse
     * @param p output properties to which options will be added, overwriting
     *            them if already in
     * @throws SQLException in case of parsing error
     */
    public static void urlOptionsToProperties(String urlOptions, Properties p)
            throws SQLException
    {
        String key;
        StringTokenizer st = new StringTokenizer(urlOptions,
                URL_OPTIONS_DELIMITERS);
        while (st.hasMoreTokens())
        {
            key = st.nextToken();
            if (!st.hasMoreTokens())
            {
                throw new SQLException("Invalid empty value for property '"
                        + key + "' in URL: " + urlOptions);
            }
            String value = st.nextToken();
            p.setProperty(key, value);
        }
    }

    /**
     * Iterates through all properties and removes the ones that are router-
     * specific. Sets the member variables with these values
     * 
     * @throws SQLException upon illegal values
     */
    protected void transferRouterPropertiesToMemberVariables()
            throws SQLException
    {
        transferPropertiesToRouterURLMemberVariables(props);
    }

    public void transferPropertiesToRouterURLMemberVariables(Properties propsArg)
            throws SQLException
    {
        // If QOS is among the properties, set it explicitly and remove it from
        // there
        String qosValue = (String) propsArg.remove(KEY_QOS);
        if (qosValue != null)
        {
            try
            {
                qos = QualityOfService
                        .valueOf(QualityOfService.class, qosValue);

            }
            catch (IllegalArgumentException i)
            {
                StringBuilder msg = new StringBuilder();
                msg.append("Invalid value '").append(qosValue)
                        .append("' passed for the quality of service.")
                        .append(" Valid values are: ");
                for (QualityOfService q : QualityOfService.values())
                {
                    msg.append(q.toString()).append(' ');
                }
                throw new SQLException(msg.toString());
            }
        }
        // Same for max latency...
        String maxAppliedLatencyValue = (String) propsArg
                .remove(KEY_MAX_APPLIED_LATENCY);
        if (maxAppliedLatencyValue != null)
        {
            try
            {
                this.maxAppliedLatency = Double
                        .parseDouble(maxAppliedLatencyValue);
            }
            catch (NumberFormatException nfe)
            {
                logger.warn("URL option maxAppliedLatency value "
                        + maxAppliedLatencyValue
                        + " could not be parsed correctly - defaulting to -1 (undef)");

                this.maxAppliedLatency = MAX_APPLIED_LATENCY_UNDEF;
            }
        }
        // ...for affinity...
        String affinityInProps = (String) propsArg.remove(KEY_AFFINITY);
        if (affinityInProps != null)
        {
            this.affinity = affinityInProps;
        }

        // ...and for session ID
        String propsSessionId = (String) propsArg.remove(KEY_SESSION_ID);
        if (propsSessionId != null)
        {
            boolean wasAutoSession = isAutoSession();
            autoSession = false;
            if (propsSessionId.equals(SESSIONID_CONNECTION)
                    ||
                    // smartScale Fall back when no sessionId is given
                    propsSessionId.equals(SESSIONID_PROVIDED_IN_DBNAME))
            {
                autoSession = true;
                // generate a session id only if not already generated
                // previously
                if (!wasAutoSession)
                {
                    sessionId = UUID.randomUUID().toString();
                }
            }
            else if (propsSessionId.equals(SESSIONID_DATABASE))
            {
                autoSession = true;
                if (dbname != null)
                {
                    sessionId = dbname;
                }
                else
                {
                    throw new SQLException(
                            "You must supply a database name to use the "
                                    + "DATABASE based sessionId");
                }
            }
            else if (propsSessionId.equals(SESSIONID_USER))
            {
                autoSession = true;
                String user = getProperty(KEY_USER);
                if (user != null)
                {
                    sessionId = user;
                }
                else
                {
                    throw new SQLException(
                            "You must supply a user name for the URL property "
                                    + "'user' to use the USER based sessionId");
                }
            }
            else
            {
                sessionId = propsSessionId;
            }
        }
    }

    /**
     * Extracts the next lexical token from the provided URL base.
     * 
     * @param url The URL being parsed
     * @param pos The current position in the URL string.
     * @return The next string until one of the following character is found:
     *         '?' ';' ':' '/', or null if no token (ie. empty token) was found
     */
    private String nextUrlBaseToken(String url)
    {
        StringBuffer token = new StringBuffer();

        while (urlBaseEndIndex < url.length())
        {
            char ch = url.charAt(urlBaseEndIndex++);

            if (ch == ':' || ch == ';' || ch == '?')
            {
                break;
            }

            if (ch == '/')
            {
                if (urlBaseEndIndex < url.length()
                        && url.charAt(urlBaseEndIndex) == '/')
                {
                    urlBaseEndIndex++;
                    continue;
                }
                else
                {
                    break;
                }
            }
            token.append(ch);
        }

        if (token.length() == 0)
            return null;
        else
            return token.toString();
    }

    public void setProperties(String propsStr) throws SQLException
    {
        String key;
        StringTokenizer st = new StringTokenizer(propsStr,
                URL_OPTIONS_DELIMITERS);
        while (st.hasMoreTokens())
        {
            key = st.nextToken();
            if (!st.hasMoreTokens())
            {
                throw new SQLException("Invalid empty value for property '"
                        + key + "' in properties string: " + propsStr);
            }
            String value = st.nextToken();
            props.setProperty(key, value);
        }
        transferRouterPropertiesToMemberVariables();
    }

    /**
     * Returns the dataServiceName value.
     * 
     * @return Returns the dataServiceName.
     */
    public String getDataServiceName()
    {
        return dataServiceName;
    }

    /**
     * Changes the database name. This triggers a replacement of the database
     * name in the URL string as well
     * 
     * @param dbname The dbname to set.
     */
    public void setDbname(String dbname)
    {
        this.dbname = dbname;
    }

    public double getMaxAppliedLatency()
    {
        return maxAppliedLatency;
    }

    public void setMaxAppliedLatency(double maxAppliedLatencyPrm)
    {
        maxAppliedLatency = maxAppliedLatencyPrm;
    }

    /**
     * Upon catalog change, it can be required to change the session ID
     * 
     * @param newSessionId
     */
    public void setSessionId(String newSessionId)
    {
        sessionId = newSessionId;
    }

    public String getSessionId()
    {
        return sessionId;
    }

    public String getService()
    {
        return dataServiceName;
    }

    public String getDbname()
    {
        return dbname;
    }

    public Properties getProps()
    {
        return props;
    }

    public QualityOfService getQos()
    {
        return qos;
    }

    public void setQoS(QualityOfService qosParam)
    {
        qos = qosParam;
    }

    public String getAffinity()
    {
        return affinity;
    }

    public void setAffinity(String affinityParam)
    {
        affinity = affinityParam;
    }

    /**
     * Provides the property with the given key from the parsed and passed URL
     * props. If the property is not set, returns null
     * 
     * @param key the property key to retrieve
     * @return the given property or null if no such property exists
     */
    public String getProperty(String key)
    {
        return props.getProperty(key);
    }

    public Properties getObfuscatedPasswordPropsCopy()
    {
        Properties obfuscatedPasswordProps = (Properties) props.clone();
        obfuscatedPasswordProps.setProperty(KEY_PASSWORD, OBFUSCATED_PASSWORD);
        return obfuscatedPasswordProps;
    }

    public String toString()
    {
        Properties obfuscatedPasswordProps = getObfuscatedPasswordPropsCopy();
        StringBuilder sb = new StringBuilder(URL_FULL_HEADER);
        sb.append(dataServiceName).append('/').append(dbname).append(" QoS=")
                .append(qos).append(" sessionId=").append(sessionId)
                .append(" maxAppliedLatency=").append(maxAppliedLatency)
                .append(" affinity=").append(affinity)
                .append(" jdbc driver options=")
                .append(obfuscatedPasswordProps);
        return sb.toString();
    }

    public boolean isAutoSession()
    {
        return autoSession;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || !(obj instanceof RouterURL))
        {
            return false;
        }
        RouterURL compareTo = (RouterURL) obj;
        // data service name can't be null, or that would have thrown an error
        // in the constructor
        if (!dataServiceName.equals(compareTo.dataServiceName))
        {
            return false;
        }
        // database name can't be null, or that would have thrown an error
        // in the constructor
        if (!dbname.equals(compareTo.dbname))
        {
            return false;
        }
        // props can't be null, they are initialized at construction time
        return props.equals(compareTo.props);
    }

    @Override
    public Object clone() throws CloneNotSupportedException
    {
        RouterURL clone = null;
        try
        {
            clone = new RouterURL(URL_FULL_HEADER + dataServiceName + '/'
                    + dbname, props);
        }
        catch (SQLException sqle)
        {
            throw new CloneNotSupportedException(
                    "Failed to create clone because of "
                            + sqle.getLocalizedMessage());
        }
        clone.maxAppliedLatency = this.maxAppliedLatency;
        clone.affinity = this.affinity;
        clone.autoSession = this.autoSession;
        clone.urlBaseEndIndex = this.urlBaseEndIndex;
        clone.qos = this.qos;
        clone.sessionId = this.sessionId;

        return clone;
    }
}