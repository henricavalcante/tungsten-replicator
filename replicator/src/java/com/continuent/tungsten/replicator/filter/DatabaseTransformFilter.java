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
 * Contributor(s): Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.filter;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to transform a specific database name to a new value using Java
 * regular expression rules. This filter matches the schema name using the
 * fromRegex expression and then does a replacement on the name using the
 * toRegex expression.<br/>
 * Can also be used to transform table names instead by turning on
 * transformTables flag. However, statement data transformation is unsupported
 * in this case.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 * @see java.util.regex.Pattern
 * @see java.util.regex.Matcher
 */
public class DatabaseTransformFilter implements Filter
{
    private static Logger logger = Logger.getLogger(LoggingFilter.class);

    private String        fromRegex1;
    private String        toRegex1;

    Pattern               pattern1;
    Matcher               matcher1;

    private String        fromRegex2;
    private String        toRegex2;

    Pattern               pattern2;
    Matcher               matcher2;

    private String        fromRegex3;
    private String        toRegex3;

    Pattern               pattern3;
    Matcher               matcher3;

    private String        fromRegex4;
    private String        toRegex4;

    Pattern               pattern4;
    Matcher               matcher4;

    private boolean       transformTables = false;

    /** Sets the regex used to match the database name. */
    public void setFromRegex1(String fromRegex)
    {
        this.fromRegex1 = fromRegex;
    }

    /** Sets the corresponding regex to transform the name. */
    public void setToRegex1(String toRegex)
    {
        this.toRegex1 = toRegex;
    }

    /** Sets the regex used to match the database name. */
    public void setFromRegex2(String fromRegex)
    {
        this.fromRegex2 = fromRegex;
    }

    /** Sets the corresponding regex to transform the name. */
    public void setToRegex2(String toRegex)
    {
        this.toRegex2 = toRegex;
    }

    /** Sets the regex used to match the database name. */
    public void setFromRegex3(String fromRegex)
    {
        this.fromRegex3 = fromRegex;
    }

    /** Sets the corresponding regex to transform the name. */
    public void setToRegex3(String toRegex)
    {
        this.toRegex3 = toRegex;
    }

    /** Sets the regex used to match the database name. */
    public void setFromRegex4(String fromRegex)
    {
        this.fromRegex4 = fromRegex;
    }

    /** Sets the corresponding regex to transform the name. */
    public void setToRegex4(String toRegex)
    {
        this.toRegex4 = toRegex;
    }
    
    /** Transform table instead of database names. */
    public void setTransformTables(boolean transformTables)
    {
        this.transformTables = transformTables;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        for (DBMSData dataElem : data)
        {
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges())
                {
                    if (!updateRowChange(orc, matcher1, toRegex1)
                            && !updateRowChange(orc, matcher2, toRegex2)
                            && !updateRowChange(orc, matcher3, toRegex3))
                        updateRowChange(orc, matcher4, toRegex4);
                }
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;
                if (!updateStatementData(sdata, matcher1, toRegex1)
                        && !updateStatementData(sdata, matcher2, toRegex2)
                        && !updateStatementData(sdata, matcher3, toRegex3))
                    updateStatementData(sdata, matcher4, toRegex4);
            }
        }
        return event;
    }

    /**
     * updateStatementData updates the schema name of the given StatementData
     * object if it matches the regular expression.<br/>
     * If transformTables flag is set, this does nothing.
     * 
     * @param sdata the StatementData object to process
     * @param matcher the matcher used to check the schema name
     * @param toRegex the name of the new schema to use
     * @return true if nothing more is expected to be done on the StatementData,
     *         either because it matched the regular expression or because the
     *         matcher or the schema were null (matcher is null if the
     *         corresponding FromRegex property was not set)
     */
    private boolean updateStatementData(StatementData sdata, Matcher matcher,
            String toRegex)
    {
        // Table transformation is unsupported for StatementData.
        if (transformTables)
            return true;

        if (matcher == null)
            return true;

        String schema = sdata.getDefaultSchema();
        if (schema == null)
            return true;

        matcher.reset(schema);

        if (matcher.matches())
        {
            String oldSchema = schema;
            sdata.setDefaultSchema(matcher.replaceAll(toRegex));
            if (logger.isDebugEnabled())
                logger.debug("Filtered event schema name: old=" + oldSchema
                        + " new=" + sdata.getDefaultSchema());
            return true;
        }
        return false;
    }

    /**
     * updateRowChange updates the schema name of the given OneRowChange object
     * if it matches the regular expression.
     * 
     * @param orc the OneRowChange object to process
     * @param matcher the matcher used to check the schema name
     * @param toRegex the name of the new schema to use
     * @return true if nothing more is expected to be done on the OneRowChange,
     *         either because it matched the regular expression or because the
     *         matcher was null (matcher is null if the corresponding FromRegex
     *         property was not set)
     */
    private boolean updateRowChange(OneRowChange orc, Matcher matcher,
            String toRegex)
    {
        if (matcher == null)
            return true;

        if (transformTables)
            matcher.reset(orc.getTableName());
        else
            matcher.reset(orc.getSchemaName());

        if (matcher.matches())
        {
            if (transformTables)
            {
                String oldTable = orc.getTableName();
                orc.setTableName(matcher.replaceAll(toRegex));
                if (logger.isDebugEnabled())
                    logger.debug("Filtered event table name: old=" + oldTable
                            + " new=" + orc.getTableName());
            }
            else
            {
                String oldSchema = orc.getSchemaName();
                orc.setSchemaName(matcher.replaceAll(toRegex));
                if (logger.isDebugEnabled())
                    logger.debug("Filtered event schema name: old=" + oldSchema
                            + " new=" + orc.getSchemaName());
            }
            return true;
        }

        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        if (fromRegex1 == null)
            throw new ReplicatorException(
                    "fromRegex1 property must be set for regex filter to work");
        if (toRegex1 == null)
            throw new ReplicatorException(
                    "toRegex1 property must be set for regex filter to work");

        if (fromRegex2 != null && toRegex2 == null)
            throw new ReplicatorException(
                    "toRegex2 property must be set if fromRegex2 is set");

        if (fromRegex3 != null)
            if (toRegex3 == null)
                throw new ReplicatorException(
                        "toRegex3 property must be set if fromRegex3 is set");
            else if (fromRegex2 == null)
                throw new ReplicatorException(
                        "fromRegex2 should be defined instead of using fromRegex3");

        if (fromRegex4 != null)
            if (toRegex4 == null)
                throw new ReplicatorException(
                        "toRegex4 property must be set if fromRegex4 is set");
            else if (fromRegex3 == null)
                throw new ReplicatorException(
                        "fromRegex3 should be defined instead of using fromRegex4");
            else if (fromRegex2 == null)
                throw new ReplicatorException(
                        "fromRegex2 should be defined instead of using fromRegex4");

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Compile the pattern used for matching.
        try
        {
            pattern1 = Pattern.compile(fromRegex1);
            matcher1 = pattern1.matcher("");
        }
        catch (PatternSyntaxException e)
        {
            throw new ReplicatorException(
                    "Replicator fromRegex is invalid:  expression="
                            + fromRegex1, e);
        }

        if (fromRegex2 != null)
            // Compile the pattern used for matching.
            try
            {
                pattern2 = Pattern.compile(fromRegex2);
                matcher2 = pattern2.matcher("");
            }
            catch (PatternSyntaxException e)
            {
                throw new ReplicatorException(
                        "Replicator fromRegex2 is invalid:  expression="
                                + fromRegex2, e);
            }

        if (fromRegex3 != null)
            // Compile the pattern used for matching.
            try
            {
                pattern3 = Pattern.compile(fromRegex3);
                matcher3 = pattern3.matcher("");
            }
            catch (PatternSyntaxException e)
            {
                throw new ReplicatorException(
                        "Replicator fromRegex3 is invalid:  expression="
                                + fromRegex3, e);
            }

        if (fromRegex4 != null)
            // Compile the pattern used for matching.
            try
            {
                pattern4 = Pattern.compile(fromRegex4);
                matcher4 = pattern4.matcher("");
            }
            catch (PatternSyntaxException e)
            {
                throw new ReplicatorException(
                        "Replicator fromRegex4 is invalid:  expression="
                                + fromRegex4, e);
            }

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
    }
}
