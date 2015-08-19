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

package com.continuent.tungsten.replicator.filter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;

/**
 * SetToStringFilter transforms MySQL SET data type values to corresponding
 * string representation as follows:<br/>
 * 1. On each event it checks whether targeted table has SET data type column.<br/>
 * 2. If it does, corresponding SET column values of the event are mapped from
 * integer (bitmap) into string representations.<br/>
 * <br/>
 * The filter is to be used with row replication.<br/>
 * <br/>
 * Filter takes an optional parameter for performance tuning. Instead of
 * checking all the tables you may define only a specific comma-delimited list
 * in process_tables_schemas parameter. Eg.:<br/>
 * replicator.filter.settostringfilter.process_tables_schemas=myschema.mytable1
 * ,myschema.mytable2 <br/>
 * <br/>
 * This class is heavily based on EnumToStringFilter - only the core is
 * overridden.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class SetToStringFilter extends EnumToStringFilter
{
    private static Logger logger = Logger.getLogger(SetToStringFilter.class);

    /**
     * Overriding EnumToStringFilter core to parse SET definition.
     * 
     * @see SetToStringFilter#parseSet(String)
     */
    @Override
    protected String[] parseListType(String listTypeDefinition)
    {
        return parseSet(listTypeDefinition);
    }

    /**
     * Utility method for external callers.
     * 
     * @param setDefinition String of the following form: set('val1','val2',...)
     * @return Length of the largest element in given SET definition.
     */
    public static int largestElementLen(String setDefinition)
    {
        return largestElementLen(parseSet(setDefinition));
    }

    /**
     * Overriding EnumToStringFilter core to check for SET columns in the event.
     * If found, transforms values from integers to corresponding strings.
     */
    @Override
    protected void checkForListType(OneRowChange orc) throws SQLException,
            ReplicatorException
    {
        checkForListType(orc, "SET");
    }

    /**
     * Overriding EnumToStringFilter core to transform SET column values to
     * comma separated string representation.
     */
    @Override
    protected void transformColumns(ArrayList<ColumnSpec> columns,
            ArrayList<ArrayList<ColumnVal>> columnValues,
            HashMap<Integer, String[]> setDefinitions, String typeCaption)
            throws ReplicatorException
    {
        // Looping through all and checking the real underlying index of each,
        // because there might be gaps as an outcome of some other filters.
        for (int c = 0; c < columns.size(); c++)
        {
            ColumnSpec colSpec = columns.get(c);
            if (setDefinitions.containsKey(colSpec.getIndex()))
            {
                if (logger.isDebugEnabled())
                    logger.debug("Transforming " + typeCaption + "("
                            + colSpec.getIndex() + ")");
                if (colSpec.getType() == java.sql.Types.INTEGER /*
                                                                 * SET under
                                                                 * disguise
                                                                 */
                        || colSpec.getType() == java.sql.Types.NULL)
                {
                    // Change the underlying type in the event.
                    colSpec.setType(java.sql.Types.VARCHAR);

                    // Iterate through all rows in the event and transform each.
                    for (int row = 0; row < columnValues.size(); row++)
                    {
                        // ColumnVal keyValue = keyValues.get(row).get(k);
                        ColumnVal colValue = columnValues.get(row).get(c);
                        // It must be integer at this point.
                        if (colValue.getValue() != null)
                        {
                            long currentValue = (Long) colValue.getValue();
                            String setDefs[] = setDefinitions.get(colSpec
                                    .getIndex());
                            String newValue = null;
                            if (currentValue == 0)
                            {
                                // SET value of 0 means no options in the SET.
                                newValue = "";
                            }
                            else
                            {
                                newValue = binarySetMapToString(setDefs,
                                        currentValue);
                            }
                            colValue.setValue(newValue);
                            if (logger.isDebugEnabled())
                                logger.debug("Col " + colSpec.getIndex()
                                        + " Row " + row + ": " + currentValue
                                        + " -> " + newValue);
                        }
                        else
                        {
                            if (logger.isDebugEnabled())
                                logger.debug("Col " + colSpec.getIndex()
                                        + " Row " + row + ": null");
                        }
                    }
                }
                else if (colSpec.getType() == java.sql.Types.VARCHAR)
                    logger.warn("Column type is already VARCHAR! Assuming it is because this event was already transformed by this filter. Ignoring this column");
                else
                    logger.error("Unexpected column type ("
                            + colSpec.getType()
                            + ") in supposedly SET column! Ignoring this column");
            }
        }
    }

    /**
     * Parses MySQL SET type definition statement. Eg.:<br/>
     * set('a','b','c','d','e','f','g')<br/>
     * 
     * @param setDefinition String of the following form: set('val1','val2',...)
     * @return Set elements in an array. Unquoted. Eg.: a,b,c
     */
    public static String[] parseSet(String setDefinition)
    {
        return EnumToStringFilter.parseListDefString("set", setDefinition);
    }

    /**
     * Transforms a binary map encoded in a long type, eg.:<br/>
     * 011101<br/>
     * to a string with a comma-separated list of corresponding values, above
     * eg.: a,c,d,e
     * 
     * @see <a href="http://dev.mysql.com/doc/refman/5.0/en/set.html">MySQL SET
     *      type</a>
     * @param setDefs String values of each bit. For the above example:
     *            a,b,c,d,e,f.
     * @param currentValue A bitmap to decode.
     * @return A comma-separated list of decoded values, eg.: "a,c,d,e".
     */
    public static String binarySetMapToString(String[] setDefs,
            long currentValue)
    {
        StringBuilder list = new StringBuilder();
        String binary = Long.toBinaryString(currentValue);

        // Decode binary representation.
        for (int i = 0; i < binary.length(); i++)
        {
            if (binary.charAt(binary.length() - i - 1) == '1')
            {
                String option = setDefs[i];
                if (list.length() > 0)
                    list.append(",");
                list.append(option);
                if (logger.isDebugEnabled())
                    logger.debug("Bit " + i + " -> " + option);
            }
        }

        return list.toString();
    }
}
