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

package com.continuent.tungsten.replicator.applier;

import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;

import oracle.jdbc.OraclePreparedStatement;
import oracle.sql.CLOB;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.AdditionalTypes;
import com.continuent.tungsten.replicator.datatypes.MySQLUnsignedNumeric;
import com.continuent.tungsten.replicator.datatypes.Numeric;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;

public class OracleApplier extends JdbcApplier
{
    private static Logger logger  = Logger.getLogger(OracleApplier.class);

    private CLOB getCLOB(String xmlData) throws SQLException
    {
        CLOB tempClob = null;
        Connection dbConn = conn.getConnection();
        try
        {
            // If the temporary CLOB has not yet been created, create new
            tempClob = CLOB
                    .createTemporary(dbConn, true, CLOB.DURATION_SESSION);

            // Open the temporary CLOB in readwrite mode to enable writing
            tempClob.open(CLOB.MODE_READWRITE);
            // Get the output stream to write
            // Writer tempClobWriter = tempClob.getCharacterOutputStream();
            Writer tempClobWriter = tempClob.setCharacterStream(0);
            // Write the data into the temporary CLOB
            tempClobWriter.write(xmlData);

            // Flush and close the stream
            tempClobWriter.flush();
            tempClobWriter.close();

            // Close the temporary CLOB
            tempClob.close();
        }
        catch (SQLException sqlexp)
        {
            tempClob.freeTemporary();
            sqlexp.printStackTrace();
        }
        catch (Exception exp)
        {
            tempClob.freeTemporary();
            exp.printStackTrace();
        }
        return tempClob;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#setObject(java.sql.PreparedStatement,
     *      int, com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal,
     *      com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec)
     */
    @Override
    protected void setObject(PreparedStatement prepStatement, int bindLoc,
            ColumnVal value, ColumnSpec columnSpec) throws SQLException
    {
        int type = columnSpec.getType();
        try
        {
            if (value.getValue() == null)
                prepStatement.setObject(bindLoc, null);
            /*
             * prepStatement.setNull(bindLoc, type); else if (type ==
             * Types.FLOAT) ((OraclePreparedStatement)
             * prepStatement).setBinaryFloat( bindLoc, ((Float)
             * value.getValue()).floatValue()); else if (type == Types.DOUBLE)
             * ((OraclePreparedStatement) prepStatement).setBinaryDouble(
             * bindLoc, ((Double) value.getValue()).doubleValue());
             */
            else if (type == AdditionalTypes.XML)
            {
                CLOB clob = getCLOB((String) (value.getValue()));
                ((OraclePreparedStatement) prepStatement).setObject(bindLoc,
                        clob);
            }
            else if (type == Types.DATE
                    && (value.getValue() instanceof java.sql.Timestamp))
            { // Issue 704 - unsuccessful DATETIME to DATE conversion
                Timestamp ts = (Timestamp) value.getValue();
                ((OraclePreparedStatement) prepStatement).setTimestamp(bindLoc,
                        ts, Calendar.getInstance(TimeZone.getTimeZone("GMT")));
            }
            else if (type == Types.DATE
                    && (value.getValue() instanceof java.lang.Long))
            { // TENT-311 - no conversion is needed if the underlying value is
              // Date.
                Timestamp ts = new Timestamp((Long) (value.getValue()));
                ((OraclePreparedStatement) prepStatement)
                        .setObject(bindLoc, ts);
            }
            else if (type == Types.BLOB
                    || (type == Types.NULL && value.getValue() instanceof SerialBlob))
            { // ______^______
              // Blob in the incoming event masked as NULL,
              // though this happens with a non-NULL value!
              // Case targeted with this: MySQL.TEXT -> Oracle.VARCHARx

                SerialBlob blob = (SerialBlob) value.getValue();

                if (columnSpec.isBlob())
                {
                    // Blob in the incoming event and in Oracle table.
                    // IMPORTANT: the bellow way only fixes INSERTs.
                    // Blobs in key lookups of DELETEs and UPDATEs is
                    // not supported.
                    prepStatement.setBytes(bindLoc,
                            blob.getBytes(1, (int) blob.length()));
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("BLOB support in Oracle is only for INSERT currently; key lookup during DELETE/UPDATE will result in an error");
                    }
                }
                else
                {
                    // Blob in the incoming event, but not in Oracle.
                    // Case targeted with this: MySQL.TEXT -> Oracle.VARCHARx
                    String toString = null;
                    if (blob != null)
                        toString = new String(blob.getBytes(1,
                                (int) blob.length()));
                    prepStatement.setString(bindLoc, toString);
                }
            }
            else if (type == Types.INTEGER)
            { // Issue 798 - MySQLExtractor extracts UNSIGNED numbers in a not
              // platform-indendent way.
                Object valToInsert = null;
                Numeric numeric = new Numeric(columnSpec, value);
                if (columnSpec.isUnsigned() && numeric.isNegative())
                {
                    // We assume that if column is unsigned - it's MySQL on the
                    // master side, as Oralce doesn't have UNSIGNED modifier.
                    valToInsert = MySQLUnsignedNumeric
                            .negativeToMeaningful(numeric);
                    prepStatement.setObject(bindLoc, valToInsert);
                }
                else
                    prepStatement.setObject(bindLoc, value.getValue());
            }
            else
                prepStatement.setObject(bindLoc, value.getValue());
        }
        catch (SQLException e)
        {
            logger.error("Binding column (bindLoc=" + bindLoc + ", type="
                    + type + ") failed:");
            throw e;
        }
    }
}