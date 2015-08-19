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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

// Not used for now. Removing the warning.
//import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Table;

/**
 * This class defines a Version
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class Version
{
    public static final String TABLE_NAME    = "version";
    public static final String MODULE_COLUMN = "module";
    public static final String MAJOR_COLUMN  = "major";
    public static final String MINOR_COLUMN  = "minor";
    public static final String SUFFIX_COLUMN = "suffix";

    public static final String SELECT        = "SELECT * FROM ";

    private int                major         = 0;
    private int                minor         = 0;
    private String             suffix        = "";

    public static Table getVersionTableDefinition(String schema)
    {
        Table t = new Table(schema, TABLE_NAME);

        t.AddColumn(new Column(MODULE_COLUMN, Types.CHAR, 64));
        t.AddColumn(new Column(MAJOR_COLUMN, Types.INTEGER));
        t.AddColumn(new Column(MINOR_COLUMN, Types.INTEGER));
        t.AddColumn(new Column(SUFFIX_COLUMN, Types.CHAR, 64));

        return t;
    }

    public Version(int major, int minor, String suffix)
    {
        this.major = major;
        this.minor = minor;
        this.suffix = suffix;
    }

    private static String constructSelect(String schema, String module)
    {
        String select = Version.SELECT + schema + "." + TABLE_NAME + " WHERE "
                + MODULE_COLUMN + " = '" + module + "'";
        return select;
    }

    public static Version getVersionFromDB(Statement statement, String schema,
            String module) throws SQLException
    {
        Version ret = null;

        statement.setFetchSize(1);
        ResultSet rs = null;

        try
        {
            rs = statement.executeQuery(constructSelect(schema, module));
            if (rs.next())
            {
                int major = rs.getInt(MAJOR_COLUMN);
                int minor = rs.getInt(MINOR_COLUMN);
                String suffix = rs.getString(SUFFIX_COLUMN);
                ret = new Version(major, minor, suffix);
            }
        }
        finally
        {
            rs.close();
        }
        return ret;
    }

    public static void saveVersionToDB(Statement statement, String schema,
            String module, Version version) throws SQLException
    {
        ResultSet old = null;
        String sql;

        try
        {
            old = statement.executeQuery(constructSelect(schema, module));
            if (old.next())
            {
                /* update version row */
                sql = "UPDATE " + schema + "." + TABLE_NAME + " SET "
                        + MAJOR_COLUMN + " = " + version.major + ", "
                        + MINOR_COLUMN + " = " + version.minor + ", "
                        + SUFFIX_COLUMN + " = '" + version.suffix + "' WHERE "
                        + MODULE_COLUMN + " = '" + module + "'";
                // old.updateInt(MAJOR_COLUMN, version.major);
                // old.updateInt(MINOR_COLUMN, version.minor);
                // old.updateString(SUFFIX_COLUMN, version.suffix);
                // old.updateRow();
            }
            else
            {
                /* insert new version */
                sql = "INSERT INTO " + schema + "." + TABLE_NAME + " VALUES ('"
                        + module + "'," + version.major + "," + version.minor
                        + ",'" + version.suffix + "')";
                // code below requires CONCUR_UPDATABLE
                // old.moveToInsertRow();
                // old.updateString(MODULE_COLUMN, module);
                // old.updateInt(MAJOR_COLUMN, version.major);
                // old.updateInt(MINOR_COLUMN, version.minor);
                // old.updateString(SUFFIX_COLUMN, version.suffix);
                // old.insertRow();
            }
        }
        finally
        {
            old.close();
        }
        statement.execute(sql);
    }

    /**
     * Compares another version to current version.
     * 
     * @return 1, 0, -1 if this is bigger, equal or lower than another
     */
    public int compare(Version another)
    {
        if (this.major == another.major)
        {
            if (this.minor == another.minor)
            {
                return this.suffix.compareToIgnoreCase(another.suffix);
            }
            else if (this.minor > another.minor)
            {
                return 1;
            }
            else
            {
                return -1;
            }
        }
        else if (this.major > another.major)
        {
            return 1;
        }
        else
        {
            return -1;
        }
    }

    public int getMajor()
    {
        return major;
    }

    public int getMinor()
    {
        return minor;
    }

    public String getSuffix()
    {
        return suffix;
    }

    public String toString()
    {
        if (suffix.length() > 0)
        {
            return ("" + major + "." + minor + "-" + suffix);
        }
        else
        {
            return ("" + major + "." + minor);
        }
    }
}
