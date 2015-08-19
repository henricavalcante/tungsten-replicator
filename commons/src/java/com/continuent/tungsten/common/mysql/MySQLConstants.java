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
 * Initial developer(s): Csaba Simon
 * Contributor(s): Gilles Rayrat, Edward Archibald
 */

package com.continuent.tungsten.common.mysql;

import java.util.HashMap;

/**
 * The <code>MySQLConstants</code> object contains the MySQL protocol
 * constants. This constants can be found in the include/mysql_com.h file from
 * the MySQL source.
 * 
 * @author <a href="mailto:csaba.simon@continuent.com">Csaba Simon</a>
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 */

/**
 * Provides MySQL protocol constants and server version string.
 * 
 * @author Csaba Simon (csaba.simon@continuent.com)
 * @author <a href="gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version $Rev: $, $Date: $
 */
public class MySQLConstants
{
    /*
     * The following command constants are the exact counterpart of the
     * constants found in include/mysql_com.h.
     */
    public static final int                       COM_SLEEP                       = 0;

    public static final int                       COM_QUIT                        = 1;

    public static final int                       COM_INIT_DB                     = 2;

    public static final int                       COM_QUERY                       = 3;

    public static final int                       COM_FIELD_LIST                  = 4;

    public static final int                       COM_CREATE_DB                   = 5;

    public static final int                       COM_DROP_DB                     = 6;

    public static final int                       COM_REFRESH                     = 7;
    // COM_REFRESH sub_command.
    // http://dev.mysql.com/doc/internals/en/com-refresh.html
    public static final int                       REFRESH_GRANT                   = 0x01;
    public static final int                       REFRESH_LOG                     = 0x02;
    public static final int                       REFRESH_TABLES                  = 0x04;
    public static final int                       REFRESH_HOSTS                   = 0x08;
    public static final int                       REFRESH_STATUS                  = 0x10;
    public static final int                       REFRESH_THREADS                 = 0x20;
    public static final int                       REFRESH_SLAVE                   = 0x40;
    public static final int                       REFRESH_MASTER                  = 0x80;

    public static final int                       COM_SHUTDOWN                    = 8;

    public static final int                       COM_STATISTICS                  = 9;

    public static final int                       COM_PROCESS_INFO                = 10;

    public static final int                       COM_CONNECT                     = 11;

    public static final int                       COM_PROCESS_KILL                = 12;

    public static final int                       COM_DEBUG                       = 13;

    public static final int                       COM_PING                        = 14;

    public static final int                       COM_TIME                        = 15;

    public static final int                       COM_DELAYED_INSERT              = 16;

    public static final int                       COM_CHANGE_USER                 = 17;

    public static final int                       COM_BINLOG_DUMP                 = 18;

    public static final int                       COM_TABLE_DUMP                  = 19;

    public static final int                       COM_CONNECT_OUT                 = 20;

    public static final int                       COM_REGISTER_SLAVE              = 21;

    public static final int                       COM_STMT_PREPARE                = 22;

    public static final int                       COM_STMT_EXECUTE                = 23;

    public static final int                       COM_STMT_SEND_LONG_DATA         = 24;

    public static final int                       COM_STMT_CLOSE                  = 25;

    public static final int                       COM_STMT_RESET                  = 26;

    public static final int                       COM_SET_OPTION                  = 27;

    public static final int                       COM_STMT_FETCH                  = 28;

    /** String equivalents of above commands for display purposes */
    static final String                           commandStrings[]                = {
            "COM_SLEEP", "COM_QUIT", "COM_INIT_DB", "COM_QUERY",
            "COM_FIELD_LIST", "COM_CREATE_DB", "COM_DROP_DB", "COM_REFRESH",
            "COM_SHUTDOWN", "COM_STATISTICS", "COM_PROCESS_INFO",
            "COM_CONNECT", "COM_PROCESS_KILL", "COM_DEBUG", "COM_PING",
            "COM_TIME", "COM_DELAYED_INSERT", "COM_CHANGE_USER",
            "COM_BINLOG_DUMP", "COM_TABLE_DUMP", "COM_CONNECT_OUT",
            "COM_REGISTER_SLAVE", "COM_STMT_PREPARE", "COM_STMT_EXECUTE",
            "COM_STMT_SEND_LONG_DATA", "COM_STMT_CLOSE", "COM_STMT_RESET",
            "COM_SET_OPTION", "COM_STMT_FETCH"                                    };

    /* Client flags from include/mysql_com.h. */
    public static final int                       CLIENT_LONG_PASSWORD            = 1;

    public static final int                       CLIENT_FOUND_ROWS               = 2;

    public static final int                       CLIENT_LONG_FLAG                = 4;

    public static final int                       CLIENT_CONNECT_WITH_DB          = 8;

    public static final int                       CLIENT_NO_SCHEMA                = 16;

    public static final int                       CLIENT_COMPRESS                 = 32;

    public static final int                       CLIENT_ODBC                     = 64;

    public static final int                       CLIENT_LOCAL_FILES              = 128;

    public static final int                       CLIENT_IGNORE_SPACE             = 256;

    public static final int                       CLIENT_PROTOCOL_41              = 512;

    public static final int                       CLIENT_INTERACTIVE              = 1024;

    public static final int                       CLIENT_SSL                      = 2048;

    public static final int                       CLIENT_IGNORE_SIGPIPE           = 4096;

    public static final int                       CLIENT_TRANSACTIONS             = 8192;

    public static final int                       CLIENT_RESERVED                 = 16384;

    public static final int                       CLIENT_SECURE_CONNECTION        = 32768;

    public static final int                       CLIENT_MULTI_STATEMENTS         = 65536;

    public static final int                       CLIENT_MULTI_RESULTS            = 131072;

    /* Server status from include/mysql_com.h. */
    public static final short                     SERVER_STATUS_IN_TRANS          = 1;

    public static final short                     SERVER_STATUS_AUTOCOMMIT        = 2;

    public static final short                     SERVER_MORE_RESULTS_EXISTS      = 8;

    public static final short                     SERVER_QUERY_NO_GOOD_INDEX_USED = 16;

    public static final short                     SERVER_QUERY_NO_INDEX_USED      = 32;

    public static final short                     SERVER_STATUS_CURSOR_EXISTS     = 64;

    public static final short                     SERVER_STATUS_LAST_ROW_SENT     = 128;

    /* Field flags from include/mysql_com.h. */
    public static final int                       NOT_NULL_FLAG                   = 1;

    public static final int                       PRI_KEY_FLAG                    = 2;

    public static final int                       UNIQUE_KEY_FLAG                 = 4;

    public static final int                       MULTIPLE_KEY_FLAG               = 8;

    public static final int                       BLOB_FLAG                       = 16;

    public static final int                       UNSIGNED_FLAG                   = 32;

    public static final int                       ZEROFILL_FLAG                   = 64;

    public static final int                       BINARY_FLAG                     = 128;

    public static final int                       ENUM_FLAG                       = 256;

    public static final int                       AUTO_INCREMENT_FLAG             = 512;

    public static final int                       TIMESTAMP_FLAG                  = 1024;

    public static final int                       SET_FLAG                        = 2048;

    public static final int                       NO_DEFAULT_VALUE_FLAG           = 4096;

    public static final int                       NUM_FLAG                        = 32768;

    /* Character set constants */
    public static final short                     CHARSET_BINARY                  = 63;

    public static final short                     CHARSET_UTF8                    = 33;

    /* Cursor type */
    public static final byte                      CURSOR_TYPE_NO_CURSOR           = (byte) 0;
    public static final byte                      CURSOR_TYPE_READ_ONLY           = (byte) 1;
    public static final byte                      CURSOR_TYPE_FOR_UPDATE          = (byte) 2;
    public static final byte                      CURSOR_TYPE_SCROLLABLE          = (byte) 4;

    /* Type constants */
    /* These constants were extracted from include/mysql_com.h */

    public static final byte                      MYSQL_TYPE_DECIMAL              = 0;

    public static final byte                      MYSQL_TYPE_TINY                 = 1;

    public static final byte                      MYSQL_TYPE_SHORT                = 2;

    public static final byte                      MYSQL_TYPE_LONG                 = 3;

    public static final byte                      MYSQL_TYPE_FLOAT                = 4;

    public static final byte                      MYSQL_TYPE_DOUBLE               = 5;

    public static final byte                      MYSQL_TYPE_NULL                 = 6;

    public static final byte                      MYSQL_TYPE_TIMESTAMP            = 7;

    public static final byte                      MYSQL_TYPE_LONGLONG             = 8;

    public static final byte                      MYSQL_TYPE_INT24                = 9;

    public static final byte                      MYSQL_TYPE_DATE                 = 10;

    public static final byte                      MYSQL_TYPE_TIME                 = 11;

    public static final byte                      MYSQL_TYPE_DATETIME             = 12;

    public static final byte                      MYSQL_TYPE_YEAR                 = 13;

    public static final byte                      MYSQL_TYPE_NEWDATE              = 14;

    public static final byte                      MYSQL_TYPE_VARCHAR              = 15;

    public static final byte                      MYSQL_TYPE_BIT                  = 16;

    public static final byte                      MYSQL_TYPE_NEWDECIMAL           = -10;

    public static final byte                      MYSQL_TYPE_ENUM                 = -9;

    public static final byte                      MYSQL_TYPE_SET                  = -8;

    public static final byte                      MYSQL_TYPE_TINY_BLOB            = -7;

    public static final byte                      MYSQL_TYPE_MEDIUM_BLOB          = -6;

    public static final byte                      MYSQL_TYPE_LONG_BLOB            = -5;

    public static final byte                      MYSQL_TYPE_BLOB                 = -4;

    public static final byte                      MYSQL_TYPE_VAR_STRING           = -3;

    public static final byte                      MYSQL_TYPE_STRING               = -2;

    public static final byte                      MYSQL_TYPE_GEOMETRY             = -1;

    /* Errors taken from mysql_error.h */
    // ER_NO_ERROR is myosotis specific
    public static final int                       ER_NO_ERROR                     = -1;

    public static final int                       ER_NO                           = 1002;

    public static final int                       ER_DBACCESS_DENIED_ERROR        = 1044;

    public static final int                       ER_BAD_DB_ERROR                 = 1049;

    public static final int                       ER_TOO_MANY_USER_CONNECTIONS    = 1203;

    public static final int                       ER_WRONG_ARGUMENTS              = 1210;

    public static final int                       ER_NOT_SUPPORTED_YET            = 1235;

    public static final int                       ER_UNKNOWN_STMT_HANDLER         = 1243;

    public static final int                       ER_OUTOFMEMORY                  = 1037;

    public static final int                       ER_NO_DB_ERROR                  = 1046;

    public static final int                       ER_UNKNOWN_ERROR                = 1055;

    public static final int                       ER_LOST_CONNECTION              = 2013;

    public static final int                       ER_SERVER_GONE_AWAY             = 2006;

    public static final int                       ER_CON_COUNT_ERROR              = 1040;

    public static final int                       ER_SERVER_SHUTDOWN              = 1053;

    public static final int                       ER_UNKNOWN_SYSTEM_VARIABLE      = 1193;

    /* Misc constants */
    /** size of the long data header */
    public static final int                       MYSQL_LONG_DATA_HEADER          = 6;

    public static final String                    SQL_STATE_COMMUNICATION_ERROR   = "08S01";

    public static final String                    SQL_STATE_INVALID_CONNECTION    = "08";

    /* Define SQLSTATE values for corresponding MySQL error codes */
    private static final HashMap<Integer, String> mapSqlStates                    = new HashMap<Integer, String>()
                                                                                  {
                                                                                      private static final long serialVersionUID = 1L;

                                                                                      {
                                                                                          put(ER_DBACCESS_DENIED_ERROR,
                                                                                                  "42000");
                                                                                          put(ER_TOO_MANY_USER_CONNECTIONS,
                                                                                                  "42000");
                                                                                          put(ER_SERVER_SHUTDOWN,
                                                                                                  "08S01");
                                                                                          put(ER_UNKNOWN_SYSTEM_VARIABLE,
                                                                                                  "HY000");
                                                                                      }
                                                                                  };

    /* Define SQL queries values for corresponding MySQL commands */
    private static final HashMap<Integer, String> mapCommandToSQL                 = new HashMap<Integer, String>()
                                                                                  {
                                                                                      private static final long serialVersionUID = 1L;

                                                                                      {
                                                                                          put(REFRESH_GRANT,
                                                                                                  "flush privileges");
                                                                                          put(REFRESH_LOG,
                                                                                                  "flush logs");
                                                                                          put(REFRESH_TABLES,
                                                                                                  "flush tables");
                                                                                          put(REFRESH_HOSTS,
                                                                                                  "flush hosts");
                                                                                          put(REFRESH_STATUS,
                                                                                                  "flush status");
                                                                                          put(REFRESH_SLAVE,
                                                                                                  "reset slave");
                                                                                          put(REFRESH_MASTER,
                                                                                                  "reset master");
                                                                                      }
                                                                                  };

    public static final String                    MSG_SERVER_SHUTDOWN             = "Server shutdown in progress...";
    public static final String                    MSG_UNKNOWN_SYSTEM_VARIABLE     = "Unknown system variable '%s'";

    /** Private constructor to prevent instantiation. */
    private MySQLConstants()
    {
        // empty
    }

    /**
     * Converts the given command number into a human readable string
     * 
     * @param command command number to convert, one of COM_XXX above
     * @return a string representing the given command constant name
     */
    public static String commandToString(int command)
    {
        return commandStrings[command];
    }

    /**
     * Get the SQLSTATE corresponding to the MySQL errorcode.
     * 
     * @param mysqlErrorCode the errorcode for which we want the corresponding
     *            SQLSTATE
     * @return String for the corresponding SQLSTATE. null if the SQLSTATE is
     *         not defined in our constants.
     */
    public static String getSqlState(Integer mysqlErrorCode)
    {
        return mapSqlStates.get(mysqlErrorCode);
    }

    /**
     * Get the SQL query corresponding to the MySQL command.
     * 
     * @param command the command for which we want the corresponding sql
     * @return String for the corresponding sql query. null if the command is
     *         not defined in our constants.
     */
    public static String getSqlQueryForCommand(Integer command)
    {
        return mapCommandToSQL.get(command);
    }
}
