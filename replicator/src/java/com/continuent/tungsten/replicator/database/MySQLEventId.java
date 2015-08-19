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

package com.continuent.tungsten.replicator.database;

/**
 * Implements a MySQL event ID, which have the following form:
 * <p/>
 * 
 * <pre>[prefix-name.]file_index:offset[;session_id]</pre>
 * For example, a MySQL event ID formatted by Tungsten would typically appear as
 * follows:
 * 
 * <pre>mysql-bin.014371:0000000064207416;0</pre>
 * This class will correctly match and sort that event ID against events having
 * the following formats:
 * 
 * <pre>mysql-bin.014371:64207416</pre>
 * 
 * <pre>014371:64207416</pre>
 */
public class MySQLEventId implements EventId
{
    // Raw event ID and parts derived from parsing.
    private String  rawEventId;
    private String  fileName;
    private String  filePrefix;
    private long    fileIndex = -1;
    private long    offset    = -1;
    private long    sessionId = -1;
    private boolean valid     = false;

    public MySQLEventId(String rawEventId)
    {
        this.rawEventId = rawEventId;

        // Parse into constituent parts.
        int dotIndex = rawEventId.indexOf('.');
        int colonIndex = rawEventId.indexOf(':');
        int semicolonIndex = rawEventId.indexOf(";");

        // Get the file name. Look for the file index within that
        // name.
        if (colonIndex > -1)
        {
            fileName = rawEventId.substring(0, colonIndex);
            if (dotIndex > -1)
            {

                filePrefix = rawEventId.substring(0, dotIndex);
                fileIndex = getLong(rawEventId, dotIndex + 1, colonIndex);
            }
            else
            {
                fileIndex = getLong(fileName, 0, colonIndex);
            }

            // Get the offset and optional session ID.
            if (semicolonIndex > -1)
            {
                offset = getLong(rawEventId, colonIndex + 1, semicolonIndex);
                sessionId = getLong(rawEventId, semicolonIndex + 1, -1);
            }
            else
            {
                offset = getLong(rawEventId, colonIndex + 1, -1);
            }
        }

        // Finally, decide whether this is a valid event ID.
        if (fileIndex > 0 && offset > -1)
            valid = true;
        else
            valid = false;
    }

    // Utility method to convert a long and catch failures of any kind.
    private long getLong(String buffer, int startIndex, int endIndex)
    {
        // Substring and parse to long value.
        try
        {
            String longAsString;
            if (endIndex > -1)
                longAsString = buffer.substring(startIndex, endIndex);
            else
                longAsString = buffer.substring(startIndex);
            return Long.valueOf(longAsString);
        }
        catch (IndexOutOfBoundsException e)
        {
            return -1;
        }
        catch (NumberFormatException e)
        {
            return -1;
        }
    }

    public String getRawEventId()
    {
        return rawEventId;
    }

    public String getFileName()
    {
        return fileName;
    }

    public long getFileIndex()
    {
        return fileIndex;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getSessionId()
    {
        return sessionId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.EventId#isValid()
     */
    public boolean isValid()
    {
        return valid;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.EventId#getDbmsType()
     */
    public String getDbmsType()
    {
        return "mysql";
    }

    /**
     * Compares two event IDs using the file index and offset as determinants
     * for collation.
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(EventId eventId)
    {
        MySQLEventId other = (MySQLEventId) eventId;
        return compareTo(other);
    }

    /**
     * Compares two event IDs using the file index and offset as determinants
     * for collation.
     */
    public int compareTo(MySQLEventId eventId)
    {
        // Compare first on the index.
        long indexDiff = this.fileIndex - eventId.getFileIndex();
        if (indexDiff != 0)
            return (indexDiff > 0) ? 1 : -1;

        // Compare next on the index.
        long offsetDiff = this.offset - eventId.getOffset();
        if (offsetDiff < 0)
            return -1;
        else if (offsetDiff == 0)
            return 0;
        else
            return 1;
    }

    /**
     * Prints event ID in standard format for MySQL. If invalid, return the raw
     * event ID.
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        if (valid)
        {
            StringBuffer sb = new StringBuffer();
            if (filePrefix != null)
                sb.append(filePrefix).append(".");
            sb.append(String.format("%06d:%016d", fileIndex, offset));
            if (sessionId > -1)
                sb.append(";").append(sessionId);
            return sb.toString();
        }
        else
            return rawEventId;
    }
}
