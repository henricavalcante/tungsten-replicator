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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class RotateLogEvent extends LogEvent
{
    /**
     * Fixed data part:
     * <ul>
     * <li>8 bytes. The position of the first event in the next log file. Always
     * contains the number 4 (meaning the next event starts at position 4 in the
     * next binary log). This field is not present in v1; presumably the value
     * is assumed to be 4.</li>
     * </ul>
     * <p>
     * Variable data part:
     * <ul>
     * <li>The name of the next binary log. The filename is not null-terminated.
     * Its length is the event size minus the size of the fixed parts.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    static Logger  logger = Logger.getLogger(RotateLogEvent.class);

    private int    filenameLength;
    private String filename;

    public String getNewBinlogFilename()
    {
        return filename;
    }

    /**
     * Creates a new <code>Rotate_log_event</code> object read normally from
     * log.
     * 
     * @param currentPosition
     * @throws ReplicatorException
     */
    public RotateLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, String currentPosition)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent, MysqlBinlog.START_EVENT_V3);

        this.startPosition = currentPosition;
        type = MysqlBinlog.ROTATE_EVENT;

        int headerSize = descriptionEvent.commonHeaderLength;
        int postHeaderLength = descriptionEvent.postHeaderLength[MysqlBinlog.ROTATE_EVENT - 1];
        int filenameOffset = headerSize + postHeaderLength;

        if (eventLength < headerSize)
        {
            throw new MySQLExtractException("Rotate event length is too short");
        }

        filenameLength = eventLength - filenameOffset;

        if (descriptionEvent.useChecksum())
        {
            filenameLength -= 4;
        }

        if (filenameLength > MysqlBinlog.FN_REFLEN - 1)
        {
            filenameLength = MysqlBinlog.FN_REFLEN - 1;
        }
        filename = new String(buffer, filenameOffset, filenameLength);

        if (logger.isDebugEnabled())
            logger.debug("New binlog file is : " + filename);

        doChecksum(buffer, filenameOffset + filenameLength, descriptionEvent);

    }

    /**
     * Creates a new <code>Rotate_log_event</code> without log information. This
     * is used to generate missing log rotation events.
     */
    public RotateLogEvent(String newLogFilename)
    {
        this.filename = newLogFilename;
        this.filenameLength = -1;
    }
}
