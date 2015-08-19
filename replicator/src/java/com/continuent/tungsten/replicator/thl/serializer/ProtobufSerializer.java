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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.serial.SerialException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.LoadDataFileQuery;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.event.ReplOption;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.Header;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufEventOption;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufLoadDataFileFragment;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufLoadDataFileQuery;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufOneChange;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufOneChange.Builder;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufOneRowChange;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufOneRowChange.ActionType;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufOneRowChange.ProtobufColumnSpec;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufOneRowChange.ProtobufRowValue;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufOneRowChange.ProtobufRowValue.ProtobufColumnVal;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufOneRowChange.ProtobufRowValue.ProtobufColumnVal.Type;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufReplDBMSEvent;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufRowChangeData;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufRowIdData;
import com.continuent.tungsten.replicator.thl.protobuf.TungstenProtos.ProtobufStatementData;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ProtobufSerializer implements Serializer
{
    static Logger        logger           = Logger.getLogger(ProtobufSerializer.class);

    private int          deserializeCount = 0;
    private long         globalDeserTime  = 0;

    private StringBuffer trace            = new StringBuffer();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.serializer.Serializer#deserializeEvent(java.io.InputStream)
     */
    public THLEvent deserializeEvent(InputStream inStream) throws IOException
    {
        long startTime = 0;
        if (logger.isDebugEnabled())
            startTime = System.nanoTime();
        try
        {
            ReplDBMSEvent event = null;
            Header header = deserializeHeader(inStream);

            Timestamp sourceTstamp = new Timestamp(header.getSourceTstamp());

            if (header.getFilteredEvent())
            {
                event = new ReplDBMSFilteredEvent(header.getSeqno(),
                        (short) header.getFragno(), header.getSeqnoEnd(),
                        (short) header.getFragnoEnd(), header.getLastFrag(),
                        header.getEventId(), header.getSourceId(),
                        sourceTstamp, header.getEpochNumber());
            }
            else
            {
                ProtobufReplDBMSEvent protobufReplDBMSEvent = ProtobufReplDBMSEvent
                        .parseDelimitedFrom(inStream);

                ArrayList<DBMSData> data = new ArrayList<DBMSData>();
                List<ProtobufOneChange> changeList = protobufReplDBMSEvent
                        .getChangeList();
                for (ProtobufOneChange protobufOneChange : changeList)
                {
                    DBMSData statementChange = deserializeOneChange(protobufOneChange);
                    data.add(statementChange);
                }

                DBMSEvent dbmsEvent = new DBMSEvent(header.getEventId(), null,
                        data, sourceTstamp);
                event = new ReplDBMSEvent(header.getSeqno(),
                        (short) header.getFragno(), header.getLastFrag(),
                        header.getSourceId(), header.getEpochNumber(),
                        sourceTstamp, dbmsEvent);

                for (ProtobufEventOption protobufEventOption : protobufReplDBMSEvent
                        .getMetadataList())
                {
                    dbmsEvent.addMetadataOption(protobufEventOption.getName(),
                            protobufEventOption.getValue());
                }

                for (ProtobufEventOption protobufEventOption : protobufReplDBMSEvent
                        .getOptionsList())
                {
                    dbmsEvent.addOption(protobufEventOption.getName(),
                            protobufEventOption.getValue());
                }
            }
            return new THLEvent(header.getEventId(), event);
        }
        finally
        {
            if (logger.isDebugEnabled())
            {
                deserializeCount++;
                globalDeserTime += System.nanoTime() - startTime;
                if (deserializeCount % 1000 == 0)
                {
                    logger.debug("Average extraction time = " + globalDeserTime
                            / 1000 + " ns/event.");
                    deserializeCount = 0;
                    globalDeserTime = 0;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.serializer.Serializer#serializeEvent(com.continuent.tungsten.replicator.thl.THLEvent,
     *      java.io.OutputStream)
     */
    public void serializeEvent(THLEvent thlEvent, OutputStream outStream)
            throws IOException
    {
        ProtobufReplDBMSEvent.Builder protoEventBuilder = ProtobufReplDBMSEvent
                .newBuilder();

        serializeHeader(thlEvent, outStream);
        ReplEvent event = thlEvent.getReplEvent();
        if (!(event instanceof ReplDBMSFilteredEvent))
        {
            // ReplDBMSFilteredEvent are serialized within the header
            ReplDBMSEvent ev = (ReplDBMSEvent) event;

            ProtobufEventOption.Builder metadataBuilder;
            for (ReplOption replOption : ev.getDBMSEvent().getMetadata())
            {
                metadataBuilder = ProtobufEventOption.newBuilder();
                metadataBuilder.setName(replOption.getOptionName());
                metadataBuilder.setValue(replOption.getOptionValue());
                protoEventBuilder.addMetadata(metadataBuilder);
            }
            for (ReplOption replOption : ev.getDBMSEvent().getOptions())
            {
                metadataBuilder = ProtobufEventOption.newBuilder();
                metadataBuilder.setName(replOption.getOptionName());
                metadataBuilder.setValue(replOption.getOptionValue());
                protoEventBuilder.addOptions(metadataBuilder);
            }

            ArrayList<DBMSData> evData = ev.getData();

            // Initializing protobuf builder
            ProtobufOneChange.Builder oneChangeBuilder = null;
            ProtobufRowChangeData.Builder rowDataBuilder = null;

            for (DBMSData dbmsData : evData)
            {
                oneChangeBuilder = ProtobufOneChange.newBuilder();
                if (dbmsData instanceof RowChangeData)
                {
                    oneChangeBuilder.setType(ProtobufOneChange.Type.ROW_DATA);

                    RowChangeData rowEv = (RowChangeData) dbmsData;
                    rowDataBuilder = ProtobufRowChangeData.newBuilder();

                    serializeRows(rowDataBuilder, rowEv);
                    oneChangeBuilder.setData(rowDataBuilder);
                }
                else if (dbmsData instanceof LoadDataFileQuery)
                {
                    oneChangeBuilder
                            .setType(ProtobufOneChange.Type.LOAD_DATA_FILE_QUERY);
                    LoadDataFileQuery data = (LoadDataFileQuery) dbmsData;
                    serializeLoadDataFileQuery(oneChangeBuilder, data);
                }
                else if (dbmsData instanceof LoadDataFileFragment)
                {
                    oneChangeBuilder
                            .setType(ProtobufOneChange.Type.LOAD_DATA_FILE_FRAGMENT);
                    LoadDataFileFragment data = (LoadDataFileFragment) dbmsData;
                    serializeLoadDataFileFragment(oneChangeBuilder, data);

                }
                else if (dbmsData instanceof StatementData)
                {
                    oneChangeBuilder
                            .setType(ProtobufOneChange.Type.STATEMENT_DATA);
                    StatementData data = (StatementData) dbmsData;
                    oneChangeBuilder
                            .setStatement((ProtobufStatementData.Builder) serializeStatement(data));

                }
                else if (dbmsData instanceof RowIdData)
                {
                    RowIdData data = (RowIdData) dbmsData;
                    serializeRowIdData(oneChangeBuilder, data);
                }
                else
                {
                    if (dbmsData == null)
                        logger.warn("Trying to serialize null object");
                    else
                        logger.warn("Type " + dbmsData.getClass().getName()
                                + " is not implemented yet.");
                    continue;
                }
                protoEventBuilder.addChange(oneChangeBuilder);
            }
        }
        protoEventBuilder.build().writeDelimitedTo(outStream);
        outStream.flush();
    }

    private DBMSData deserializeOneChange(ProtobufOneChange protobufOneChange)
    {
        logger.debug("Event type is : " + protobufOneChange.getType());
        if (protobufOneChange.getType().equals(
                ProtobufOneChange.Type.STATEMENT_DATA))
        {
            StatementData statement = deserializeStatement(protobufOneChange
                    .getStatement());
            return statement;
        }
        else if (protobufOneChange.getType() == ProtobufOneChange.Type.ROW_ID_DATA)
        {
            ProtobufRowIdData rowId = protobufOneChange.getRowId();
            // Using type 2 as default (INSERT_ID -- this is the old incorrect
            // behavior) in case no type is found in THL
            return new RowIdData(rowId.getId(), (rowId.hasType()
                    ? rowId.getType()
                    : RowIdData.INSERT_ID));
        }
        else if (protobufOneChange.getType() == ProtobufOneChange.Type.ROW_DATA)
        {
            return deserializeRows(protobufOneChange.getData());
        }
        else if (protobufOneChange.getType().equals(
                ProtobufOneChange.Type.LOAD_DATA_FILE_FRAGMENT))
        {
            LoadDataFileFragment loadDataFragment = deserializeFileFragment(protobufOneChange
                    .getFileFragment());
            return loadDataFragment;
        }
        else if (protobufOneChange.getType().equals(
                ProtobufOneChange.Type.LOAD_DATA_FILE_QUERY))
        {
            LoadDataFileQuery loadDataFileQuery = deserializeLoadDataFileQuery(protobufOneChange
                    .getFileQuery());
            return loadDataFileQuery;

        }
        return null;
    }

    private LoadDataFileQuery deserializeLoadDataFileQuery(
            ProtobufLoadDataFileQuery fileQuery)
    {
        ProtobufStatementData statement = fileQuery.getStatement();
        LoadDataFileQuery loadFileQuery = new LoadDataFileQuery(
                statement.getQuery(), statement.getTimestamp(),
                (statement.hasDefaultSchema()
                        ? statement.getDefaultSchema()
                        : null), fileQuery.getFileId(),
                fileQuery.getFilenameStartPos(), fileQuery.getFilenameEndPos());
        loadFileQuery.setErrorCode(statement.getErrorCode());
        for (ProtobufEventOption statementDataOption : statement
                .getOptionsList())
        {
            loadFileQuery.addOption(statementDataOption.getName(),
                    statementDataOption.getValue());
        }

        return loadFileQuery;
    }

    private LoadDataFileFragment deserializeFileFragment(
            ProtobufLoadDataFileFragment fileFragment)
    {
        return new LoadDataFileFragment(fileFragment.getFileId(), fileFragment
                .getData().toByteArray(), fileFragment.getDatabase());
    }

    public void serializeHeader(THLEvent event, OutputStream outStream)
            throws IOException
    {
        Header.Builder headerBuilder = Header.newBuilder();

        headerBuilder.setSeqno(event.getSeqno());
        headerBuilder.setFragno(event.getFragno());
        headerBuilder.setLastFrag(event.getLastFrag());
        headerBuilder.setSourceTstamp(event.getSourceTstamp().getTime());
        headerBuilder.setExtractedTstamp(System.currentTimeMillis());
        headerBuilder.setSourceId(event.getSourceId());
        headerBuilder.setEpochNumber(event.getEpochNumber());
        headerBuilder.setEventId(event.getEventId());
        headerBuilder.setShardId("#DEFAULT");

        if (event.getReplEvent() != null
                && event.getReplEvent() instanceof ReplDBMSFilteredEvent)
        {
            ReplDBMSFilteredEvent ev = (ReplDBMSFilteredEvent) event
                    .getReplEvent();
            headerBuilder.setFilteredEvent(true);
            // Fix up time for filtered events, which don't have a proper
            // DBMSEvent.
            Timestamp extractedTs = ev.getExtractedTstamp();
            long extractedTime;
            if (extractedTs == null)
                extractedTime = System.currentTimeMillis();
            else
                extractedTime = extractedTs.getTime();
            headerBuilder.setExtractedTstamp(extractedTime);
            headerBuilder.setSourceTstamp(extractedTime);
            headerBuilder.setFragnoEnd(ev.getFragnoEnd());
            headerBuilder.setSeqnoEnd(ev.getSeqnoEnd());
        }
        else
            headerBuilder.setFilteredEvent(false);
        Header builder = headerBuilder.build();
        builder.writeDelimitedTo(outStream);
    }

    public Header deserializeHeader(InputStream is) throws IOException
    {
        return Header.parseDelimitedFrom(is);
    }

    private void serializeRows(ProtobufRowChangeData.Builder rowDataBuilder,
            RowChangeData rowEv)
    {
        ProtobufOneRowChange.Builder oneRowBuilder;
        ProtobufColumnSpec.Builder colSpecBuilder;
        ProtobufRowValue.Builder rowBuilder;
        ProtobufColumnVal.Builder valueBuilder;
        ArrayList<OneRowChange> rowChanges = rowEv.getRowChanges();

        List<ReplOption> options = rowEv.getOptions();
        if (options != null && !options.isEmpty())
        {
            ProtobufEventOption.Builder optionsBuilder;
            for (ReplOption replOption : options)
            {
                optionsBuilder = ProtobufEventOption.newBuilder();
                optionsBuilder.setName(replOption.getOptionName());
                optionsBuilder.setValue(replOption.getOptionValue());
                rowDataBuilder.addOptions(optionsBuilder);
            }
        }

        trace = new StringBuffer();
        try
        {
            for (OneRowChange oneRowChange : rowChanges)
            {
                oneRowBuilder = ProtobufOneRowChange.newBuilder();
                oneRowBuilder.setSchemaName(oneRowChange.getSchemaName());
                oneRowBuilder.setTableName(oneRowChange.getTableName());
                oneRowBuilder.setTableId(oneRowChange.getTableId());

                switch (oneRowChange.getAction())
                {
                    case INSERT :
                        if (logger.isDebugEnabled())
                            trace.append("INSERT into ");
                        oneRowBuilder.setAction(ActionType.INSERT);
                        break;
                    case DELETE :
                        if (logger.isDebugEnabled())
                            trace.append("DELETE from ");
                        oneRowBuilder.setAction(ActionType.DELETE);
                        break;
                    case UPDATE :
                        if (logger.isDebugEnabled())
                            trace.append("UPDATE ");
                        oneRowBuilder.setAction(ActionType.UPDATE);
                        break;
                    default :
                        break;
                }

                if (logger.isDebugEnabled())
                {
                    trace.append(oneRowChange.getSchemaName());
                    trace.append(".");
                    trace.append(oneRowChange.getTableName());
                    trace.append("\n    Columns spec :\n");
                }

                ArrayList<ColumnSpec> list = oneRowChange.getColumnSpec();
                for (ColumnSpec columnSpec : list)
                {
                    traceColumnSpec(columnSpec);

                    colSpecBuilder = ProtobufColumnSpec.newBuilder();
                    colSpecBuilder.setIndex(columnSpec.getIndex());
                    colSpecBuilder.setLength(columnSpec.getLength());
                    if (columnSpec.getName() != null)
                    {
                        colSpecBuilder.setName(columnSpec.getName());
                    }
                    colSpecBuilder.setNotNull(columnSpec.isNotNull());
                    colSpecBuilder.setSigned(!columnSpec.isUnsigned());
                    colSpecBuilder.setType(columnSpec.getType());
                    if (columnSpec.getTypeDescription() != null)
                    {
                        colSpecBuilder.setTypeDescription(columnSpec
                                .getTypeDescription());
                    }
                    oneRowBuilder.addColumnSpec(colSpecBuilder);
                }

                if (logger.isDebugEnabled())
                    trace.append("\n    Keys spec :\n");
                list = oneRowChange.getKeySpec();
                for (ColumnSpec columnSpec : list)
                {
                    traceColumnSpec(columnSpec);

                    colSpecBuilder = ProtobufColumnSpec.newBuilder();
                    colSpecBuilder.setIndex(columnSpec.getIndex());
                    colSpecBuilder.setLength(columnSpec.getLength());
                    if (columnSpec.getName() != null)
                    {
                        colSpecBuilder.setName(columnSpec.getName());
                    }
                    colSpecBuilder.setNotNull(columnSpec.isNotNull());
                    colSpecBuilder.setSigned(!columnSpec.isUnsigned());
                    colSpecBuilder.setType(columnSpec.getType());
                    if (columnSpec.getTypeDescription() != null)
                    {
                        colSpecBuilder.setTypeDescription(columnSpec
                                .getTypeDescription());
                    }
                    oneRowBuilder.addKeySpec(colSpecBuilder);
                }

                ArrayList<ArrayList<ColumnVal>> rowValues = oneRowChange
                        .getColumnValues();

                if (logger.isDebugEnabled())
                    trace.append("\n    Columns values :\n");

                for (ArrayList<ColumnVal> row : rowValues)
                {
                    rowBuilder = ProtobufRowValue.newBuilder();
                    for (int i = 0; i < row.size(); i++)
                    {
                        if (logger.isDebugEnabled() && i > 0)
                            trace.append(", ");

                        valueBuilder = ProtobufColumnVal.newBuilder();

                        ColumnVal colValue = row.get(i);
                        ColumnSpec colSpec = oneRowChange.getColumnSpec()
                                .get(i);
                        serializeRowValue(valueBuilder, colValue, colSpec);
                        rowBuilder.addColumnValue(valueBuilder);
                    }
                    oneRowBuilder.addColumnValues(rowBuilder);
                    if (logger.isDebugEnabled())
                        trace.append("\n");
                }

                if (logger.isDebugEnabled())
                    trace.append("\n    Keys values :\n");

                rowValues = oneRowChange.getKeyValues();
                for (ArrayList<ColumnVal> row : rowValues)
                {
                    rowBuilder = ProtobufRowValue.newBuilder();
                    for (int i = 0; i < row.size(); i++)
                    {
                        if (logger.isDebugEnabled() && i > 0)
                            trace.append(", ");

                        valueBuilder = ProtobufColumnVal.newBuilder();

                        ColumnVal colValue = row.get(i);
                        ColumnSpec colSpec = oneRowChange.getKeySpec().get(i);
                        serializeRowValue(valueBuilder, colValue, colSpec);
                        rowBuilder.addColumnValue(valueBuilder);
                    }
                    if (logger.isDebugEnabled())
                        trace.append("\n");

                    oneRowBuilder.addKeyValues(rowBuilder);
                }
                rowDataBuilder.addRowChange(oneRowBuilder);
            }
        }
        catch (Exception e)
        {
            if (logger.isDebugEnabled())
                logger.debug("Failure while storing " + trace.toString(), e);
            throw new RuntimeException(e);
        }
        if (logger.isDebugEnabled())
            logger.debug(trace.toString());
    }

    private void serializeRowValue(ProtobufColumnVal.Builder valueBuilder,
            ColumnVal colValue, ColumnSpec colSpec)
    {
        if (logger.isDebugEnabled())
        {
            trace.append("Type = ");
            trace.append(colSpec.getType());
        }

        if (colSpec.getType() == Types.NULL)
        {
            // This is an optimization : the whole column was null
            valueBuilder.setType(Type.NULL);
            if (logger.isDebugEnabled())
            {
                trace.append(" / NULL");
            }
            return;
        }
        else if (colValue.getValue() == null)
        {
            // This single value was null (as opposed to the whole column
            // above-case
            valueBuilder.setType(Type.NULL);
            if (logger.isDebugEnabled())
            {
                trace.append(" / NULL");
            }
            return;
        }

        Object value = colValue.getValue();
        switch (colSpec.getType())
        {
            case Types.INTEGER :
                switch (colSpec.getLength())
                {
                    case 0 :
                        if (value instanceof Integer)
                        {
                            valueBuilder.setIntValue((Integer) value);
                            if (logger.isDebugEnabled())
                            {
                                trace.append(" / ");
                                trace.append(value);
                            }
                            valueBuilder.setType(Type.INT);
                        }
                        else if (value instanceof Long)
                        {
                            valueBuilder.setLongValue((Long) value);
                            if (logger.isDebugEnabled())
                            {
                                trace.append(" / ");
                                trace.append(value);
                            }
                            valueBuilder.setType(Type.LONG);
                        }
                        break;
                    case 1 :
                    case 2 :
                    case 3 :
                    case 4 :
                        valueBuilder.setIntValue((Integer) value);
                        if (logger.isDebugEnabled())
                        {
                            trace.append(" / ");
                            trace.append(value);
                        }
                        valueBuilder.setType(Type.INT);
                        break;
                    case 8 :
                        valueBuilder.setLongValue((Long) value);
                        if (logger.isDebugEnabled())
                        {
                            trace.append(" / ");
                            trace.append(value);
                        }
                        valueBuilder.setType(Type.LONG);
                        break;
                    default :
                        logger.warn("Undefined type");
                        break;
                }
                break;
            case Types.DECIMAL :
            case Types.NUMERIC :
                BigDecimal bigDec = (BigDecimal) value;
                if (logger.isDebugEnabled())
                {
                    trace.append(" / ");
                    trace.append(bigDec);
                }

                BigInteger unscaledValue = bigDec.unscaledValue();
                int scale = bigDec.scale();
                valueBuilder.setType(Type.DECIMAL);
                valueBuilder.setBytesValue(ByteString.copyFrom(unscaledValue
                        .toByteArray()));
                valueBuilder.setIntValue(scale);
                break;
            case Types.FLOAT :
                valueBuilder.setFloatValue((Float) value);
                if (logger.isDebugEnabled())
                {
                    trace.append(" / ");
                    trace.append(value);
                }

                valueBuilder.setType(Type.FLOAT);
                break;
            case Types.DOUBLE :
                valueBuilder.setDoubleValue((Double) value);
                if (logger.isDebugEnabled())
                {
                    trace.append(" / ");
                    trace.append(value);
                }
                valueBuilder.setType(Type.DOUBLE);
                break;
            case Types.BIT :
                valueBuilder.setIntValue((Integer) value);
                if (logger.isDebugEnabled())
                {
                    trace.append(" / ");
                    trace.append(value);
                }
                valueBuilder.setType(Type.BIT);
                break;
            case Types.TIMESTAMP :
                if (value instanceof Integer)
                {
                    valueBuilder.setIntValue(0);
                }
                else
                {
                    long time = ((Timestamp) value).getTime();

                    // Need to check whether timestamp is negative to compute
                    // milliseconds
                    int millis = (int) (time % 1000);
                    if (millis < 0)
                        millis += 1000;

                    int nanos = ((Timestamp) value).getNanos()
                            - (millis * 1000000);

                    if (logger.isDebugEnabled())
                    {
                        trace.append(" / ");
                        trace.append(time);
                    }
                    valueBuilder.setLongValue(time);
                    if (nanos > 0)
                        valueBuilder.setIntValue(nanos);
                }
                valueBuilder.setType(Type.TIMESTAMP);
                break;
            case Types.TIME :
                long time = 0;
                if (value instanceof Time)
                    time = ((Time) value).getTime();
                else if (value instanceof Timestamp)
                {
                    Timestamp ts = (Timestamp) value;
                    time = ts.getTime();

                    // Need to check whether timestamp is negative to compute
                    // milliseconds
                    int millis = (int) (time % 1000);
                    if (millis < 0)
                        millis += 1000;

                    int nanos = ts.getNanos() - (millis * 1000000);

                    if (logger.isDebugEnabled())
                        logger.debug("Serializing TIME2 : " + ts + " - " + time
                                + " - " + millis + " - " + nanos);

                    // Even if nanoseconds part is 0, store it in THL as
                    // deserialization will check whether it exists or not in
                    // order to distinguish old events from new events in THL
                    valueBuilder.setIntValue(nanos);
                }
                if (logger.isDebugEnabled())
                {
                    trace.append(" / ");
                    trace.append(time);
                }
                valueBuilder.setLongValue(time);
                valueBuilder.setType(Type.TIME);
                break;
            case Types.DATE :
                if (value instanceof Integer)
                    valueBuilder.setIntValue(0);
                else if (value instanceof Timestamp)
                {
                    // Handling DATETIME datatype (using GMT to apply)
                    time = ((Timestamp) value).getTime();
                    valueBuilder.setLongValue(time);
                    valueBuilder.setStringValue("GMT");

                    // Need to check whether timestamp is negative to compute
                    // milliseconds
                    int millis = (int) (time % 1000);
                    if (millis < 0)
                        millis += 1000;

                    int nanos = ((Timestamp) value).getNanos()
                            - (millis * 1000000);
                    if (nanos > 0)
                        valueBuilder.setIntValue(nanos);
                }
                else
                {
                    time = ((Date) value).getTime();
                    if (logger.isDebugEnabled())
                    {
                        trace.append(" / ");
                        trace.append(time);
                    }
                    valueBuilder.setLongValue(time);
                }
                valueBuilder.setType(Type.DATE);
                break;
            case Types.OTHER :
                valueBuilder.setIntValue((Integer) value);
                if (logger.isDebugEnabled())
                {
                    trace.append(" / ");
                    trace.append(value);
                }
                valueBuilder.setType(Type.INT);
                break;
            case Types.BLOB :
                if (logger.isDebugEnabled())
                {
                    trace.append(" / ");
                    trace.append(value);
                }

                try
                {
                    byte[] blob = ((SerialBlob) value).getBytes(1,
                            (int) ((SerialBlob) value).length());
                    valueBuilder.setBytesValue(ByteString.copyFrom(blob));
                }
                catch (SerialException e)
                {
                    logger.error("Failed to serialize blob", e);
                }
                valueBuilder.setType(Type.BLOB);
                break;
            case Types.CHAR :
            case Types.NCHAR :
            case Types.VARCHAR :
            case Types.NVARCHAR :
                if (value instanceof String)
                {
                    valueBuilder.setStringValue((String) value);
                    if (logger.isDebugEnabled())
                    {
                        trace.append(" / ");
                        trace.append(value);
                    }

                    valueBuilder.setType(Type.STRING);
                }
                else
                {
                    if (logger.isDebugEnabled())
                    {
                        trace.append(" / ");
                        trace.append(value);
                    }
                    valueBuilder.setBytesValue(ByteString
                            .copyFrom((byte[]) value));
                    valueBuilder.setType(Type.BINARYSTRING);
                }
                break;
            case Types.CLOB :
                // CLOB
                if (value instanceof Clob)
                {
                    valueBuilder.setStringValue(((Clob) value).toString());
                }
                else
                    valueBuilder.setStringValue((String) value);

                if (logger.isDebugEnabled())
                {
                    trace.append(" / ");
                    trace.append(value);
                }

                valueBuilder.setType(Type.STRING);
                break;
            default :
                logger.warn("Unimplemented type " + colSpec.getType());
                break;
        }
    }

    private RowChangeData deserializeRows(ProtobufRowChangeData rows)
    {
        RowChangeData data = new RowChangeData();

        List<ProtobufOneRowChange> rowChangeList = rows.getRowChangeList();
        for (ProtobufOneRowChange oneRowChange : rowChangeList)
        {
            OneRowChange rowChange = new OneRowChange(
                    oneRowChange.getSchemaName(), oneRowChange.getTableName(),
                    RowChangeData.ActionType.valueOf(oneRowChange.getAction()
                            .name()));
            if (oneRowChange.hasTableId())
                rowChange.setTableId(oneRowChange.getTableId());

            for (ProtobufColumnSpec columnSpec : oneRowChange.getKeySpecList())
            {
                ColumnSpec c = rowChange.new ColumnSpec();
                c.setIndex(columnSpec.getIndex());
                c.setLength(columnSpec.getLength());
                c.setName(columnSpec.getName());
                c.setNotNull(columnSpec.getNotNull());
                c.setSigned(columnSpec.getSigned());
                c.setType(columnSpec.getType());
                c.setTypeDescription(columnSpec.getTypeDescription());
                rowChange.getKeySpec().add(c);
            }

            for (ProtobufColumnSpec columnSpec : oneRowChange
                    .getColumnSpecList())
            {
                ColumnSpec c = rowChange.new ColumnSpec();
                c.setIndex(columnSpec.getIndex());
                c.setLength(columnSpec.getLength());
                c.setName(columnSpec.getName());
                c.setNotNull(columnSpec.getNotNull());
                c.setSigned(columnSpec.getSigned());
                c.setType(columnSpec.getType());
                c.setTypeDescription(columnSpec.getTypeDescription());
                rowChange.getColumnSpec().add(c);
            }

            ArrayList<ColumnVal> colValues = null;

            for (ProtobufRowValue rowValue : oneRowChange.getColumnValuesList())
            {
                colValues = new ArrayList<ColumnVal>();
                for (ProtobufColumnVal columnVal : rowValue
                        .getColumnValueList())
                {
                    ColumnVal v = rowChange.new ColumnVal();
                    Serializable value = deserializeColumnValue(columnVal);
                    if (value == null)
                        v.setValueNull();
                    else
                        v.setValue(value);
                    colValues.add(v);
                }
                rowChange.getColumnValues().add(colValues);
            }

            for (ProtobufRowValue rowValue : oneRowChange.getKeyValuesList())
            {
                colValues = new ArrayList<ColumnVal>();
                for (ProtobufColumnVal columnVal : rowValue
                        .getColumnValueList())
                {
                    ColumnVal v = rowChange.new ColumnVal();
                    Serializable value = deserializeColumnValue(columnVal);
                    if (value == null)
                        v.setValueNull();
                    else
                        v.setValue(value);
                    colValues.add(v);
                }
                rowChange.getKeyValues().add(colValues);
            }

            data.appendOneRowChange(rowChange);
        }

        for (ProtobufEventOption rowsDataOption : rows.getOptionsList())
        {
            data.addOption(rowsDataOption.getName(), rowsDataOption.getValue());
        }

        return data;
    }

    private Serializable deserializeColumnValue(ProtobufColumnVal columnVal)
    {
        switch (columnVal.getType())
        {
            case NULL :
                return null;
            case INT :
                return Integer.valueOf(columnVal.getIntValue());
            case LONG :
                if (columnVal.hasLongValue())
                    return columnVal.getLongValue();
                else
                    return null;
            case STRING :
                return columnVal.getStringValue();
            case TIMESTAMP :
                if (columnVal.hasLongValue())
                {
                    Timestamp timestamp = new Timestamp(
                            columnVal.getLongValue());
                    if (columnVal.hasIntValue())
                    {
                        // When setting nanos, don't forget millis that are
                        // already stored in timestamp object
                        timestamp.setNanos(timestamp.getNanos()
                                + columnVal.getIntValue());
                    }
                    return timestamp;
                }
                else if (columnVal.hasIntValue())
                    return Integer.valueOf(0);
                break;
            case DATE :
                if (columnVal.hasLongValue())
                {
                    if (columnVal.hasStringValue())
                    {
                        // Handling DATETIME datatype (using GMT to apply)
                        Timestamp timestamp = new Timestamp(
                                columnVal.getLongValue());
                        if (columnVal.hasIntValue())
                            timestamp.setNanos(timestamp.getNanos()
                                    + columnVal.getIntValue());
                        return timestamp;
                    }
                    else
                        return new Date(columnVal.getLongValue());
                }
                else if (columnVal.hasIntValue())
                    return Integer.valueOf(0);
                break;
            case BLOB :
                byte[] blob = columnVal.getBytesValue().toByteArray();
                try
                {
                    return new SerialBlob(blob);
                }
                catch (SerialException e)
                {
                    logger.warn("Failed to deserialize blob", e);
                }
                catch (SQLException e)
                {
                    logger.warn("Failed to deserialize blob", e);
                }
                break;
            case TIME :
                if (columnVal.hasIntValue())
                {
                    // This is time with microseconds (since MySQL 5.6)
                    // We need to use timestamps in order to save microseconds
                    Timestamp time = new Timestamp(columnVal.getLongValue());

                    // When setting nanos, don't forget millis that are already
                    // stored in timestamp object
                    time.setNanos(time.getNanos() + columnVal.getIntValue());
                    return time;
                }
                else
                {
                    // Fall back on previous behavior
                    return new Time(columnVal.getLongValue());
                }
            case FLOAT :
                return Float.valueOf(columnVal.getFloatValue());
            case DOUBLE :
                return Double.valueOf(columnVal.getDoubleValue());
            case BIT :
                return Integer.valueOf(columnVal.getIntValue());
            case DECIMAL :
                return new BigDecimal(new BigInteger(columnVal.getBytesValue()
                        .toByteArray()), columnVal.getIntValue());
            case BINARYSTRING :
                return columnVal.getBytesValue().toByteArray();
            default :
                break;
        }
        return null;
    }

    private Message.Builder serializeStatement(StatementData data)
    {
        ProtobufStatementData.Builder statementBuilder = ProtobufStatementData
                .newBuilder();
        if (data.getDefaultSchema() != null)
            statementBuilder.setDefaultSchema(data.getDefaultSchema());
        if (data.getTimestamp() != null)
            statementBuilder.setTimestamp(data.getTimestamp());
        if (data.getQueryAsBytes() == null)
            statementBuilder.setQuery(data.getQuery());
        else
            statementBuilder.setQueryAsBytes(ByteString.copyFrom(data
                    .getQueryAsBytes()));

        statementBuilder.setErrorCode(data.getErrorCode());
        List<ReplOption> options = data.getOptions();
        if (options != null && !options.isEmpty())
        {
            ProtobufEventOption.Builder optionsBuilder;
            for (ReplOption replOption : options)
            {
                optionsBuilder = ProtobufEventOption.newBuilder();
                optionsBuilder.setName(replOption.getOptionName());
                optionsBuilder.setValue(replOption.getOptionValue());
                statementBuilder.addOptions(optionsBuilder);
            }
        }
        return statementBuilder;
    }

    private void serializeLoadDataFileQuery(Builder oneChangeBuilder,
            LoadDataFileQuery data)
    {
        ProtobufLoadDataFileQuery.Builder loadDataQueryBuilder = ProtobufLoadDataFileQuery
                .newBuilder();
        loadDataQueryBuilder
                .setStatement((ProtobufStatementData.Builder) serializeStatement(data));
        loadDataQueryBuilder.setFileId(data.getFileID());
        loadDataQueryBuilder.setFilenameStartPos(data.getFilenameStartPos());
        loadDataQueryBuilder.setFilenameEndPos(data.getFilenameEndPos());
        oneChangeBuilder.setFileQuery(loadDataQueryBuilder);
    }

    private void serializeLoadDataFileFragment(Builder oneChangeBuilder,
            LoadDataFileFragment data)
    {
        ProtobufLoadDataFileFragment.Builder loadDataFragBuilder = ProtobufLoadDataFileFragment
                .newBuilder();
        loadDataFragBuilder.setFileId(data.getFileID());
        loadDataFragBuilder.setData(ByteString.copyFrom(data.getData()));
        if (data.getDefaultSchema() != null)
            loadDataFragBuilder.setDatabase(data.getDefaultSchema());

        oneChangeBuilder.setFileFragment(loadDataFragBuilder);
    }

    private StatementData deserializeStatement(ProtobufStatementData statement)
    {
        StatementData statementData = new StatementData(null,
                (statement.hasTimestamp() ? statement.getTimestamp() : null),
                (statement.hasDefaultSchema()
                        ? statement.getDefaultSchema()
                        : null));
        if (statement.hasQuery())
            statementData.setQuery(statement.getQuery());
        else if (statement.hasQueryAsBytes())
            statementData.setQuery(statement.getQueryAsBytes().toByteArray());
        else
            logger.warn("Logged statement did not contain any query");

        statementData.setErrorCode(statement.getErrorCode());
        for (ProtobufEventOption statementDataOption : statement
                .getOptionsList())
        {
            statementData.addOption(statementDataOption.getName(),
                    statementDataOption.getValue());
        }
        return statementData;
    }

    private void serializeRowIdData(Builder oneChangeBuilder, RowIdData data)
    {
        oneChangeBuilder.setType(ProtobufOneChange.Type.ROW_ID_DATA);
        ProtobufRowIdData.Builder rowIdBuilder = ProtobufRowIdData.newBuilder();
        rowIdBuilder.setId(data.getRowId());
        rowIdBuilder.setType(data.getType());
        oneChangeBuilder.setRowId(rowIdBuilder);
    }

    /**
     * Debug method that add the column specification into the trace.
     * 
     * @param columnSpec
     */
    private void traceColumnSpec(ColumnSpec columnSpec)
    {
        if (logger.isDebugEnabled())
        {
            trace.append("          - ");
            trace.append("col #");
            trace.append(columnSpec.getIndex());
            trace.append(" - length = ");
            trace.append(columnSpec.getLength());
            if (columnSpec.getName() != null)
            {
                trace.append(" - ");
                trace.append(columnSpec.getName());
            }
            trace.append(" - Type :");
            trace.append(columnSpec.getType());
            trace.append("\n");
        }
    }

}
