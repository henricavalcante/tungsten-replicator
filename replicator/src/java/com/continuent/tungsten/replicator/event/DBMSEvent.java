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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.event;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.continuent.tungsten.replicator.dbms.DBMSData;

/**
 * Contains SQL row updates and/or statements that must be replicated.
 * Extractors generate updates using this class and appliers receive updates in
 * it. Each instance is implicitly a single transaction.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class DBMSEvent implements Serializable
{
    private static final long      serialVersionUID = 1300L;
    private String                 id;
    private LinkedList<ReplOption> metadata;
    private ArrayList<DBMSData>    data;
    private boolean                lastFrag;
    private Timestamp              sourceTstamp;
    private LinkedList<ReplOption> options;

    /**
     * Creates a new instance of raw replicated data.
     * 
     * @param id Native transaction ID
     * @param metadata List of name/value pairs containing metadata about this
     *            event
     * @param data List of SQL statements or row updates
     * @param lastFrag True if this is the last fragment of a transaction
     * @param sourceTstamp Time of the transaction
     */
    public DBMSEvent(String id, LinkedList<ReplOption> metadata,
            ArrayList<DBMSData> data, boolean lastFrag, Timestamp sourceTstamp)
    {
        // Eliminate all possibilities of null pointers.
        if (id == null)
            this.id = "NIL";
        else
            this.id = id;
        if (metadata == null)
            this.metadata = new LinkedList<ReplOption>();
        else
            this.metadata = metadata;
        if (data == null)
            this.data = new ArrayList<DBMSData>();
        else
            this.data = data;
        this.lastFrag = lastFrag;
        if (sourceTstamp == null)
            this.sourceTstamp = new Timestamp(System.currentTimeMillis());
        else
            this.sourceTstamp = sourceTstamp;
        options = new LinkedList<ReplOption>();
    }

    public DBMSEvent(String id, ArrayList<DBMSData> data, Timestamp sourceTstamp)
    {
        this(id, new LinkedList<ReplOption>(), data, true, sourceTstamp);
    }

    public DBMSEvent(String id, ArrayList<DBMSData> data, boolean lastFrag,
            Timestamp sourceTstamp)
    {
        this(id, new LinkedList<ReplOption>(), data, lastFrag, sourceTstamp);
    }

    public DBMSEvent(String id, LinkedList<ReplOption> metadata,
            ArrayList<DBMSData> data, Timestamp sourceTstamp)
    {
        this(id, metadata, data, true, sourceTstamp);
    }

    /**
     * Constructor for dummy DBMSEvent with an event ID only; all other values
     * are defaults
     */
    public DBMSEvent(String id)
    {
        this(id, null, null, true, null);
    }

    /**
     * Constructor for dummy DBMSEvents. All values are defaults.
     */
    public DBMSEvent()
    {
        this(null, null, null, true, null);
    }

    /**
     * Returns the native event ID.
     * 
     * @return id
     */
    public String getEventId()
    {
        return id;
    }

    /**
     * Returns the metadata options.
     * 
     * @return metadata
     */
    public LinkedList<ReplOption> getMetadata()
    {
        return metadata;
    }

    /**
     * Adds a metadata option, which is assumed not to exist previously.
     */
    public void addMetadataOption(String name, String value)
    {
        metadata.add(new ReplOption(name, value));
    }

    /**
     * Sets an existing metadata option or if absent adds it.
     */
    public void setMetaDataOption(String name, String value)
    {
        for (int i = 0; i < metadata.size(); i++)
        {
            ReplOption option = metadata.get(i);
            if (name.equals(option.getOptionName()))
            {
                metadata.set(i, new ReplOption(name, value));
                return;
            }
        }
        addMetadataOption(name, value);
    }

    /**
     * Gets a metadata option.
     */
    public ReplOption getMetadataOption(String name)
    {
        for (ReplOption option : metadata)
        {
            if (name.equals(option.getOptionName()))
                return option;
        }
        return null;
    }

    /**
     * Gets a metadata value..
     */
    public String getMetadataOptionValue(String name)
    {
        for (ReplOption option : metadata)
        {
            if (name.equals(option.getOptionName()))
                return option.getOptionValue();
        }
        return null;
    }

    /**
     * Removes an existing metadata option if it exists and returns the value.
     */
    public String removeMetadataOption(String name)
    {
        // Remove previous value, if any.
        ReplOption existingOption = null;
        for (ReplOption replOption : metadata)
        {
            if (name.equals(replOption.getOptionName()))
                existingOption = replOption;
        }
        if (existingOption != null)
        {
            metadata.remove(existingOption);
            return existingOption.getOptionValue();
        }
        return null;
    }

    /**
     * Returns all database updates.
     * 
     * @return data
     */
    public ArrayList<DBMSData> getData()
    {
        return data;
    }

    /**
     * Returns true if this is the last fragment of a transaction.
     */
    public boolean isLastFrag()
    {
        return lastFrag;
    }

    /**
     * Returns the source timestamp, i.e., when the transaction occurred.
     * 
     * @return Returns the sourceTstamp.
     */
    public Timestamp getSourceTstamp()
    {
        return sourceTstamp;
    }

    public void setOptions(LinkedList<ReplOption> savedOptions)
    {
        this.options.addAll(savedOptions);
    }

    public List<ReplOption> getOptions()
    {
        return options;
    }

    public void addOption(String name, String value)
    {
        options.add(new ReplOption(name, value));
    }

}
