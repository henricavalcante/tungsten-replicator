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

/**
 * Stores information about a cluster member that is helpful in deciding whether
 * a group of members has quorum. To prevent construction of inconsistent member
 * sets the constructor and setter methods are package protected.
 */
public class ClusterMember
{
    private String  name           = null;
    private boolean configured     = false;
    private boolean inView         = false;
    private boolean passiveWitness = false;
    private boolean activeWitness  = false;
    private boolean dbMember       = false;
    private boolean validated      = false;
    private boolean reachable      = false;

    /**
     * Instantiate a new member record.
     * 
     * @param name The member name. Names must be unique within a group.
     */
    ClusterMember(String name)
    {
        this.name = name;
    }

    /** Returns the member name. */
    public String getName()
    {
        return name;
    }

    /**
     * Returns true if the member belongs to the external configuration.
     */
    public boolean isConfigured()
    {
        return configured;
    }

    /**
     * Specifies if this member is externally configured to be a member of the
     * group, regardless whether it participates in the view or not.
     */
    void setConfigured(boolean configured)
    {
        this.configured = configured;
    }

    /** Returns true if the member is in the current group view. */
    public boolean isInView()
    {
        return inView;
    }

    /**
     * Specifies whether this member is in the current group view. Members may
     * be in the group view even if they are not externally configured as
     * cluster members.
     */
    void setInView(boolean inView)
    {
        this.inView = inView;
    }

    /** Returns true if this member is actually a witness host. */
    public boolean isPassiveWitness()
    {
        return passiveWitness;
    }

    /** Specifies whether this member is a witness. */
    void setPassiveWitness(boolean witness)
    {
        this.passiveWitness = witness;
    }

    /** Returns true if this member has been validated by pinging through GC. */
    public Boolean getValidated()
    {
        return validated;
    }

    /** Specifies whether this member has been validated by pinging through GC. */
    void setValidated(Boolean validated)
    {
        this.validated = validated;
    }

    /**
     * Returns true if this member is reachable using a ping command over the
     * network.
     */
    public Boolean getReachable()
    {
        return reachable;
    }

    /**
     * Specifies whether this member is reachable using a ping command over the
     * network.
     */
    void setReachable(Boolean reachable)
    {
        this.reachable = reachable;
    }

    public boolean isActiveWitness()
    {
        return activeWitness;
    }

    public void setActiveWitness(boolean activeWitness)
    {
        this.activeWitness = activeWitness;
    }

    public boolean isDbMember()
    {
        return dbMember;
    }

    public void setDbMember(boolean dbMember)
    {
        this.dbMember = dbMember;
    }

    public String toString()
    {
        return String.format("%s(%s, validated=%s, reachable=%s)", name,
                getMemberType(), validated, reachable);
    }

    private String getMemberType()
    {
        if (isDbMember())
        {
            return "DB";
        }
        else if (isActiveWitness())
        {
            return "ACTIVE WITNESS";
        }
        else if (isPassiveWitness())
        {
            return "PASSIVE WITNESS";
        }
        else
        {
            return "UNKNOWN";
        }

    }
}