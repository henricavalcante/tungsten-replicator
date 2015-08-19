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

package com.continuent.tungsten.common.cluster.resource;

/**
 * Lists command parameters accepted by the replicator for those JMX commands
 * that are parameterized.
 */
public class OpenReplicatorParams
{
    // Parameters for online2() JMX call.

    /** Set initial event ID when going online. */
    public static final String INIT_EVENT_ID         = "extractFromId";

    /** Set base sequence number of uninitialized log. */
    public static final String BASE_SEQNO            = "baseSeqno";

    /** Skip applying first N events after going online. */
    public static final String SKIP_APPLY_EVENTS     = "skipApplyEvents";

    /** Stay online until sequence number has been processed. */
    public static final String ONLINE_TO_SEQNO       = "toSeqno";

    /** Stay online until event ID has been processed. */
    public static final String ONLINE_TO_EVENT_ID    = "toEventId";

    /** Stay online until source timestamp has been processed. */
    public static final String ONLINE_TO_TIMESTAMP   = "toTimestamp";

    /** Stay online until next heartbeat has been processed. */
    public static final String ONLINE_TO_HEARTBEAT   = "toHeartbeat";

    /** Skip events from a list. */
    public static final String SKIP_APPLY_SEQNOS     = "skipApplySeqnos";

    /** Whether to skip consistency checks when going online. */
    public static final String FORCE                 = "force";

    /** Whether to allow checksums while online (set to false to disable). */
    public static final String DO_CHECKSUM           = "do_checksum";

    /** If set this online operation is due to auto recovery. */
    public static final String AUTO_RECOVERY         = "auto_recovery";

    /**
     * Number of milliseconds to delay while processing an online operation.
     * This is intended to allow time for clean-up during auto-recovery but
     * could have other uses as well.
     */
    public static final String ONLINE_DELAY_MILLIS   = "online_delay_millis";

    /**
     * Whether to start provisioning pipeline at startup (set to true to
     * enable).
     */
    public static final String DO_PROVISION          = "do_provision";

    // Parameters for offlineDeferred() JMX call.

    /** Go offline safely after next transactional boundary. */
    public static final String OFFLINE_TRANSACTIONAL = "atTransaction";

    /** Go offline after sequence number has been processed. */
    public static final String OFFLINE_AT_SEQNO      = "atSeqno";

    /** Go offline after event ID has been processed. */
    public static final String OFFLINE_AT_EVENT_ID   = "atEventId";

    /** Go offline after source timestamp has been processed. */
    public static final String OFFLINE_AT_TIMESTAMP  = "atTimestamp";

    /** Go offline after next heartbeat has been processed. */
    public static final String OFFLINE_AT_HEARTBEAT  = "atHeartbeat";

    // Parameters for heartbeat() JMX call.

    /** Name of heartbeat. */
    public static final String HEARTBEAT_NAME        = "heartbeatName";

    // Parameters for purge JMX call.

    /** Timeout in seconds to wait for a purge operation. */
    public static final String TIMEOUT               = "timeout";
}
