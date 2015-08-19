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

package com.continuent.tungsten.replicator.thl;

/**
 * This class provides a list of defined Protocol class option names.
 */
public class ProtocolParams
{
    /**
     * Initial event ID to search for when starting. If present value supercedes
     * the sequence number value provided by the client. The server will instead
     * search for and return the transaction that matches this native event ID.
     */
    public static final String INIT_EVENT_ID = "extractFromId";
    
    /** Client's RMI host. **/
    public static final String RMI_HOST = "rmiHost";
    
    /** Client's RMI port. **/
    public static final String RMI_PORT = "rmiPort";
}