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

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Denotes a class that validates client responses to the protocol handshake.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ProtocolHandshakeResponseValidator
{
    /**
     * Executes implementation-defined logic to validate the client response.
     * 
     * @param handshakeResponse Response received from client
     * @throws ReplicatorException Thrown if validation fails
     */
    public void validateResponse(ProtocolHandshakeResponse handshakeResponse)
            throws ReplicatorException, InterruptedException;
}
