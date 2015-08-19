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

package com.continuent.tungsten.common.sockets;

import java.io.IOException;

/**
 * Denotes an exception that occurs when an operation on a socket fails due to
 * the socket being closed in another other thread.
 */
public class SocketTerminationException extends IOException
{
    private static final long serialVersionUID = 1L;

    /**
     * Instantiate a new exception, which includes the underlying exception
     * trapped by the socket wrapper code.
     */
    SocketTerminationException(String msg, IOException trappedException)
    {
        super(msg, trappedException);
    }
}
