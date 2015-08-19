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

package com.continuent.tungsten.replicator;

import java.io.Serializable;

/**
 * This class defines a StateChangeNotification
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class StateChangeNotification implements Serializable
{

    static final long serialVersionUID = 1L;
    String            prevState        = null;
    String            newState         = null;
    String            cause            = null;

    /**
     * Creates a new <code>StateChangeNotification</code> object
     * 
     * @param prevState
     * @param newState
     * @param cause
     */
    public StateChangeNotification(String prevState, String newState,
            String cause)
    {
        this.prevState = prevState;
        this.newState = newState;
        this.cause = cause;
    }

    /**
     * Returns the previous state.
     */
    public String getPrevState()
    {
        return prevState;
    }

    /**
     * Returns the new state.
     */
    public String getNewState()
    {
        return newState;
    }

    /**
     * Returns the cause of the state change.
     */
    public String getCause()
    {
        return cause;
    }
}
