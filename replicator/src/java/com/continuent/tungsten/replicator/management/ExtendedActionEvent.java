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
 */

package com.continuent.tungsten.replicator.management;

import java.util.regex.Pattern;

import com.continuent.tungsten.fsm.core.Action;
import com.continuent.tungsten.fsm.core.Event;

/**
 * Defines an event containing an extended command which a regexp specifying
 * states in which the command may be legally processed.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ExtendedActionEvent extends Event
{
    private final String  stateRegexp;
    private final Pattern statePattern;
    private final Action  extendedAction;

    public ExtendedActionEvent(String stateRegexp, Action extendedAction)
    {
        super(null);
        this.stateRegexp = stateRegexp;
        this.extendedAction = extendedAction;
        this.statePattern = Pattern.compile(stateRegexp);
    }

    public String getStateRegexp()
    {
        return stateRegexp;
    }

    public Action getExtendedAction()
    {
        return extendedAction;
    }

    public Pattern getStatePattern()
    {
        return statePattern;
    }
}