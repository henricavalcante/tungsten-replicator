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
 * Contributor(s): Alex Yurchenko
 */

package com.continuent.tungsten.common.patterns.notification.adaptor;

public class NotificationAdaptorException extends Exception
{
    private static final long serialVersionUID = 8339994478408859274L;

    public NotificationAdaptorException(String reason)
    {
        super(reason);
    }

    public NotificationAdaptorException(String reason, Throwable cause)
    {
        super(reason, cause);
    }
}
