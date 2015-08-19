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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.common.cluster.resource;

import java.text.MessageFormat;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;

public enum QualityOfService
{

    RW_STRICT, RW_RELAXED, RO_STRICT, RO_RELAXED, RW_SESSION, UNDEFINED;
    
    public static QualityOfService fromString(String x) throws ConfigurationException
    {
        for (QualityOfService currentType : QualityOfService.values())
        {
            if (x.equalsIgnoreCase(currentType.toString()))
                return currentType;
        }
        throw new ConfigurationException( MessageFormat.format(
                        "Cannot cast into a known COMMAND: {0}", x));
    }
}
