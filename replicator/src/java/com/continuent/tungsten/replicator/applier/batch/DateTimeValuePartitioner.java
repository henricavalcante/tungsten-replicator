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

package com.continuent.tungsten.replicator.applier.batch;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Implements a value partitioner that always returns an empty string.
 */
public class DateTimeValuePartitioner implements ValuePartitioner
{
    SimpleDateFormat formatter = new SimpleDateFormat();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ValuePartitioner#setFormat(java.lang.String)
     */
    public void setFormat(String format)
    {
        formatter.applyPattern(format);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ValuePartitioner#setTimeZone(java.util.TimeZone)
     */
    public void setTimeZone(TimeZone tz)
    {
        formatter.setTimeZone(tz);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ValuePartitioner#partition(java.lang.Object)
     */
    public String partition(Object value)
    {
        return formatter.format(value);
    }
}
