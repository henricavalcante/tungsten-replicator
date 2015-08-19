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

import java.util.TimeZone;

/**
 * Denotes a file that partitions a data value based on a key generated from an
 * argument passed to the partition method.
 */
public interface ValuePartitioner
{
    /** Format string for partitioner set in configuration file. */
    public void setFormat(String format);

    /** Time zone set in configuration file. */
    public void setTimeZone(TimeZone tz);

    /** Return a key that can be used to group data. */
    public String partition(Object value);
}
