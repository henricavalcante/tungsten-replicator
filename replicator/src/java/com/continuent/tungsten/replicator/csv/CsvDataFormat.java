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

package com.continuent.tungsten.replicator.csv;

import java.util.TimeZone;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Denotes a class that formats object for writing to CSV. Instances of this
 * class allow users to choose a preferred format for representing string
 * values, which can vary independently of the conventions for CSV formatting
 * such as line and column separator characters.
 */
public interface CsvDataFormat
{
    /** Time zone to use for date/time conversions */
    public void setTimeZone(TimeZone tz);

    /** Ready the converter for use. */
    public void prepare();

    /** Converts value to a CSV-ready string. */
    public String csvString(Object value, int javaType, boolean blob)
            throws ReplicatorException;
}