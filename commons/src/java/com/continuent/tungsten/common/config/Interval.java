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

package com.continuent.tungsten.common.config;

/**
 * Implements a time interval, which is expressed in milliseconds. Intervals can
 * be manipulated as long values but also translate to strings of the form </p>
 * 
 * <pre>NNN{d|h|m|s}</pre>
 * 
 * </p> where NNN is a number and the letter following denotes a time unit of
 * days, hours, minutes, or seconds, respectively. If the time unit is left off
 * the value is assumed to be milliseconds.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class Interval
{
    private long duration;

    public Interval(long duration)
    {
        this.duration = duration;
    }

    /**
     * Creates an interval from a string.
     */
    public Interval(String duration) throws NumberFormatException
    {
        this.duration = parseDurationString(duration);
    }

    // Parses the duration string.
    private long parseDurationString(String duration)
            throws NumberFormatException
    {
        // Parse out the number. Ensure we have a valid string with NNN + one
        // character for the unit type.
        duration = duration.trim();
        int index = 0;
        StringBuffer numberBuf = new StringBuffer();
        for (; index < duration.length()
                && Character.isDigit(duration.charAt(index)); index++)
        {
            numberBuf.append(duration.charAt(index));
        }
        if (index == 0 || index + 1 < duration.length())
            throw new NumberFormatException(
                    "Invalid interval format; must be NNN{d|h|m|s}: "
                            + duration);

        // Convert the number. If we are at the end of the string, we are done.
        long number = new Long(numberBuf.toString());
        if (index == duration.length())
            return number;

        // Parse out the units.
        char unit = duration.charAt(index);
        int multiplier = -1;
        switch (Character.toLowerCase(unit))
        {
            case 's' :
                multiplier = 1000;
                break;
            case 'm' :
                multiplier = 1000 * 60;
                break;
            case 'h' :
                multiplier = 1000 * 60 * 60;
                break;
            case 'd' :
                multiplier = 1000 * 60 * 60 * 24;
                break;
            default :
                throw new NumberFormatException(
                        "Invalid interval format; must be NNN{d|h|m|s}: "
                                + duration);
        }

        return (long) number * multiplier;
    }

    /** Return interval as millisecond value. */
    public long longValue()
    {
        return duration;
    }

    /**
     * Returns true if the start and end times are greater than the duration of
     * this interval.
     * 
     * @param startMillis Start time in milliseconds
     * @param endMillis End time in milliseconds
     * @return true if the given interval is greater that this instance one,
     *         false otherwise
     */
    public boolean overInterval(long startMillis, long endMillis)
    {
        return (endMillis - startMillis) > duration;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof Interval))
            return false;
        else
        {
            return duration == ((Interval) o).longValue();
        }
    }
}
