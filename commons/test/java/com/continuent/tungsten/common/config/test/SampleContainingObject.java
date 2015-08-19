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

package com.continuent.tungsten.common.config.test;

/**
 * This class is used to test setting values on an object that contains embedded
 * Java beans.
 */
public class SampleContainingObject
{
    private String       myString;
    private SampleObject myObject1;
    private SampleObject myObject2;

    public SampleContainingObject()
    {
    }

    public String getMyString()
    {
        return myString;
    }

    public void setMyString(String string)
    {
        this.myString = string;
    }

    public SampleObject getMyObject1()
    {
        return myObject1;
    }

    public void setMyObject1(SampleObject myObject1)
    {
        this.myObject1 = myObject1;
    }

    public SampleObject getMyObject2()
    {
        return myObject2;
    }

    public void setMyObject2(SampleObject myObject2)
    {
        this.myObject2 = myObject2;
    }

    public boolean equals(Object o)
    {
        if (o == null)
            return false;
        if (!(o instanceof SampleContainingObject))
            return false;

        SampleContainingObject to = (SampleContainingObject) o;

        if (!compare(myString, to.getMyString()))
            return false;
        if (!compare(myObject1, to.getMyObject1()))
            return false;
        if (!compare(myObject2, to.getMyObject2()))
            return false;

        return true;
    }

    // Object comparison helper method.
    private boolean compare(Object o1, Object o2)
    {
        if (o1 == null)
            return (o2 == null);
        else if (!o1.getClass().equals(o2.getClass()))
            return false;
        else if (!o1.equals(o2))
            return false;
        else
            return true;
    }

    public enum SampleEnum
    {
        ONE, TWO, THREE
    }
}