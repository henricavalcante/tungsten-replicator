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

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * This class is used to test setting property values.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SampleObject
{
    private String       string;
    private int          myInt;
    private long         myLong;
    private float        myFloat;
    private double       myDouble;
    private boolean      myBoolean;
    private char         myChar;
    private Date         myDate;
    private BigDecimal   myBigDecimal;
    private SampleEnum   myEnum;
    private List<String> myStringList;

    public SampleObject()
    {
    }

    public String getString()
    {
        return string;
    }

    public void setString(String string)
    {
        this.string = string;
    }

    public int getMyInt()
    {
        return myInt;
    }

    public void setMyInt(int myInt)
    {
        this.myInt = myInt;
    }

    public long getMyLong()
    {
        return myLong;
    }

    public void setMyLong(long myLong)
    {
        this.myLong = myLong;
    }

    public float getMyFloat()
    {
        return myFloat;
    }

    public void setMyFloat(float myFloat)
    {
        this.myFloat = myFloat;
    }

    public double getMyDouble()
    {
        return myDouble;
    }

    public void setMyDouble(double myDouble)
    {
        this.myDouble = myDouble;
    }

    public boolean isMyBoolean()
    {
        return myBoolean;
    }

    public void setMyBoolean(boolean myBoolean)
    {
        this.myBoolean = myBoolean;
    }

    public char getMyChar()
    {
        return myChar;
    }

    public void setMyChar(char myChar)
    {
        this.myChar = myChar;
    }

    public Date getMyDate()
    {
        return myDate;
    }

    public void setMyDate(Date myDate)
    {
        this.myDate = myDate;
    }

    public BigDecimal getMyBigDecimal()
    {
        return this.myBigDecimal;
    }

    public void setMyBigDecimal(BigDecimal myBigDecimal)
    {
        this.myBigDecimal = myBigDecimal;
    }

    public SampleEnum getMyEnum()
    {
        return this.myEnum;
    }

    public void setMyEnum(SampleEnum myEnum)
    {
        this.myEnum = myEnum;
    }

    public List<String> getMyStringList()
    {
        return this.myStringList;
    }

    public void setMyStringList(List<String> myStringList)
    {
        this.myStringList = myStringList;
    }

    public boolean equals(Object o)
    {
        if (o == null)
            return false;
        if (!(o instanceof SampleObject))
            return false;
        SampleObject to = (SampleObject) o;
        if (string == null)
        {
            if (to.getString() != null)
                return false;
        }
        else
        {
            if (!string.equals(to.getString()))
                return false;
        }
        if (myInt != to.getMyInt())
            return false;
        if (myLong != to.getMyLong())
            return false;
        if (myFloat != to.getMyFloat())
            return false;
        if (myDouble != to.getMyDouble())
            return false;
        if (myBoolean != to.isMyBoolean())
            return false;
        if (myChar != to.getMyChar())
            return false;
        if (myDate != to.getMyDate())
            return false;
        if (myBigDecimal != to.getMyBigDecimal())
            return false;
        if (myEnum != to.getMyEnum())
            return false;
        if (myStringList == null)
        {
            if (to.getMyStringList() != null)
                return false;
        }
        else
        {
            if (!myStringList.equals(to.getMyStringList()))
                return false;
        }
        return true;
    }

    public enum SampleEnum
    {
        ONE, TWO, THREE
    }
}