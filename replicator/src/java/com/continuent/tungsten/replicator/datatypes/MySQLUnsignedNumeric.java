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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):  
 */

package com.continuent.tungsten.replicator.datatypes;

import java.math.BigInteger;

import org.apache.log4j.Logger;

/**
 * This class represents an UNSIGNED MySQL numeric data type.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class MySQLUnsignedNumeric
{
    private static Logger          logger              = Logger.getLogger(MySQLUnsignedNumeric.class);

    public static final int        TINYINT_MAX_VALUE   = 255;
    public static final int        SMALLINT_MAX_VALUE  = 65535;
    public static final int        MEDIUMINT_MAX_VALUE = 16777215;
    public static final long       INTEGER_MAX_VALUE   = 4294967295L;
    public static final BigInteger BIGINT_MAX_VALUE    = new BigInteger(
                                                               "18446744073709551615");

    /**
     * Converts raw *negative* extracted from the binary log *unsigned* numeric
     * value into correct *positive* numeric representation. MySQL saves large
     * unsigned values as signed (negative) ones in the binary log, hence the
     * need for transformation.<br/>
     * See Issue 798 for more details.<br/>
     * Make sure your numeric value is really negative before calling this.
     * 
     * @return Converted positive number if value was negative. Same value, if
     *         it was already positive. null, if column specification was not
     *         supported.
     * @param numeric Numeric object value to convert.
     */
    public static Object negativeToMeaningful(Numeric numeric)
    {
        Object valToInsert = null;
        if (numeric.isNegative())
        {
            // Convert raw negative unsigned to positive as MySQL does.
            switch (numeric.getColumnSpec().getLength())
            {
                case 1 :
                    valToInsert = TINYINT_MAX_VALUE + 1
                            + numeric.getExtractedValue();
                    break;
                case 2 :
                    valToInsert = SMALLINT_MAX_VALUE + 1
                            + numeric.getExtractedValue();
                    break;
                case 3 :
                    valToInsert = MEDIUMINT_MAX_VALUE + 1
                            + numeric.getExtractedValue();
                    break;
                case 4 :
                    valToInsert = INTEGER_MAX_VALUE + 1
                            + numeric.getExtractedValue();
                    break;
                case 8 :
                    valToInsert = BIGINT_MAX_VALUE.add(BigInteger
                            .valueOf(1 + numeric.getExtractedValue()));
                    break;
                default :
                    logger.warn("Column length unsupported: "
                            + numeric.getColumnSpec().getLength());
                    break;
            }
            if (logger.isDebugEnabled())
                logger.debug(numeric.getExtractedValue() + " -> " + valToInsert);
        }
        else
        {
            // Positive value already - leaving as is.
            valToInsert = numeric.getExtractedValue();
        }
        return valToInsert;
    }
}
