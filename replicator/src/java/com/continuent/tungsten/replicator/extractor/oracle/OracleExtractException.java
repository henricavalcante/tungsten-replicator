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
 * Initial developer(s): Scott Martin
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.oracle;

import com.continuent.tungsten.replicator.extractor.ExtractorException;

/**
 * This class defines an OracleExtractException
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */

public class OracleExtractException extends ExtractorException
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new <code>OracleExtractException</code> object
     * 
     * @param message
     * @param cause
     */
    public OracleExtractException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Creates a new <code>OracleExtractException</code> object
     * 
     * @param message
     */
    public OracleExtractException(String message)
    {
        super(message);
    }

    /**
     * Creates a new <code>OracleExtractException</code> object
     * 
     * @param cause
     */
    public OracleExtractException(Throwable cause)
    {
        super(cause);
    }
}
