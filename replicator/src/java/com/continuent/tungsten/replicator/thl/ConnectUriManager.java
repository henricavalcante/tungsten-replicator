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

package com.continuent.tungsten.replicator.thl;

import java.util.List;

/**
 * Implements a simple strategy class that hands out connection URIs and allows
 * the caller to cycle through a list of them.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ConnectUriManager
{
    private int          index      = 0;
    private List<String> uriList;
    private long         iterations = 0;

    /**
     * Creates a new instance with a list of URIs.
     * 
     * @param connectUri List of one or more URIs
     * @throws THLException Thrown if array is 0 length
     */
    public ConnectUriManager(List<String> connectUri) throws THLException
    {
        this.uriList = connectUri;
        if (connectUri.size() == 0)
        {
            throw new THLException(
                    "Connect URI value is empty; must be a list of one or more THL URIs");
        }
    }

    /**
     * Returns the next THL URI in the list.
     */
    public String next()
    {
        String uri = uriList.get(index++);
        if (index >= uriList.size())
        {
            index = 0;
            iterations++;
        }
        return uri;
    }

    /**
     * Returns the number of full iterations over the list of URIs.
     */
    public long getIterations()
    {
        return iterations;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < uriList.size(); i++)
        {
            if (i > 0)
                sb.append(",");
            sb.append(uriList.get(i));
        }
        return sb.toString();
    }
}