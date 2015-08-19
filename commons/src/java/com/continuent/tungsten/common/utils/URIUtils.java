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
 */

package com.continuent.tungsten.common.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Vector;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * This class defines a URLUtils
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class URIUtils
{
    private static final String   START_OF_ADDITIONAL_ARG = "&";
    private static final int      KEY_INDEX               = 0;
    private static final int      VAL_INDEX               = 1;
    private static final String   EQUALS                  = "=";
    private static final String[] validKeys               = {
            "com.continuent.tungsten.common.config.routerLatency",
            "sessionId", "qos"                            };

    public static TungstenProperties parse(String url)
            throws URISyntaxException
    {
        URI uri = null;

        // Let the URI constructor do some of the heavy lifting
        uri = new URI(url);
        return parseQuery(uri.getQuery());

    }

    public static TungstenProperties parseQuery(String query)
            throws URISyntaxException
    {
        TungstenProperties args = new TungstenProperties();

        if (query == null)
        {
            return args;
        }

        String[] argSets = query.split(START_OF_ADDITIONAL_ARG);

        for (String argSet : argSets)
        {
            String[] keyVal = argSet.split(EQUALS);
            if (keyVal.length != 2)
            {
                throw new URISyntaxException(String.format(
                        "Malformed URI, expected key=value, got ='%s'",
                        arrayToString(keyVal)), argSet);
            }
            else
            {
                if (keyVal[KEY_INDEX].length() == 0)
                {
                    throw new URISyntaxException(
                            String.format("Malformed URI, expected key=value, got empty key"),
                            argSet);
                }
                args.setString(keyVal[KEY_INDEX].trim(),
                        keyVal[VAL_INDEX].trim());
            }
        }

        return args;
    }

    private static String arrayToString(String[] array)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        int valCount = 0;
        for (String val : array)
        {
            if (valCount++ > 0)
            {
                builder.append(", ");
            }
            builder.append(val);

        }
        builder.append("}");

        return builder.toString();
    }

    public static void checkKeys(TungstenProperties props)
            throws URISyntaxException
    {
        Vector<String> invalidKeys = new Vector<String>();
        for (String key : props.keyNames())
        {
            boolean wasFound = false;
            for (String validKey : validKeys)
            {
                if (key.equals(validKey))
                {
                    wasFound = true;
                }
            }
            if (!wasFound)
            {
                invalidKeys.add(key);
            }
        }

        if (invalidKeys.size() > 0)
        {
            throw new URISyntaxException(String.format(
                    "Found one or more invalid keys. Invalid values are: '%s'\n"
                            + "Valid values are: %s", invalidKeys.toString(),
                    arrayToString(validKeys)), props.toString());

        }

    }
}
