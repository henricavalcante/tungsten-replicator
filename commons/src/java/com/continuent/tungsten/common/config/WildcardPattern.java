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

package com.continuent.tungsten.common.config;

/**
 * Represents methods used to work with wildcard patterns.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class WildcardPattern
{
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;

    /**
     * Converts wildcard pattern to a regular expression pattern.
     * 
     * @param wildcard String that might contain * and ? wildcards.
     * @return Regular expression matching ready string.
     */
    public static String wildcardToRegex(String wildcard)
    {
        StringBuffer s = new StringBuffer(wildcard.length());
        s.append('^');
        for (int i = 0; i < wildcard.length(); i++)
        {
            char c = wildcard.charAt(i);
            switch (c)
            {
                // Support for * and ? wildcards:
                case '*' :
                    s.append(".*");
                    break;
                case '?' :
                    s.append(".");
                    break;
                // Escape special regular expression characters:
                case '(' :
                case ')' :
                case '[' :
                case ']' :
                case '$' :
                case '^' :
                case '.' :
                case '{' :
                case '}' :
                case '|' :
                case '\\' :
                    s.append("\\");
                    s.append(c);
                    break;
                default :
                    s.append(c);
                    break;
            }
        }
        s.append('$');
        return s.toString();
    }
}