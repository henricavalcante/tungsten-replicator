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

package com.continuent.tungsten.replicator.database;

/**
 * Denotes an interface to set and fetch comments in SQL statements.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface SqlCommentEditor
{
    /**
     * If true, comment editing is enabled.  If false, comment editing
     * is ignored and we just return the current statement. 
     */
    public void setCommentEditingEnabled(boolean enabled);
    
    /**
     * Inserts a comment safely into a SQL statement.
     * 
     * @param statement Statement that requires a comment to be inserted.
     * @param sqlOperation Metadata from parsing statement, if any
     * @param comment Comment string to be added.
     * @return Query with comment added
     */
    public String addComment(String statement, SqlOperation sqlOperation,
            String comment);

    /**
     * Formats an appendable comment if this works.
     * 
     * @param sqlOperation Metadata from parsing statement
     * @param comment Comment string to be added.
     * @return Appendable comment or null if such comments are not safe for
     *         current statement
     */
    public String formatAppendableComment(SqlOperation sqlOperation,
            String comment);

    /**
     * Set comment regex. This is used to fetch out specific parts of the
     * comment and is set once to avoid regex recompilation.
     */
    public void setCommentRegex(String regex);

    /**
     * Fetches the first comment string that matches the regex.
     * 
     * @param baseStatement Statement to search
     * @return Matching comment string or null if not found.
     */
    public String fetchComment(String baseStatement, SqlOperation sqlOperation);
}