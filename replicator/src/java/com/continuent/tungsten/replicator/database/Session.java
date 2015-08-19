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
 * This class defines an active session on the DBMS server.
 */
public class Session
{
    private String login;
    private Object identifier;

    /**
     * Creates an empty session definition.
     */
    public Session()
    {
    }

    /**
     * Creates a new session with preset values.
     * 
     * @param login User name
     * @param identifier A DBMS-specific value that can be used to kill the
     *            session
     */
    public Session(String login, Object identifier)
    {
        this.login = login;
        this.identifier = identifier;
    }

    public String getLogin()
    {
        return login;
    }

    public void setLogin(String login)
    {
        this.login = login;
    }

    public Object getIdentifier()
    {
        return identifier;
    }

    public void setIdentifier(Object identifier)
    {
        this.identifier = identifier;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(" identifier=").append(identifier);
        sb.append(" login=").append(login);
        return sb.toString();
    }
}