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
 * This class defines a database user.
 */
public class User
{
    private String  login;
    private String  password;
    private boolean privileged;

    /**
     * Creates an empty user definition.
     */
    public User()
    {
    }

    /**
     * Creates a new user with preset values.
     * 
     * @param login User login
     * @param password Password for account
     * @param privileged If true, this is a superuser login
     */
    public User(String login, String password, boolean privileged)
    {
        this.login = login;
        this.password = password;
        this.privileged = privileged;
    }

    public String getLogin()
    {
        return login;
    }

    public void setLogin(String login)
    {
        this.login = login;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public boolean isPrivileged()
    {
        return privileged;
    }

    public void setPrivileged(boolean privileged)
    {
        this.privileged = privileged;
    }
}
