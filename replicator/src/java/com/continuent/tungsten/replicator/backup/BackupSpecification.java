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
package com.continuent.tungsten.replicator.backup;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Contains backup metadata.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BackupSpecification
{
    private String agentName;
    private List<BackupLocator> backupLocators;
    private Date backupDate;
    
    public BackupSpecification()
    {
    }

    public String getAgentName()
    {
        return agentName;
    }

    public void setAgentName(String agentName)
    {
        this.agentName = agentName;
    }

    public List<BackupLocator> getBackupLocators()
    {
        return backupLocators;
    }

    public Date getBackupDate()
    {
        return backupDate;
    }

    public void setBackupDate(Date backupDate)
    {
        this.backupDate = backupDate;
    }
    
    public void addBackupLocator(BackupLocator locator)
    {
        if(backupLocators == null)
            backupLocators = new ArrayList<BackupLocator>();
        backupLocators.add(locator);
    }

    public void releaseLocators()
    {
        for (BackupLocator locator : backupLocators)
        {
            locator.release();
        }
    }
}