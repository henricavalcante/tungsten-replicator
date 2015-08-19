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

package com.continuent.tungsten.replicator.backup.generic;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.backup.AbstractBackupAgent;
import com.continuent.tungsten.replicator.backup.BackupCapabilities;
import com.continuent.tungsten.replicator.backup.BackupException;
import com.continuent.tungsten.replicator.backup.BackupLocator;
import com.continuent.tungsten.replicator.backup.BackupSpecification;
import com.continuent.tungsten.replicator.backup.FileBackupLocator;
import com.continuent.tungsten.replicator.util.ProcessHelper;

/**
 * Implements a backup agent that works by calling an external script. The
 * script must support the following semantics:
 * <p>
 * <table>
 * <theader>
 * <tr>
 * <td>Operation</td>
 * <td>Syntax</td>
 * <td>Output</td>
 * </tr>
 * </theader> <tbody>
 * <tr>
 * <td>Backup</td>
 * <td><code>${script} -backup -properties filename [-options ${options}]</code>
 * </td>
 * <td>0 for success; in addition, the script must write the name of the backup
 * file into the file name provide by the -properties option.</td>
 * </tr>
 * <tr>
 * <td>Backup</td>
 * <td>
 * <code>${script} -restore -properties filename [-options ${options}]</code></td>
 * <td>0 for success; the file to restore is provided in the file indicated by
 * -properties</td>
 * </tr>
 * </tbody>
 * </table>
 * The properties file is used to exchange file information from a backup or to
 * a restore. The properties file has standard Java key=value format as shown in
 * the following example. Lines preceded by # are comments.
 * <p>
 * <code><pre>file=/tmp/backup/store-0000000021-custom-backup.tar.gz</pre></code>
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ScriptDumpAgent extends AbstractBackupAgent
{
    // Backup parameters.
    private String        script           = "my-script";
    private String        commandPrefix    = "";
    private String        options;
    private boolean       hotBackupEnabled = false;
    private String        logFilename      = null;

    private ProcessHelper processHelper;

    public ScriptDumpAgent()
    {
    }

    public String getScript()
    {
        return script;
    }

    public void setScript(String script)
    {
        this.script = script;
    }

    public String getCommandPrefix()
    {
        return commandPrefix;
    }

    public void setCommandPrefix(String commandPrefix)
    {
        this.commandPrefix = commandPrefix;
    }

    public String getOptions()
    {
        return options;
    }

    public void setOptions(String options)
    {
        this.options = options;
    }

    public boolean isHotBackupEnabled()
    {
        return hotBackupEnabled;
    }

    public void setHotBackupEnabled(boolean hotBackupEnabled)
    {
        this.hotBackupEnabled = hotBackupEnabled;
    }
    
    public String getLogFilename()
    {
        return logFilename;
    }

    public void setLogFilename(String logFilename)
    {
        this.logFilename = logFilename;
    }

    /**
     * Run backup by invoking a script. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupAgent#backup()
     */
    public BackupSpecification backup() throws BackupException
    {
        BackupSpecification spec = new BackupSpecification();
        spec.setBackupDate(new Date());

        File outputProperties = null;
        File dumpFile = null;
        BackupLocator locator = null;
        File logFile = null;
        
        try
        {
            // Generate file to which script must write file location.
            outputProperties = File.createTempFile("script-", ".properties");

            // Generate command to execute.
            ArrayList<String> cmdList = new ArrayList<String>();
            cmdList.add(script);
            cmdList.add("-backup");
            cmdList.add("-properties");
            cmdList.add(outputProperties.getAbsolutePath());
            if (options != null)
            {
                cmdList.add("-options");
                cmdList.add(options);
            }
            
            String[] backupCmd = new String[cmdList.size()];
            backupCmd = cmdList.toArray(backupCmd);

            // Execute the command.
            if (getLogFilename() != null)
            {
                logFile = new File(getLogFilename());
                if (logFile.exists())
                {
                    logFile.delete();
                }
                processHelper.exec("Dumping database using custom script",
                        backupCmd, null, logFile, logFile, true, true);
            }
            else
            {
                processHelper.exec("Dumping database using custom script",
                        backupCmd);
            }

            // Find out where the dump file is.
            TungstenProperties props = new TungstenProperties();
            FileInputStream fis = new FileInputStream(outputProperties);
            props.load(fis);
            fis.close();
            dumpFile = new File(props.getString("file"));

            // Ensure file exists and is readable.
            if (!dumpFile.canRead())
                throw new BackupException("Dump file is not readable: "
                        + dumpFile.getAbsolutePath());

            // Return a backup locator.
            locator = new FileBackupLocator(dumpFile, true);
            spec.addBackupLocator(locator);
        }
        catch (BackupException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new BackupException("Unexpected error on backup: "
                    + e.getMessage(), e);
        }
        finally
        {
            // Delete properties file.
            if (outputProperties != null)
                outputProperties.delete();

            // If we have a left over dump file, delete that too.
            if (locator == null && dumpFile != null)
                dumpFile.delete();
        }

        return spec;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#configure()
     */
    public void configure() throws BackupException
    {
        // Configure process helper.
        processHelper = new ProcessHelper();
        if (!"".equals(commandPrefix))
            processHelper.setCmdPrefix(commandPrefix);
        processHelper.configure();

        // Record capabilities.
        capabilities = new BackupCapabilities();
        capabilities.setHotBackupEnabled(hotBackupEnabled);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#release()
     */
    public void release() throws BackupException
    {
        // Nothing to do!
    }

    @Override
    protected void restoreOneLocator(BackupLocator locator)
            throws BackupException, FileNotFoundException
    {
        File inputProperties = null;
        FileOutputStream fos = null;
        File logFile = null;

        try
        {
            try
            {
                // Open locator and get backup file location.
                locator.open();
                File backupFile = locator.getContents();

                // Generate properties file with backup file location.
                inputProperties = File.createTempFile("script-", "properties");
                TungstenProperties props = new TungstenProperties();
                props.setString("file", backupFile.getAbsolutePath());

                fos = new FileOutputStream(inputProperties);
                props.store(fos);
                fos.close();

                // Generate command to execute.
                ArrayList<String> cmdList = new ArrayList<String>();
                cmdList.add(script);
                cmdList.add("-restore");
                cmdList.add("-properties");
                cmdList.add(inputProperties.getAbsolutePath());
                if (options != null)
                {
                    cmdList.add("-options");
                    cmdList.add(options);
                }
                String[] restoreCmd = new String[cmdList.size()];
                restoreCmd = cmdList.toArray(restoreCmd);

                // Execute the command.
                if (getLogFilename() != null)
                {
                    logFile = new File(getLogFilename());
                    if (logFile.exists())
                    {
                        logFile.delete();
                    }
                    processHelper.exec("Restoring database using custom script",
                            restoreCmd, null, logFile, logFile, true, true);
                }
                else
                {
                    processHelper.exec("Restoring database using custom script",
                            restoreCmd);
                }
            }
            finally
            {
                // Close locator.
                locator.release();
            }
        }
        catch (BackupException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new BackupException("Unexpected error on restore: "
                    + e.getMessage(), e);
        }
        finally
        {
            // Close file output stream.
            if (fos != null)
            {
                try
                {
                    fos.close();
                }
                catch (IOException e)
                {
                }
            }

            // Delete properties file.
            if (inputProperties != null)
                inputProperties.delete();
        }
    }
}