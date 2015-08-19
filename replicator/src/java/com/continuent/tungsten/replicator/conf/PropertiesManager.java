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

package com.continuent.tungsten.replicator.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Provides consolidated handling of properties in replicator. Properties
 * consist of static properties read only from replicator.properties and dynamic
 * properties which are settable from client calls and stored in
 * dynamic.properties. Dynamic properties, if set, take precedence over static
 * properties.
 * <p>
 * This class has synchronization required to ensure properties operations are
 * visible across threads and to prevent property value inconsistencies when
 * writing and reading properties at the same time.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PropertiesManager
{
    private static Logger      logger = Logger.getLogger(PropertiesManager.class);

    private TungstenProperties staticProperties;
    private TungstenProperties dynamicProperties;

    // Locations of property files.
    private final File         staticPropertiesFile;
    private final File         dynamicPropertiesFile;
    private final File         dynamicRoleFile;

    /**
     * Creates a new <code>PropertiesManager</code> object
     * 
     * @param staticPropertiesFile File containing static properties
     *            (replicator.properties), which must exist
     * @param dynamicPropertiesFile File containing dynamic properties, which
     *            does not have to exist
     * @param dynamicRoleFile File containing dynamic role, which likewise does
     *            not have to exist
     */
    public PropertiesManager(File staticPropertiesFile,
            File dynamicPropertiesFile, File dynamicRoleFile)
    {
        this.staticPropertiesFile = staticPropertiesFile;
        this.dynamicPropertiesFile = dynamicPropertiesFile;
        this.dynamicRoleFile = dynamicRoleFile;
    }

    // Loads all properties.
    public void loadProperties() throws ReplicatorException
    {
        loadStaticProperties();
        loadDynamicProperties();
    }

    /**
     * Returns current state of properties. Dynamic properties are automatically
     * read from files and merged to their current values. You must load
     * properties before calling this method.
     */
    public synchronized TungstenProperties getProperties()
    {
        TungstenProperties rawProps = new TungstenProperties();
        rawProps.putAll(staticProperties);
        rawProps.putAll(dynamicProperties);

        // Kludge to perform variable substitutions on merged properties.
        // Otherwise we lose substitutions that come from the dynamic
        // properties
        Properties substitutedProps = rawProps.getProperties();
        TungstenProperties.substituteSystemValues(substitutedProps, 10);
        TungstenProperties props = new TungstenProperties();
        props.load(substitutedProps);
        return props;
    }

    /**
     * Clear in-memory dynamic properties and delete on-disk file, if it exists.
     */
    public synchronized void clearDynamicProperties()
            throws ReplicatorException
    {
        logger.info("Clearing dynamic properties");

        // Check for null; this may be invoked before properties are loaded.
        if (dynamicProperties != null)
            dynamicProperties.clear();
        if (dynamicPropertiesFile.exists())
        {
            if (!dynamicPropertiesFile.delete())
                logger.error("Unable to delete dynamic properties file: "
                        + dynamicPropertiesFile.getAbsolutePath());
        }

        // Clear the dynamic role file.
        if (dynamicRoleFile != null)
        {
            if (dynamicRoleFile.exists())
            {
                if (!dynamicRoleFile.delete())
                    logger.error("Unable to delete dynamic role file: "
                            + dynamicRoleFile.getAbsolutePath());
            }
        }
    }

    /**
     * Return current values of all supported dynamic values.
     */
    public synchronized TungstenProperties getDynamicProperties()
            throws ReplicatorException
    {
        validateProperties();
        TungstenProperties dynamic = new TungstenProperties();
        TungstenProperties all = getProperties();
        for (String dynamicName : ReplicatorConf.DYNAMIC_PROPERTIES)
        {
            dynamic.setString(dynamicName, all.getString(dynamicName));
        }
        return dynamic;
    }

    /**
     * Sets one or more dynamic properties after checking we permit them to be
     * set.
     */
    public synchronized void setDynamicProperties(TungstenProperties dynaProps)
            throws ReplicatorException
    {
        validateProperties();

        // Ensure each property may be set dynamically.
        for (String name : dynaProps.keyNames())
        {
            boolean settable = false;
            for (String settableName : ReplicatorConf.DYNAMIC_PROPERTIES)
            {
                if (settableName.equals(name))
                {
                    settable = true;
                    break;
                }
            }
            if (!settable)
                throw new ReplicatorException(
                        "Property does not exist or is not dynamically settable: "
                                + name);
        }

        // Update dynamic properties.
        dynamicProperties.putAll(dynaProps);
        FileOutputStream fos = null;
        try
        {
            fos = new FileOutputStream(dynamicPropertiesFile);
            dynamicProperties.store(fos);
        }
        catch (IOException e)
        {
            String msg = "Unable to write dymamic properties file: "
                    + dynamicPropertiesFile.getAbsolutePath();
            throw new ReplicatorException(msg, e);
        }
        finally
        {
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
        }

        // List updated properties.
        for (String name : dynaProps.keyNames())
        {
            logger.info("Dynamic property updated: name=" + name + " value="
                    + dynaProps.getString(name));
        }
    }

    // Ensure properties are loaded.
    private void validateProperties() throws ReplicatorException
    {
        if (staticProperties == null || dynamicProperties == null)
        {
            loadProperties();
        }
    }

    /**
     * Load static properties from current replicator.properties location.
     */
    private void loadStaticProperties() throws ReplicatorException
    {
        logger.debug("Reading static properties file: "
                + staticPropertiesFile.getAbsolutePath());
        staticProperties = loadProperties(staticPropertiesFile);
    }

    /**
     * Load dynamic properties from current dynamic.properties location. If the
     * properties file does not exist, we make the properties empty.
     */
    private void loadDynamicProperties() throws ReplicatorException
    {
        if (dynamicPropertiesFile.exists())
        {
            logger.debug("Reading dynamic properties file: "
                    + dynamicPropertiesFile.getAbsolutePath());
            dynamicProperties = loadProperties(dynamicPropertiesFile);
        }
        else
            dynamicProperties = new TungstenProperties();
    }

    // Loads a properties file throwing an exception if there is a failure.
    public static TungstenProperties loadProperties(File propsFile)
            throws ReplicatorException
    {
        try
        {
            TungstenProperties newProps = new TungstenProperties();
            newProps.load(new FileInputStream(propsFile), false);
            return newProps;
        }
        catch (FileNotFoundException e)
        {
            logger.error("Unable to find properties file: " + propsFile);
            logger.debug("Properties search failure", e);
            throw new ReplicatorException("Unable to find properties file: "
                    + e.getMessage());
        }
        catch (IOException e)
        {
            logger.error("Unable to read properties file: " + propsFile);
            logger.debug("Properties read failure", e);
            throw new ReplicatorException("Unable to read properties file: "
                    + e.getMessage());
        }
    }
}
