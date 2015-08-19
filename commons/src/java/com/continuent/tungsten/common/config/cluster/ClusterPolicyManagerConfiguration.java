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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges, Edward Archibald
 */

package com.continuent.tungsten.common.config.cluster;

/**
 **/
public class ClusterPolicyManagerConfiguration extends ClusterConfiguration
{
    @SuppressWarnings("unused")
    private static final long serialVersionUID               = 1L;

    /**
     * RMI service name
     */
    private String            serviceName                    = null;
    /**
     * RMI port
     */
    private int               port                           = -1;
    /**
     * RMI host
     */
    private String            host                           = null;

    /**
     * Port on which we can receive notifications via UDP
     */
    private int               notifyPort                     = 10121;
    private String            notifierMonitorClass           = "com.continuent.tungsten.common.patterns.notification.adaptor.ResourceNotifierStub";

    /** Whether to add automatically new Datasources */
    private boolean           autoCreateDataSources          = false;

    /**
     * Frequency at which to check for data sources that didn't receive any
     * update
     */
    int                       silentDScheckFrequencyInSecs   = 0;
    /** Delay after which to disable data sources if they didn't get updated */
    int                       silentDSdisablingTimeoutInSecs = 0;
    /** Delay after which to remove data sources if they didn't get updated */
    int                       silentDSremovalTimeoutInSecs   = 0;

    public ClusterPolicyManagerConfiguration(String clusterName)
            throws ConfigurationException
    {
        super(clusterName);

        // set up the default service values
        setPort(new Integer(ConfigurationConstants.PM_RMI_PORT_DEFAULT));
        setHost("localhost");
        setServiceName(ConfigurationConstants.PM_SERVICE_NAME);

    }

    public ClusterPolicyManagerConfiguration() throws ConfigurationException
    {
        super(null);

        // set up the default service values
        setPort(new Integer(ConfigurationConstants.PM_RMI_PORT_DEFAULT));
        setHost("localhost");
        setServiceName(ConfigurationConstants.PM_SERVICE_NAME);

    }

    /**
     * Load this configuration from the appropriate file.
     * 
     * @throws ConfigurationException
     */
    public ClusterPolicyManagerConfiguration load()
            throws ConfigurationException
    {
        load(ConfigurationConstants.PM_PROPERTIES);

        props.applyProperties(this, true);

        return this;
    }

    /**
     * Returns the notifyPort value.
     * 
     * @return Returns the notifyPort.
     */
    public int getNotifyPort()
    {
        return notifyPort;
    }

    /**
     * Sets the notifyPort value.
     * 
     * @param notifyPort The notifyPort to set.
     */
    public void setNotifyPort(int notifyPort)
    {
        this.notifyPort = notifyPort;
    }

    /**
     * Returns the notifierMonitorClass value.
     * 
     * @return Returns the notifierMonitorClass.
     */
    public String getNotifierMonitorClass()
    {
        return notifierMonitorClass;
    }

    /**
     * Sets the notifierMonitorClass value.
     * 
     * @param notifierMonitorClass The notifierMonitorClass to set.
     */
    public void setNotifierMonitorClass(String notifierMonitorClass)
    {
        this.notifierMonitorClass = notifierMonitorClass;
    }

    public String getServiceName()
    {
        return serviceName;
    }

    public void setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public boolean getAutoCreateDataSources()
    {
        return autoCreateDataSources;
    }

    public void setAutoCreateDataSources(boolean autoCreateDataSources)
    {
        this.autoCreateDataSources = autoCreateDataSources;
    }

    public int getSilentDScheckFrequencyInSecs()
    {
        return silentDScheckFrequencyInSecs;
    }

    public void setSilentDScheckFrequencyInSecs(int silentDScheckFrequencyInSecs)
    {
        this.silentDScheckFrequencyInSecs = silentDScheckFrequencyInSecs;
    }

    public int getSilentDSdisablingTimeoutInSecs()
    {
        return silentDSdisablingTimeoutInSecs;
    }

    public void setSilentDSdisablingTimeoutInSecs(
            int silentDSdisablingTimeoutInSecs)
    {
        this.silentDSdisablingTimeoutInSecs = silentDSdisablingTimeoutInSecs;
    }

    public int getSilentDSremovalTimeoutInSecs()
    {
        return silentDSremovalTimeoutInSecs;
    }

    public void setSilentDSremovalTimeoutInSecs(int silentDSremovalTimeoutInSecs)
    {
        this.silentDSremovalTimeoutInSecs = silentDSremovalTimeoutInSecs;
    }

}
