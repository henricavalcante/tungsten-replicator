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

package com.continuent.tungsten.replicator.pipeline;

import java.util.Arrays;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;

/**
 * Implements a helper to build pipeline configurations quickly.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class PipelineConfigBuilder
{
    private static Logger      logger = Logger.getLogger(PipelineConfigBuilder.class);
    private TungstenProperties conf   = new TungstenProperties();

    public PipelineConfigBuilder()
    {
    }

    /** Set the role, which selects the pipeline to run. */
    public void setRole(String role)
    {
        conf.setString(ReplicatorConf.ROLE, role);
    }

    /** Sets a generic config property. */
    public void setProperty(String name, String value)
    {
        conf.setString(name, value);
    }

    /** Adds a pipeline with one or more stages and stores. */
    public void addPipeline(String name, String stages, String storeNames)
    {
        conf.setString(ReplicatorConf.PIPELINES, name);
        conf.setString(ReplicatorConf.PIPELINE_ROOT + "." + name, stages);
        if (storeNames != null)
            conf.setString(ReplicatorConf.PIPELINE_ROOT + "." + name
                    + ".stores", storeNames);
    }

    /** Adds a pipeline with one or more stages, stores, and services. */
    public void addPipeline(String name, String stages, String storeNames,
            String serviceNames)
    {
        conf.setString(ReplicatorConf.PIPELINES, name);
        conf.setString(ReplicatorConf.PIPELINE_ROOT + "." + name, stages);
        if (storeNames != null)
            conf.setString(ReplicatorConf.PIPELINE_ROOT + "." + name
                    + ".stores", storeNames);
        if (serviceNames != null)
            conf.setString(ReplicatorConf.PIPELINE_ROOT + "." + name
                    + ".services", serviceNames);
    }

    /** Adds a stage with one or more filters. */
    public void addStage(String name, String extractor, String applier,
            String filters)
    {
        conf.setString(ReplicatorConf.STAGE_ROOT + "." + name,
                SingleThreadStageTask.class.getName());
        conf.setString(ReplicatorConf.STAGE_ROOT + "." + name + ".extractor",
                extractor);
        conf.setString(ReplicatorConf.STAGE_ROOT + "." + name + ".applier",
                applier);
        if (filters != null)
            conf.setString(ReplicatorConf.STAGE_ROOT + "." + name + ".filters",
                    filters);
    }

    /** Adds a component entry. */
    public void addComponent(String type, String name, Class<?> clazz)
            throws Exception
    {
        validateComponentType(type);
        conf.setString("replicator." + type + "." + name, clazz.getName());
    }

    /** Adds a property on a component entry. */
    public void addProperty(String type, String name, String key, String value)
            throws Exception
    {
        validateComponentType(type);
        conf.setString("replicator." + type + "." + name + "." + key, value);
    }

    /** Returns the config file. */
    public TungstenProperties getConfig()
    {
        if (logger.isInfoEnabled())
        {
            StringBuffer sb = new StringBuffer();
            sb.append("{\n");
            Object[] keyArray = conf.keyNames().toArray();
            Arrays.sort(keyArray);
            for (Object key : keyArray)
            {
                sb.append("  ").append(key).append("=")
                        .append(conf.get((String) key)).append("\n");
            }
            sb.append("}\n");
            logger.info("Properties: " + sb.toString());
        }
        return conf;
    }

    // Validates the component type.
    private void validateComponentType(String type) throws Exception
    {
        if (ReplicatorConf.APPLIER.equals(type)
                || ReplicatorConf.EXTRACTOR.equals(type)
                || ReplicatorConf.FILTER.equals(type)
                || ReplicatorConf.STORE.equals(type)
                || ReplicatorConf.STAGE.equals(type)
                || ReplicatorConf.SERVICE.equals(type)
                || ReplicatorConf.DATASOURCE.equals(type))
            return;
        else
            throw new Exception("Unrecognized type: " + type);
    }
}