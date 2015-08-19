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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.conf;

import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.applier.MySQLApplier;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.extractor.mysql.MySQLExtractor;
import com.continuent.tungsten.replicator.filter.DummyFilter;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.SingleThreadStageTask;
import com.continuent.tungsten.replicator.pipeline.Stage;

/**
 * Tests basic configuration functions including reading a replicator properties
 * configuration.
 * 
 * @author <a href="mailto:jussi-pekka.kurikka@continuent.com">Jussi-Pekka
 *         Kurikka</a>
 * @version 1.0
 */
public class TestReplicatorRuntime extends TestCase
{
    /**
     * Prove it is possible to configure a minimal runtime. This helps quite a
     * bit with unit testing.
     */
    public void testNullRuntime() throws Exception
    {
        TungstenProperties conf = new TungstenProperties();
        conf.setString(ReplicatorConf.SERVICE_NAME, "test");
        conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_MASTER);
        conf.setString(ReplicatorConf.PIPELINES, "master");
        conf.setString(ReplicatorConf.PIPELINE_ROOT + ".master", "extract");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract",
                SingleThreadStageTask.class.toString());
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.extractor",
                "dummy");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.applier", "dummy");
        conf.setString(ReplicatorConf.APPLIER_ROOT + ".dummy",
                DummyApplier.class.getName());
        conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".dummy",
                DummyExtractor.class.getName());
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.release();
    }

    /**
     * Prove that it is possible to configure an runtime with all plug-in types.
     */
    public void testFullRuntime() throws Exception
    {
        // Set properties.
        TungstenProperties conf = new TungstenProperties();
        conf.setString(ReplicatorConf.SERVICE_NAME, "test");
        conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_MASTER);
        conf.setString(ReplicatorConf.PIPELINES, "master");
        conf.setString(ReplicatorConf.PIPELINE_ROOT + ".master", "extract");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract",
                SingleThreadStageTask.class.toString());
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.extractor",
                "dummy");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.filters",
                "filter1,filter2");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.applier", "dummy");
        conf.setString(ReplicatorConf.FILTER_ROOT + ".filter1",
                DummyFilter.class.getName());
        conf.setString(ReplicatorConf.FILTER_ROOT + ".filter2",
                DummyFilter.class.getName());
        conf.setString(ReplicatorConf.APPLIER_ROOT + ".dummy",
                DummyApplier.class.getName());
        conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".dummy",
                DummyExtractor.class.getName());

        // Configure runtime.
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline p = runtime.getPipeline();
        Assert.assertNotNull("Stageine not null", p);

        List<Stage> stages = p.getStages();
        Assert.assertEquals("Expect one stage", 1, stages.size());

        Stage stage = stages.get(0);

        Assert.assertNotNull("Extractor not null", stage.getExtractor0());
        Assert.assertNotNull("Pre-filters not null", stage.getFilters0());
        Assert.assertEquals("2 filters set", 2, stage.getFilters0().size());
        Assert.assertNotNull("Applier not null", stage.getApplier0());

        // Release resources
        runtime.release();
    }

    /**
     * Ensure that the MySQLExtractor and MySQLApplier can be configured using
     * standard property names.
     */
    public void testMySQLPlugins() throws Exception
    {
        // Set properties.
        TungstenProperties tp = new TungstenProperties();
        tp.setString(ReplicatorConf.SERVICE_NAME, "test");
        tp.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_MASTER);
        tp.setString(ReplicatorConf.PIPELINES, "master");
        tp.setString(ReplicatorConf.PIPELINE_ROOT + ".master", "extract");
        tp.setString(ReplicatorConf.STAGE_ROOT + ".extract",
                SingleThreadStageTask.class.toString());
        tp.setString(ReplicatorConf.STAGE_ROOT + ".extract.extractor", "mysql");
        tp.setString(ReplicatorConf.STAGE_ROOT + ".extract.applier", "mysql");

        String mysqlExtractor = ReplicatorConf.EXTRACTOR_ROOT + ".mysql";
        tp.setString(ReplicatorConf.EXTRACTOR_ROOT, "mysql");
        tp.setString(mysqlExtractor, MySQLExtractor.class.getName());
        tp.setString(mysqlExtractor + ".binlog_dir", "/var/lib/mysql");
        tp.setString(mysqlExtractor + ".binlog_file_pattern", "mysql-bin");
        tp.setString(mysqlExtractor + ".dataSource", "ds1");

        String mysqlApplier = ReplicatorConf.APPLIER_ROOT + ".mysql";
        tp.setString(ReplicatorConf.APPLIER_ROOT, "mysql");
        tp.setString(mysqlApplier, MySQLApplier.class.getName());
        tp.setString(mysqlApplier + ".dataSource", "global");

        // Configure runtime.
        ReplicatorRuntime runtime = new ReplicatorRuntime(tp,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();

        // Ensure both plugins are present.
        Stage stage = runtime.getPipeline().getStages().get(0);

        Assert.assertNotNull("Extractor not null", stage.getExtractor0());
        Assert.assertNotNull("Applier not null", stage.getApplier0());

        // Release resources.
        runtime.release();
    }
}
