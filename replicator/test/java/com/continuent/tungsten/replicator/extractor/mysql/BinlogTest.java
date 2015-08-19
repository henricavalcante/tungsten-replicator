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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.datasource.AliasDataSource;
import com.continuent.tungsten.replicator.extractor.ExtractorWrapper;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.SingleThreadStageTask;

/**
 * This class defines a BinlogTest. It requires a MySQL server in order to run.
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class BinlogTest extends TestCase
{
    // static Logger logger = Logger.getLogger(BinlogTest.class);
    static Logger logger = null;

    protected void setUp() throws Exception
    {
        super.setUp();
        if (logger == null)
        {
            BasicConfigurator.configure();
            logger = Logger.getLogger(BinlogTest.class);
            logger.info("logger initialized");
        }
    }

    public void test5Binlog() throws Exception
    {
        try
        {
            // Set properties.
            TungstenProperties conf = this.createConfProperties();
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql.binlog_dir",
                    ".");
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT
                    + ".mysql.binlog_file_pattern", "binlog_5events");

            // Configure runtime with these properties and prepare the
            // extractor for use.
            ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                    new MockOpenReplicatorContext(),
                    ReplicatorMonitor.getInstance());
            runtime.configure();
            MySQLExtractor extractor = getMySQLExtractor(runtime);
            extractor.setStrictVersionChecking(false);
            extractor.setDataSource("extractor");
            extractor.prepare(runtime);
            extractor.setLastEventId("000001:0");

            /* read all 5 events from file */
            logger.info("extractor starting");
            for (int i = 0; i < 4; i++)
            {
                extractor.extract();
            }
            logger.info("extractor finished");
        }
        catch (MySQLExtractException e)
        {
            fail(e.getMessage());
            logger.info("extractor failed");
        }
        return;
    }

    public void test_trx() throws Exception
    {
        try
        {
            // Set properties.
            TungstenProperties conf = this.createConfProperties();
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql.binlog_dir",
                    ".");
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT
                    + ".mysql.binlog_file_pattern", "binlog_trx");

            // Configure runtime with these properties and prepare the
            // extractor for use.
            ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                    new MockOpenReplicatorContext(),
                    ReplicatorMonitor.getInstance());
            runtime.configure();
            MySQLExtractor extractor = getMySQLExtractor(runtime);
            extractor.setStrictVersionChecking(false);
            extractor.setDataSource("extractor");
            extractor.prepare(runtime);
            extractor.setLastEventId("000001:0");

            /* read all transactions from file */
            logger.info("extractor starting");
            for (int i = 0; i < 5; i++)
            {
                extractor.extract();
            }
            logger.info("extractor finished");
        }
        catch (MySQLExtractException e)
        {
            logger.error("extractor failed", e);
            fail(e.getMessage());
        }
        return;
    }

    public void testBinlogRBR() throws Exception
    {
        try
        {
            // Set properties.
            TungstenProperties conf = this.createConfProperties();
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql.binlog_dir",
                    ".");
            conf.setString(ReplicatorConf.EXTRACTOR_ROOT
                    + ".mysql.binlog_file_pattern", "binlog_rbr_1");

            // Configure runtime with these properties and prepare the
            // extractor for use.
            ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                    new MockOpenReplicatorContext(),
                    ReplicatorMonitor.getInstance());
            runtime.configure();
            MySQLExtractor extractor = getMySQLExtractor(runtime);
            extractor.setStrictVersionChecking(false);
            extractor.setDataSource("extractor");

            extractor.prepare(runtime);
            extractor.setLastEventId("000001:0");

            /* read all 5 events from file */
            logger.info("extractor starting");
            for (int i = 0; i < 4; i++)
            {
                extractor.extract();
            }
            logger.info("extractor finished");
        }
        catch (MySQLExtractException e)
        {
            fail(e.getMessage());
            logger.info("extractor failed");
        }
        return;
    }

    public void testApplierRBR() throws Exception
    {
        if (true)
            return;

        // Code clean-up : commenting out dead code
        // try
        // {
        // // Set properties.
        // TungstenProperties conf = this.createConfProperties();
        // conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql.binlog_dir",
        // ".");
        // conf.setString(ReplicatorConf.EXTRACTOR_ROOT
        // + ".mysql.binlog_file_pattern", "mysql-bin-row");
        //
        // // Configure runtime with these properties and prepare the
        // // extractor for use.
        // ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
        // new MockOpenReplicatorContext(), ReplicatorMonitor
        // .getInstance());
        // runtime.configure();
        //
        // MySQLExtractor extractor = getMySQLExtractor(runtime);
        // extractor.setStrictVersionChecking(false);
        // extractor.prepare(runtime);
        // extractor.setLastEventId("000003:0");
        //
        // // Extract events. Make sure we get expected number (7).
        // runtime.prepare();
        // Pipeline pipeline = runtime.getPipeline();
        // pipeline.start(new EventDispatcher());
        // Future<ReplDBMSEvent> future = pipeline
        // .watchForAppliedSequenceNumber(6);
        // future.get(3, TimeUnit.SECONDS);
        // /**
        // * Applier applier = runtime.getApplier(); applier.prepare(runtime);
        // * // applier.configure(); // read all 5 events from file
        // * logger.info("RBR extractor/applier starting"); for (int i = 0; i
        // * < 7; i++) { DBMSEvent event = extractor.extract(); if (event !=
        // * null) { applier.apply(event, i, true); } }
        // */
        // logger.info("RBR extractor/applier finished");
        // }
        // catch (MySQLExtractException e)
        // {
        // fail(e.getMessage());
        // logger.info("RBR extractor/applier failed");
        // }
        // return;
    }

    // Generate a simple runtime.
    private TungstenProperties createConfProperties()
            throws ReplicatorException
    {
        TungstenProperties conf = new TungstenProperties();
        conf.setString(ReplicatorConf.SERVICE_NAME, "test");
        conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_MASTER);
        conf.setString(ReplicatorConf.PIPELINES, "master");
        conf.setString(ReplicatorConf.PIPELINE_ROOT + ".master", "extract");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract",
                SingleThreadStageTask.class.toString());
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.extractor",
                "mysql");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.applier", "dummy");

        conf.setString(ReplicatorConf.APPLIER_ROOT + ".dummy",
                DummyApplier.class.getName());
        conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".mysql",
                MySQLExtractor.class.getName());

        conf.setString("replicator.service.datasource",
                "com.continuent.tungsten.replicator.datasource.DataSourceService");
        conf.setString("replicator.datasources", "global,extractor");
        conf.setString("replicator.pipeline.master.services", "datasource");

        conf.setString("replicator.datasource.extractor",
                AliasDataSource.class.getName());
        conf.setString("replicator.datasource.extractor.dataSource", "global");
        conf.setString("replicator.datasource.global",
                "com.continuent.tungsten.replicator.datasource.SqlDataSource");

        conf.setString("replicator.datasource.global.connectionSpec",
                "com.continuent.tungsten.replicator.datasource.SqlConnectionSpecMySQL");
        conf.setString("replicator.datasource.global.connectionSpec.host",
                "dummyHost");
        conf.setString("replicator.datasource.global.connectionSpec.user",
                "dumyUser");
        conf.setString("replicator.datasource.global.connectionSpec.password",
                "dummyPass");

        return conf;
    }

    // Fetch the MySQL extractor from current pipeline.
    private MySQLExtractor getMySQLExtractor(ReplicatorRuntime runtime)
    {
        Pipeline p = runtime.getPipeline();
        ExtractorWrapper wrapper = (ExtractorWrapper) p.getStages().get(0)
                .getExtractor0();
        return (MySQLExtractor) wrapper.getExtractor();
    }
}
