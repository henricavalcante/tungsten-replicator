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
 * Contributor(s): Seppo Jaakola
 */


package com.continuent.tungsten.replicator;

import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnector;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.management.OpenReplicatorManager;
import com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean;

/**
 * This class defines a TestReplicatorManager
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class TestOpenReplicatorManager extends TestCase
{
    OpenReplicatorManager         rmgr     = null;
    JMXConnector                  conn     = null;
    OpenReplicatorManagerMBean    rmb      = null;
    static Logger                 logger   = null;
    static final String           dbUri    = "";              // "jdbc:mysql://localhost/tungsten?user=root&password=rootpass"
    // ;
    static final String    dbDriver                   = "com.mysql.jdbc.Driver";
    static JmxManager      jmxManager                 = null;

    static final String    REPLICATOR_MANAGER_SERVICE = "replicator";

    static final String    applierPlugin              = "com.continuent.tungsten.replicator.applier.DummyApplierPlugin";
    static final String    extractorPlugin            = "com.continuent.tungsten.replicator.extractor.DummyExtractor";
    static final String    thlStoragePlugin           = "com.continuent.tungsten.replicator.thl.DummyTHLStorage";

    public TestOpenReplicatorManager(String name)
    {
        super(name);
    }

    class StateNotificationListener implements NotificationListener
    {
        BlockingQueue<Notification> notifications = new LinkedBlockingQueue<Notification>();

        public void handleNotification(Notification notification,
                Object handback)
        {
            System.err.println("HandleNotification: ");
            try
            {
                notifications.put(notification);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }

        public synchronized Notification waitNotification()
                throws InterruptedException
        {
            return notifications.take();
        }

    }

    StateNotificationListener stateListener = new StateNotificationListener();

    protected void setUp() throws Exception
    {
        super.setUp();
        if (logger == null)
        {
            BasicConfigurator.configure();
            logger = Logger.getLogger(TestOpenReplicatorManager.class);
            int rmiPort = new Integer(System.getProperty(
                    ReplicatorConf.RMI_PORT, "10000")).intValue();
            jmxManager = new JmxManager("localhost", rmiPort,
                    REPLICATOR_MANAGER_SERVICE);
            jmxManager.start();
        }
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    void startReplicatorManager() throws Exception
    {
        rmgr = new OpenReplicatorManager("default");
        conn = JmxManager.getRMIConnector("localhost", 10000,
                REPLICATOR_MANAGER_SERVICE);
        rmb = (OpenReplicatorManagerMBean) JmxManager.getMBeanProxy(conn,
                OpenReplicatorManager.class, false);
        JmxManager.addNotificationListener(conn, OpenReplicatorManager.class,
                stateListener);
        rmgr.start(false);

    }

    void stopReplicatorManager() throws Exception
    {
        JmxManager.removeNotificationListener(conn, OpenReplicatorManager.class,
                stateListener);
        rmgr.stop();
    }

    /*
     * Wait some time until node manager returns requested status.
     */
    void waitStatus(String status) throws Exception
    {

        Notification notification = stateListener.waitNotification();
        StateChangeNotification stateChange = (StateChangeNotification) notification
                .getUserData();
        System.err.println("StateChange: " + stateChange.getPrevState()
                + " -> " + stateChange.getNewState());
        if (stateChange.getNewState().equals(status) == false)
            throw new Exception("Expected state " + status + " reached "
                    + stateChange.getNewState());
        /*
         * int maxCnt = 50; int cnt = 0; while (cnt++ < maxCnt) { Progress
         * progress = rmb.progressPoll(); if
         * (progress.getStatus().equals(status)) break; Thread.sleep(200); }
         * Assert.assertTrue(cnt < maxCnt);
         */
    }

    String waitStatus(StateNotificationListener sl, String status)
            throws Exception
    {

        Notification notification = sl.waitNotification();
        StateChangeNotification stateChange = (StateChangeNotification) notification
                .getUserData();
        System.err.println("StateChange: " + stateChange.getPrevState()
                + " -> " + stateChange.getNewState());
        if (stateChange.getNewState().equals(status) == false)
            throw new Exception("Expected state " + status + " reached "
                    + stateChange.getNewState());
        return stateChange.getNewState();
    }

    String waitStatusNoFail(StateNotificationListener sl, String status)
            throws Exception
    {

        Notification notification = sl.waitNotification();
        StateChangeNotification stateChange = (StateChangeNotification) notification
                .getUserData();
        System.err.println("StateChange: " + stateChange.getPrevState()
                + " -> " + stateChange.getNewState());
        return stateChange.getNewState();
    }

    /**
     * Test that node manager starts and reaches OFFLINE state.
     */
    public void testStartup() throws Exception
    {
        startReplicatorManager();
        try
        {
            waitStatus("OFFLINE");
        }
        catch (Exception e)
        {
            fail(e.getMessage());
        }
        stopReplicatorManager();
    }

    /**
     * Test that node manager survives through all allowed transitions.
     */
    public void testTransitions() throws Exception
    {
        startReplicatorManager();
        try
        {
            TungstenProperties conf = new TungstenProperties();

            conf.setString(ReplicatorConf.APPLIER, applierPlugin);
            conf.setString(ReplicatorConf.EXTRACTOR, extractorPlugin);
            conf.setString(ReplicatorConf.THL_STORAGE, thlStoragePlugin);
            conf.setString(ReplicatorConf.THL_URI, "thl://localhost/");
            conf.setString(ReplicatorConf.MASTER_CONNECT_URI, "thl://localhost/");
            conf.setString(ReplicatorConf.METADATA_SCHEMA, "test");

            // OFFLINE -> SYNCHRONIZING -> OFFLINE
            waitStatus("OFFLINE");

            // Configure node manager here as it reached OFFLINE first time
            rmb.configure(conf.map());

            rmb.online();
            waitStatus("MASTER");
            rmb.offline();
            waitStatus("OFFLINE");

            // OFFLINE -> MASTER -> OFFLINE
            rmb.online();
            waitStatus("MASTER");
            rmb.offline();
            waitStatus("OFFLINE");

            // OFFLINE -> SLAVE -> MASTER -> OFFLINE

            rmb.online();
            waitStatus("MASTER");
            rmb.offline();
            waitStatus("OFFLINE");

        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail(e.getMessage());
        }
        stopReplicatorManager();
    }

    public void testMasterSlave() throws Exception
    {

        logger.info("START TEST: testMasterSlave");
        TungstenProperties node1Conf = new TungstenProperties();
        TungstenProperties node2Conf = new TungstenProperties();

        // Master
        node1Conf.setString(ReplicatorConf.APPLIER, applierPlugin);
        node1Conf.setString(ReplicatorConf.EXTRACTOR, extractorPlugin);
        node1Conf.setString(ReplicatorConf.THL_STORAGE, thlStoragePlugin);

        node1Conf.setString(ReplicatorConf.RMI_PORT, "10001");
        node1Conf.setString(ReplicatorConf.THL_URI, "thl://localhost:2001/");
        node1Conf.setString(ReplicatorConf.MASTER_CONNECT_URI,
                "thl://localhost:2001");
        node1Conf.setString(ReplicatorConf.METADATA_SCHEMA, "node1");

        node2Conf.setString(ReplicatorConf.APPLIER, applierPlugin);
        node2Conf.setString(ReplicatorConf.EXTRACTOR, extractorPlugin);
        node2Conf.setString(ReplicatorConf.THL_STORAGE, thlStoragePlugin);
        node2Conf.setString(ReplicatorConf.THL_URI, "thl://localhost:2002/");
        node2Conf.setString(ReplicatorConf.MASTER_CONNECT_URI,
                "thl://localhost:2001/");
        node2Conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_SLAVE);
        node2Conf.setString(ReplicatorConf.METADATA_SCHEMA, "node2");

        node1Conf.setString(ReplicatorConf.RMI_PORT, "10002");

        OpenReplicatorManager rmgr1 = new OpenReplicatorManager("default");
        StateNotificationListener sl1 = new StateNotificationListener();
        rmgr1.addNotificationListener(sl1, null, null);
        rmgr1.start(false);
        OpenReplicatorManager rmgr2 = new OpenReplicatorManager("default");
        StateNotificationListener sl2 = new StateNotificationListener();
        rmgr2.addNotificationListener(sl2, null, null);
        rmgr2.start(false);

        waitStatus(sl1, "OFFLINE");
        waitStatus(sl2, "OFFLINE");

        rmgr1.configure(node1Conf);
        rmgr2.configure(node2Conf);

        rmgr1.online();
        waitStatus(sl1, "MASTER");

        rmgr2.online();
        waitStatus(sl2, "SLAVE");
        Thread.sleep(1000);
        rmgr2.offline();
        rmgr1.offline();

        rmgr2.removeNotificationListener(sl2);
        rmgr2.stop();
        rmgr1.removeNotificationListener(sl1);
        rmgr1.stop();
    }

    public void testMasterSlaveSwitchOver() throws Exception
    {
        logger.info("START TEST: testMasterSlave");

        TungstenProperties node1Conf = new TungstenProperties();
        TungstenProperties node2Conf = new TungstenProperties();

        // Master
        node1Conf.setString(ReplicatorConf.OOS_POLICY, "Wait");
        node1Conf.setString(ReplicatorConf.APPLIER, applierPlugin);
        node1Conf.setString(ReplicatorConf.EXTRACTOR, extractorPlugin);
        node1Conf.setString(ReplicatorConf.THL_STORAGE, thlStoragePlugin);

        node1Conf.setString(ReplicatorConf.THL_URI, "thl://localhost:2001/");
        node1Conf.setString(ReplicatorConf.MASTER_CONNECT_URI,
                "thl://localhost:2001");
        node1Conf.setString(ReplicatorConf.RMI_PORT, "10001");
        node1Conf.setString(ReplicatorConf.METADATA_SCHEMA, "node1");

        // Slave
        node2Conf.setString(ReplicatorConf.OOS_POLICY, "Wait");
        node2Conf.setString(ReplicatorConf.APPLIER, applierPlugin);
        node2Conf.setString(ReplicatorConf.EXTRACTOR, extractorPlugin);
        node2Conf.setString(ReplicatorConf.THL_STORAGE, thlStoragePlugin);

        node2Conf.setString(ReplicatorConf.THL_URI, "thl://localhost:2002/");
        node2Conf.setString(ReplicatorConf.MASTER_CONNECT_URI,
                "thl://localhost:2001/");
        node2Conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_SLAVE);

        node2Conf.setString(ReplicatorConf.RMI_PORT, "10002");
        node2Conf.setString(ReplicatorConf.METADATA_SCHEMA, "node2");

        OpenReplicatorManager rmgr1 = new OpenReplicatorManager("default");
        StateNotificationListener sl1 = new StateNotificationListener();
        rmgr1.addNotificationListener(sl1, null, null);
        rmgr1.start(false);

        OpenReplicatorManager rmgr2 = new OpenReplicatorManager("default");
        StateNotificationListener sl2 = new StateNotificationListener();
        rmgr2.addNotificationListener(sl2, null, null);
        rmgr2.start(false);

        waitStatus(sl1, "OFFLINE");
        waitStatus(sl2, "OFFLINE");

        rmgr1.configure(node1Conf);
        rmgr2.configure(node2Conf);

        rmgr1.online();
        waitStatus(sl1, "MASTER");

        rmgr2.online();
        waitStatus(sl2, "SLAVE");
        Thread.sleep(500);

        // Switch over

        // Shutdown node1
        rmgr1.offline();
        waitStatus(sl1, "OFFLINE");
        waitStatus(sl2, "SYNCHRONIZING");

        // Reconfigure node2 and set it as master
        node2Conf.setString(ReplicatorConf.MASTER_CONNECT_URI,
                "thl://localhost:2002/");
        node2Conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_MASTER);

        rmgr2.configure(node2Conf);
        rmgr2.online();
        waitStatus(sl2, "MASTER");

        // Reconfigure node1 as slave
        node1Conf.setString(ReplicatorConf.MASTER_CONNECT_URI,
                "thl://localhost:2002/");
        node2Conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_SLAVE);

        rmgr1.configure(node1Conf);
        rmgr1.online();
        waitStatus(sl1, "SLAVE");

        rmgr1.offline();
        rmgr2.offline();

        rmgr1.removeNotificationListener(sl1);
        rmgr1.stop();
        rmgr2.removeNotificationListener(sl2);
        rmgr2.stop();
    }

    class Node
    {
        OpenReplicatorManager         rmgr     = null;
        StateNotificationListener snl          = null;
        TungstenProperties        conf         = new TungstenProperties();
        String                    thlURI       = null;
        String                    currentState = null;

        public Node(String thlURI, String oosPolicy, String ms)
                throws Exception
        {
            this.thlURI = thlURI;

            conf.setString(ReplicatorConf.OOS_POLICY, oosPolicy);
            conf.setString(ReplicatorConf.APPLIER, applierPlugin);
            conf.setString(ReplicatorConf.EXTRACTOR, extractorPlugin);
            conf.setString(ReplicatorConf.THL_STORAGE, thlStoragePlugin);
            conf.setString(ReplicatorConf.THL_URI, thlURI);
            conf.setString(ReplicatorConf.MASTER_CONNECT_URI, thlURI);
            conf.setString(ReplicatorConf.METADATA_SCHEMA, ms);

            rmgr = new OpenReplicatorManager("default");
            rmgr.addNotificationListener(snl = new StateNotificationListener(),
                    null, null);
        }

        public String getThlURI()
        {
            return thlURI;
        }

        public void start() throws Exception
        {
            rmgr.start(false);
            currentState = waitStatus(snl, "OFFLINE");
            rmgr.configure(conf);
        }

        public void setMasterURI(String uri) throws Exception
        {
            if (currentState.equals("SLAVE"))
                currentState = waitStatus(snl, "SYNCHRONIZING");
            conf.setString(ReplicatorConf.MASTER_CONNECT_URI, uri);
            rmgr.configure(conf);
            if (currentState.equals("SLAVE"))
                currentState = waitStatus(snl, "SYNCHRONIZING");
            if (currentState.equals("SYNCHRONIZING"))
                currentState = waitStatus(snl, "SLAVE");
        }

        /* Go online but wait only for synchronizing state */
        public void goOnlineNoWait() throws Exception
        {
            rmgr.online();
            currentState = waitStatus(snl, "SLAVE");
        }

        public void goOnline() throws Exception
        {
            rmgr.online();
            currentState = waitStatus(snl, "SLAVE");
        }

        public void goMaster() throws Exception
        {
            rmgr.online();
            currentState = waitStatus(snl, "MASTER");
        }

        public void shutdown() throws Exception
        {
            rmgr.offline();
            for (int i = 0; i < 5; ++i)
            {
                currentState = waitStatusNoFail(snl, "OFFLINE");
                if (currentState.equals("OFFLINE"))
                    break;
            }
            if (currentState.equals("OFFLINE") == false)
                throw new Exception("Didn't reach offline on few tries");
        }

        public boolean isOnline()
        {
            return currentState.equals("SLAVE")
                    || currentState.equals("SYNCHRONIZING");
        }

        public void waitState(String state) throws Exception
        {
            for (int i = 0; i < 5; ++i)
            {
                currentState = waitStatusNoFail(snl, state);
                if (currentState.equals(state))
                    break;
            }
            if (currentState.equals(state) == false)
                throw new Exception("Didn't reach state: " + state);
        }

        public String getMaxSeqNo() throws Exception
        {
            return rmgr.getMaxSeqNo();
        }

    }

    Node masterElection(Vector<Node> nodes) throws Exception
    {
        Node master = null;
        String maxSeqNo = null;
        for (Node n : nodes)
        {
            if (master == null && n.isOnline())
            {
                master = n;
                maxSeqNo = n.getMaxSeqNo();
            }
            else if (n.isOnline())
            {
                String seqNo = n.getMaxSeqNo();
                if (seqNo.compareTo(maxSeqNo) > 0)
                {
                    master = n;
                    maxSeqNo = seqNo;
                }
            }
        }

        return master;
    }

    Node failOver(Vector<Node> nodes) throws Exception
    {

        int slaves = 0;
        Node master = masterElection(nodes);
        if (master == null)
            return null;

        // Reconfigure master, it should get back in slave state
        master.setMasterURI(master.getThlURI());
        master.goMaster();

        // Reconfigure all other online nodes
        for (Node n : nodes)
        {
            if (n.isOnline())
            {
                n.setMasterURI(master.getThlURI());
                slaves++;
            }
        }
        System.err.println("Slaves available: " + slaves);
        return master;
    }

    public void testMultiSlaveSwitchOver() throws Exception
    {
        Vector<Node> nodes = new Vector<Node>();

        for (int i = 0; i < 8; ++i)
        {
            nodes.add(new Node("thl://localhost:" + (2000 + i), "Wait", "node"
                    + i));
        }

        for (Node n : nodes)
            n.start();
        // First node first master
        Node master = nodes.firstElement();
        for (Node n : nodes)
            n.setMasterURI(master.getThlURI());
        master.goOnline();
        master.goMaster();
        for (Node n : nodes)
        {
            if (n != master)
                n.goOnline();
        }

        do
        {
            Thread.sleep(1000);
            master.shutdown();
            master = failOver(nodes);

        }
        while (master != null);
    }

    public void testMasterRecycling() throws Exception
    {
        System.err.println("testMasterRecycling");
        Node master = new Node("thl://localhost:2001", "Retry", "master");
        Vector<Node> slaves = new Vector<Node>();
        slaves.add(new Node("thl://localhost:2002", "Retry", "slave1"));
        slaves.add(new Node("thl://localhost:2003", "Retry", "slave2"));

        // Set slaves online before master
        for (Node n : slaves)
        {
            n.start();
            n.setMasterURI("thl://localhost:2001");
            n.goOnlineNoWait();
        }

        // Start master
        master.start();
        master.goOnline();
        master.goMaster();

        // Wait until slaves reach SLAVE state
        for (Node n : slaves)
        {
            n.waitState("SLAVE");
        }

        // Shutdown master
        master.shutdown();

        // Wait until slaves reach SYNCHRONIZING state
        for (Node n : slaves)
        {
            n.waitState("SYNCHRONIZING");
        }

        // Restart master
        master.goOnline();
        master.goMaster();

        // Wait until slaves reconnect
        for (Node n : slaves)
        {
            n.waitState("SLAVE");
        }

        // Shutdown all
        master.shutdown();

        for (Node n : slaves)
        {
            n.shutdown();
        }
    }

    public static void main(String[] args)
    {
        System.out.print("Running TestReplicatorManager.main()"
                + System.getProperty("line.separator"));
        org.junit.runner.JUnitCore
                .main("com.continuent.tungsten.replicator.TestReplicatorManager");
    }
}
