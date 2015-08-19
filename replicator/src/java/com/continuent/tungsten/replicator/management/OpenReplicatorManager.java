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
 * Contributor(s): Robert Hodges, Teemu Ollakka, Alex Yurchenko, Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.management;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.OpenReplicatorParams;
import com.continuent.tungsten.common.cluster.resource.physical.Replicator;
import com.continuent.tungsten.common.cluster.resource.physical.ReplicatorCapabilities;
import com.continuent.tungsten.common.config.PropertyException;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.jmx.DynamicMBeanHelper;
import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.common.jmx.MethodDesc;
import com.continuent.tungsten.common.jmx.ParamDesc;
import com.continuent.tungsten.fsm.core.Action;
import com.continuent.tungsten.fsm.core.Entity;
import com.continuent.tungsten.fsm.core.EntityAdapter;
import com.continuent.tungsten.fsm.core.Event;
import com.continuent.tungsten.fsm.core.EventTypeGuard;
import com.continuent.tungsten.fsm.core.FiniteStateException;
import com.continuent.tungsten.fsm.core.Guard;
import com.continuent.tungsten.fsm.core.PositiveGuard;
import com.continuent.tungsten.fsm.core.State;
import com.continuent.tungsten.fsm.core.StateChangeListener;
import com.continuent.tungsten.fsm.core.StateMachine;
import com.continuent.tungsten.fsm.core.StateTransitionLatch;
import com.continuent.tungsten.fsm.core.StateTransitionMap;
import com.continuent.tungsten.fsm.core.StateType;
import com.continuent.tungsten.fsm.core.Transition;
import com.continuent.tungsten.fsm.core.TransitionFailureException;
import com.continuent.tungsten.fsm.core.TransitionNotFoundException;
import com.continuent.tungsten.fsm.core.TransitionRollbackException;
import com.continuent.tungsten.fsm.event.EventCompletionListener;
import com.continuent.tungsten.fsm.event.EventDispatcher;
import com.continuent.tungsten.fsm.event.EventDispatcherTask;
import com.continuent.tungsten.fsm.event.EventRequest;
import com.continuent.tungsten.fsm.event.EventStatus;
import com.continuent.tungsten.replicator.ErrorNotification;
import com.continuent.tungsten.replicator.InSequenceNotification;
import com.continuent.tungsten.replicator.OutOfSequenceNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.backup.BackupCompletionNotification;
import com.continuent.tungsten.replicator.backup.BackupException;
import com.continuent.tungsten.replicator.backup.BackupManager;
import com.continuent.tungsten.replicator.backup.RestoreCompletionNotification;
import com.continuent.tungsten.replicator.backup.UnsupportedCapabilityException;
import com.continuent.tungsten.replicator.conf.PropertiesManager;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.consistency.ConsistencyCheckNotification;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.filter.FilterManualProperties;
import com.continuent.tungsten.replicator.management.events.GoOfflineEvent;
import com.continuent.tungsten.replicator.management.events.OfflineNotification;
import com.continuent.tungsten.replicator.management.tungsten.TungstenPlugin;
import com.continuent.tungsten.replicator.plugin.PluginException;
import com.continuent.tungsten.replicator.storage.Store;
import com.continuent.tungsten.replicator.thl.ConnectorHandler;
import com.continuent.tungsten.replicator.thl.ProtocolParams;
import com.continuent.tungsten.replicator.thl.THL;

/**
 * This class provides overall management for the replication and is the
 * starting class for a Tungsten Replicator instance. Replication logic is
 * encapsulated in a replicator plugin.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class OpenReplicatorManager extends NotificationBroadcasterSupport
        implements
            OpenReplicatorManagerMBean,
            OpenReplicatorContext,
            StateChangeListener,
            EventCompletionListener
{
    public static final int         MAJOR                   = 1;
    public static final int         MINOR                   = 0;
    public static final String      SUFFIX                  = "beta5";

    private static final int        ADMIN_THREAD_LIMIT      = 100;

    // Name of this replication service.
    private String                  serviceName;

    // When the service started.
    private long                    startTimeMillis         = System.currentTimeMillis();

    // Configuration is stored in the ReplicatorRuntime.
    private TungstenProperties      properties              = null;
    private PropertiesManager       propertiesManager       = null;

    // Subsystems
    private EventDispatcherTask     eventDispatcher         = null;
    private BackupManager           backupManager           = null;

    // State machine
    private StateTransitionMap      stmap                   = null;
    private StateMachine            sm                      = null;
    private long                    stateChangeTimeMillis   = 0;

    // Thread pool for administrative operations like waiting for a state.
    private ExecutorService         adminThreadPool         = Executors
                                                                    .newFixedThreadPool(ADMIN_THREAD_LIMIT);

    // Pending error, if any.
    private String                  pendingError            = null;
    private String                  pendingErrorCode        = null;
    private String                  pendingExceptionMessage = null;
    private long                    pendingErrorSeqno       = -1;
    private String                  pendingErrorEventId     = null;

    // Auto-enable and auto-recovery settings. We set reasonable
    // defaults just in case there's a bug in configuration as missing
    // values could cause the replicator service to go off the rails.
    private int                     autoRecoveryMaxAttempts = 0;
    private long                    autoRecoveryDelayMillis = 0;
    private long                    autoRecoveryResetMillis = Long.MAX_VALUE;
    private int                     autoRecoveryCount       = 0;
    private AtomicLong              autoRecoveryTotal       = new AtomicLong(0);

    // Monitoring and management
    private static Logger           logger                  = Logger.getLogger(OpenReplicatorManager.class);
    private static Logger           endUserLog              = Logger.getLogger("tungsten.userLog");

    public static final int         REPL                    = 1;
    public static final int         FLUSH                   = 2;

    /** True if the replicator should stop on checksum failure. */
    private boolean                 consistencyFailureStop;

    /** Site name to which replicator belongs. */
    private String                  siteName;

    /** Cluster name to which replicator belongs. */
    private String                  clusterName;

    // Global process information.
    private String                  rmiHost                 = null;
    private int                     rmiPort                 = -1;
    private TimeZone                hostTimeZone;
    private TimeZone                replicatorTimeZone;

    // Open replicator plugin
    private OpenReplicatorPlugin    openReplicator;
    private HashMap<String, Object> mbeans                  = new HashMap<String, Object>();

    private CountDownLatch          doneLatch               = null;

    /**
     * Creates a new <code>ReplicatorManager</code> object
     * 
     * @param serviceName name of the current replication service
     * @throws Exception
     */
    public OpenReplicatorManager(String serviceName) throws Exception
    {
        // Remember our name.
        this.serviceName = serviceName;
        // Define actions
        logger.info("Configuring state machine for replication service: "
                + serviceName);
        Action waitAction = new WaitAction();
        Action flushAction = new FlushAction();
        Action insertHeartbeatAction = new InsertHeartbeatAction();
        Action stopAction = new StopAction();
        Action goOfflineAction = new GoOfflineAction();
        Action goOnlineAfterProvisionAction = new GoOnlineAfterProvisionAction();
        Action deferredOfflineAction = new DeferredOfflineAction();
        Action offlineToSynchronizingAction = new OfflineToSynchronizingAction();
        Action offlineToProvisioningAction = new OfflineToProvisioningAction();
        Action clearDynamicPropertiesAction = new ClearDynamicPropertiesAction();
        Action configureAction = new ConfigureAction();
        Action errorClearAction = new ErrorClearAction();
        Action startToOfflineAction = new StartToOfflineAction();
        Action errorShutdownAction = new ErrorShutdownAction();
        Action errorRecordingAction = new ErrorRecordingAction();
        Action backupAction = new BackupAction();
        Action restoreAction = new RestoreAction();
        Action provisionAction = new ProvisionAction();
        Action setRoleAction = new SetRoleAction();
        Action extendedAction = new ExtendedAction();
        Action leaveOnlineAction = new LeaveOnlineAction();

        // Define replicator states.
        stmap = new StateTransitionMap();
        State start = new State("START", StateType.START);

        // offline states
        State offline = new State("OFFLINE", StateType.ACTIVE);
        State offlineNormal = new State("NORMAL", StateType.ACTIVE, offline);
        State offlineConfiguring = new State("CONFIGURING", StateType.ACTIVE,
                offline);
        State offlineError = new State("ERROR", StateType.ACTIVE, offline,
                errorShutdownAction, errorClearAction);

        // transitional states
        State goingonline = new State("GOING-ONLINE", StateType.ACTIVE);
        State goingonlineSynchronizing = new State("SYNCHRONIZING",
                StateType.ACTIVE, goingonline);

        State goingonlineProvisioning = new State("PROVISIONING",
                StateType.ACTIVE, goingonline);

        State offlineRestoring = new State("RESTORING", StateType.ACTIVE,
                offlineNormal);

        State goingoffline = new State("GOING-OFFLINE", StateType.ACTIVE);

        // online states
        State online = new State("ONLINE", StateType.ACTIVE, null, null,
                leaveOnlineAction);

        State end = new State("END", StateType.END, stopAction, null);

        stmap.addState(start);
        stmap.addState(offline);
        stmap.addState(offlineNormal);
        stmap.addState(offlineConfiguring);
        stmap.addState(offlineError);
        stmap.addState(goingonline);
        stmap.addState(goingonlineSynchronizing);
        stmap.addState(goingonlineProvisioning);
        stmap.addState(offlineRestoring);
        stmap.addState(goingoffline);
        stmap.addState(online);
        stmap.addState(end);

        // Designate error state.
        stmap.setErrorState(offlineError);

        // Define guard conditions for event types.
        Guard startGuard = new EventTypeGuard(StartEvent.class);
        Guard configureGuard = new EventTypeGuard(ConfigureEvent.class);
        Guard configuredGuard = new EventTypeGuard(ConfiguredNotification.class);
        Guard clearDynamicGuard = new EventTypeGuard(
                ClearDynamicPropertiesEvent.class);
        Guard goOnlineGuard = new EventTypeGuard(GoOnlineEvent.class);
        Guard goOnlineProvisionGuard = new EventTypeGuard(
                GoOnlineEventProvisioning.class);

        Guard inSequenceGuard = new EventTypeGuard(InSequenceNotification.class);
        Guard outOfSequenceGuard = new EventTypeGuard(
                OutOfSequenceNotification.class);
        Guard goOfflineGuard = new EventTypeGuard(GoOfflineEvent.class);
        Guard deferredOfflineGuard = new EventTypeGuard(
                DeferredOfflineEvent.class);
        Guard heartbeatGuard = new EventTypeGuard(InsertHeartbeatEvent.class);
        Guard flushGuard = new EventTypeGuard(FlushEvent.class);
        Guard seqnoWaitGuard = new EventTypeGuard(SeqnoWaitEvent.class);
        Guard stopGuard = new EventTypeGuard(StopEvent.class);
        Guard errorGuard = new EventTypeGuard(ErrorNotification.class);
        Guard consistencyFailStopGuard = new ConsistencyFailStopGuard();
        Guard consistencyWarningGuard = new ConsistencyWarningGuard();
        Guard backupGuard = new EventTypeGuard(BackupEvent.class);
        Guard backupCompleteGuard = new EventTypeGuard(
                BackupCompletionNotification.class);
        Guard restoreGuard = new EventTypeGuard(RestoreEvent.class);
        Guard restoreCompleteGuard = new EventTypeGuard(
                RestoreCompletionNotification.class);
        Guard provisionGuard = new EventTypeGuard(ProvisionEvent.class);
        Guard setRoleGuard = new EventTypeGuard(SetRoleEvent.class);
        Guard extendedActionGuard = new ExtendedActionEventGuard();

        // START state can transition to OFFLINE and END.
        stmap.addTransition(new Transition("START-TO-OFFLINE", startGuard,
                start, startToOfflineAction, offlineNormal));
        stmap.addTransition(new Transition("START-STOP", stopGuard, start,
                null, end));

        // OFFLINE state has 2 substate rmgr.go();s.
        // 1. NORMAL -- Normal non-active state.
        // 2. ERROR -- An error has occurred.
        // All offline states can transition to offline error and process
        // extended commands.
        stmap.addTransition(new Transition("OFFLINE-ERROR", errorGuard,
                offline, errorRecordingAction, offlineError));
        stmap.addTransition(new Transition("OFFLINE-EXTENDED",
                extendedActionGuard, offline, extendedAction, offline));
        stmap.addTransition(new Transition("OFFLINE-STOP", stopGuard, offline,
                null, end));

        // OFFLINE:NORMAL can transition to any of the following states.
        stmap.addTransition(new Transition("OFFLINE-OFFLINE-1", goOfflineGuard,
                offlineNormal, null, offlineNormal));
        stmap.addTransition(new Transition("OFFLINE-OFFLINE-2",
                deferredOfflineGuard, offlineNormal, null, offlineNormal));
        stmap.addTransition(new Transition("OFFLINE-CONFIGURE-1",
                configureGuard, offlineNormal, configureAction,
                offlineConfiguring));
        stmap.addTransition(new Transition("OFFLINE-CLEAR-DYNAMIC-1",
                clearDynamicGuard, offlineNormal, clearDynamicPropertiesAction,
                offlineNormal));
        stmap.addTransition(new Transition("OFFLINE-GO-ONLINE-1",
                goOnlineGuard, offlineNormal, offlineToSynchronizingAction,
                goingonlineSynchronizing));

        stmap.addTransition(new Transition("OFFLINE-GO-ONLINE-2",
                goOnlineProvisionGuard, offlineNormal,
                offlineToProvisioningAction, goingonlineProvisioning));

        stmap.addTransition(new Transition("OFFLINE-BACKUP-1", backupGuard,
                offlineNormal, backupAction, offlineNormal));
        stmap.addTransition(new Transition("OFFLINE-RESTORE", restoreGuard,
                offlineNormal, restoreAction, offlineRestoring));
        stmap.addTransition(new Transition("OFFLINE-PROVISION", provisionGuard,
                offlineNormal, provisionAction, offlineRestoring));
        stmap.addTransition(new Transition("OFFLINE-SETROLE", setRoleGuard,
                offlineNormal, setRoleAction, offlineNormal));

        // OFFLINE:CONFIGURING can transition back to following states.
        stmap.addTransition(new Transition("CONFIGURE-OFFLINE",
                configuredGuard, offlineConfiguring, null, offlineNormal));

        // OFFLINE:ERROR can transition to any of the following states.
        stmap.addTransition(new Transition("ERROR-NORMAL-1", goOfflineGuard,
                offlineError, null, offlineNormal));
        stmap.addTransition(new Transition("ERROR-NORMAL-1",
                deferredOfflineGuard, offlineError, null, offlineNormal));
        stmap.addTransition(new Transition("ERROR-CONFIGURE", configureGuard,
                offlineError, configureAction, offlineNormal));
        stmap.addTransition(new Transition("ERROR-CLEAR-DYNAMIC-2",
                clearDynamicGuard, offlineError, clearDynamicPropertiesAction,
                offlineNormal));
        stmap.addTransition(new Transition("ERROR-GO-ONLINE", goOnlineGuard,
                offlineError, offlineToSynchronizingAction,
                goingonlineSynchronizing));
        stmap.addTransition(new Transition("ERROR-BACKUP", backupGuard,
                offlineError, backupAction, offlineError));
        stmap.addTransition(new Transition("ERROR-RESTORE", restoreGuard,
                offlineNormal, restoreAction, offlineRestoring));

        // OFFLINE:BACKUP can transition to the following state(s).
        stmap.addTransition(new Transition("BACKUP-OFFLINE",
                backupCompleteGuard, offline, null, offlineNormal));

        // RESTORE can transition to the following state(s).
        stmap.addTransition(new Transition("RESTORE-OFFLINE",
                restoreCompleteGuard, offlineRestoring, null, offlineNormal));

        // GOING-ONLINE can transition to the following states.
        stmap.addTransition(new Transition("SYNCHRONIZING-ERROR", errorGuard,
                goingonline, errorRecordingAction, offlineError));
        stmap.addTransition(new Transition("GOING-ONLINE-EXTENDED",
                extendedActionGuard, goingonline, extendedAction, goingonline));

        // SYNCHRONIZING state can transition to ONLINE and OFFLINE
        stmap.addTransition(new Transition("SYNCHRONIZING-SHUTDOWN",
                goOfflineGuard, goingonlineSynchronizing, goOfflineAction,
                goingoffline));
        stmap.addTransition(new Transition("ONLINE-SHUTDOWN",
                deferredOfflineGuard, goingonlineSynchronizing,
                deferredOfflineAction, goingonlineSynchronizing));
        stmap.addTransition(new Transition("SYNCHRONIZING-ONLINE",
                inSequenceGuard, goingonlineSynchronizing, null, online));

        stmap.addTransition(new Transition("PROVISIONING-ONLINE",
                inSequenceGuard, goingonlineProvisioning,
                goOnlineAfterProvisionAction, online));

        // ONLINE state transitions
        stmap.addTransition(new Transition("ONLINE-SHUTDOWN", goOfflineGuard,
                online, goOfflineAction, goingoffline));
        stmap.addTransition(new Transition("ONLINE-SHUTDOWN",
                deferredOfflineGuard, online, deferredOfflineAction, online));

        stmap.addTransition(new Transition("ONLINE-OUTOFSEQUENCE",
                outOfSequenceGuard, online, null, goingonlineSynchronizing));
        stmap.addTransition(new Transition("CONSISTENCY-ERROR",
                consistencyFailStopGuard, online, errorRecordingAction,
                offlineError));
        stmap.addTransition(new Transition("CONSISTENCY-WARNING",
                consistencyWarningGuard, online, null, online));
        stmap.addTransition(new Transition("ONLINE-ERROR", errorGuard, online,
                errorRecordingAction, offlineError));
        stmap.addTransition(new Transition("SEQNO-WAIT", seqnoWaitGuard,
                online, waitAction, online));
        stmap.addTransition(new Transition("HOT-SLAVE-BACKUP", backupGuard,
                online, backupAction, online));
        stmap.addTransition(new Transition("HOT-SLAVE-BACKUP-COMPLETE",
                backupCompleteGuard, online, null, online));
        stmap.addTransition(new Transition("FLUSH", flushGuard, online,
                flushAction, online));
        stmap.addTransition(new Transition("HEARTBEAT", heartbeatGuard, online,
                insertHeartbeatAction, online));
        stmap.addTransition(new Transition("ONLINE-EXTENDED",
                extendedActionGuard, online, extendedAction, online));

        // GOING-OFFLINE state can transition to OFFLINE.
        stmap.addTransition(new Transition("GOING-OFFLINE-OFFLINE",
                new PositiveGuard(), goingoffline, null, offlineNormal));
        stmap.addTransition(new Transition("GOING-OFFLINE-EXTENDED",
                extendedActionGuard, goingoffline, extendedAction, goingoffline));

        stmap.build();
        sm = new StateMachine(stmap, new EntityAdapter(this));
        sm.addListener(this);

        // Start the event dispatcher.
        eventDispatcher = new EventDispatcherTask(sm);
        eventDispatcher.setListener(this);
        eventDispatcher.start(serviceName + "-dispatcher");

        // Start the property manager.
        ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf
                .getConfiguration(serviceName);
        propertiesManager = new PropertiesManager(
                runtimeConf.getReplicatorProperties(),
                runtimeConf.getReplicatorDynamicProperties(),
                runtimeConf.getReplicatorDynamicRole());
        propertiesManager.loadProperties();

        // Clear properties if that is desired.
        if (runtimeConf.getClearDynamicProperties())
            propertiesManager.clearDynamicProperties();
    }

    public void advertiseInternal()
    {
        JmxManager.registerMBean(this, OpenReplicatorManager.class,
                serviceName, true);
    }

    protected boolean isConsistencyFailureStop()
    {
        return consistencyFailureStop;
    }

    /**
     * Log state changes coming from the state machine. {@inheritDoc}
     */
    public void stateChanged(Entity entity, State oldState, State newState)
    {
        Notification notification = new Notification("ReplicatorStateChange",
                this, 0);
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("oldState", oldState.getName());
        map.put("newState", newState.getName());
        notification.setUserData(map);
        sendNotification(notification);
        stateChangeTimeMillis = System.currentTimeMillis();
        logger.info("Sent State Change Notification " + oldState.getName()
                + " -> " + newState.getName());
        endUserLog.info("State changed " + oldState.getName() + " -> "
                + newState.getName());
    }

    /**
     * Log events as they are processed in the replicator state machine.
     */
    public Object onCompletion(Event event, EventStatus status)
            throws InterruptedException
    {
        // Log according to status of the event.
        Object annotation = null;
        if (status.isSuccessful())
        {
            if (logger.isDebugEnabled())
                logger.debug("Applied event: "
                        + event.getClass().getSimpleName());
        }
        else if (status.isCancelled())
        {
            logger.warn("Event processing was cancelled: "
                    + event.getClass().getSimpleName());
        }
        else if (status.getException() != null)
        {
            Throwable t = status.getException();

            if (t instanceof TransitionNotFoundException)
            {
                // This is just a warning. We received an event that is
                // inappropriate for the current state.
                TransitionNotFoundException e = (TransitionNotFoundException) t;
                StringBuffer msg = new StringBuffer();
                msg.append("Received irrelevant event for current state: state=");
                msg.append(e.getState().getName());
                msg.append(" event=");
                msg.append(e.getEvent().getClass().getSimpleName());
                logger.warn(msg.toString());
                endUserLog.warn(msg.toString());
                annotation = new ReplicatorStateException(
                        "Operation irrelevant in current state");
            }
            else if (t instanceof TransitionRollbackException)
            {
                // A transition could not complete and rolled back to the
                // original state.
                TransitionRollbackException e = (TransitionRollbackException) t;
                StringBuffer msg = new StringBuffer();
                msg.append("State transition could not complete and was rolled back: state=");
                msg.append(e.getTransition().getInput().getName());
                msg.append(" transition=");
                msg.append(e.getTransition().getName());
                msg.append(" event=");
                msg.append(e.getEvent().getClass().getSimpleName());
                String errMsg = msg.toString();
                endUserLog.error(errMsg);
                displayErrorMessages(e);
                annotation = getStateMachineException(e, errMsg);
            }
            else if (t instanceof TransitionFailureException)
            {
                // A transition failed, causing the replicator to go into the
                // OFFLINE:ERROR state.
                TransitionFailureException e = (TransitionFailureException) t;
                StringBuffer msg = new StringBuffer();
                msg.append("State transition failed causing emergency recovery: state=");
                msg.append(e.getTransition().getInput().getName());
                msg.append(" transition=");
                msg.append(e.getTransition().getName());
                msg.append(" event=");
                msg.append(e.getEvent().getClass().getSimpleName());
                String errMsg = msg.toString();
                endUserLog.error(errMsg);
                displayErrorMessages(e);
                annotation = getStateMachineException(e, errMsg);
            }
            else if (t instanceof FiniteStateException)
            {
                // Should not exit here, this event may be result of
                // user operation
                logger.error("Unexpected state transition processing error", t);
                annotation = new ReplicatorException(
                        "Operation failed unexpectedly--see log for details", t);
            }
            else
            {
                // We probably have some sort of bug. We need to wrap it in a
                // replicator exception.
                logger.error("Unexpected processing error", t);
                annotation = new ReplicatorException(
                        "Operation failed unexpectedly--see log for details", t);
            }
        }

        // Return the annotation we have attached to the event.
        return annotation;
    }

    /**
     * getStateMachineException
     * 
     * @param e
     * @param msg
     */
    private ReplicatorStateException getStateMachineException(
            FiniteStateException e, String msg)
    {
        ReplicatorStateException replicatorStateException = new ReplicatorStateException(
                msg, e);
        if (e.getCause() != null && e.getCause() instanceof ReplicatorException)
        {
            ReplicatorException exc = (ReplicatorException) e.getCause();
            replicatorStateException.setOriginalErrorMessage(exc
                    .getOriginalErrorMessage());
            replicatorStateException.setExtraData(exc.getExtraData());
        }
        return replicatorStateException;
    }

    /**
     * Signals that the replicator should start.
     */
    class StartEvent extends Event
    {
        public StartEvent()
        {
            super(null);
        }
    }

    /**
     * Signals that the replicator should reconfigure properties.
     */
    class ConfigureEvent extends Event
    {
        /** If props are null, re-read replicator properties. */
        public ConfigureEvent(TungstenProperties props)
        {
            super(props);
        }
    }

    /**
     * Event to set the replicator role.
     */
    class SetRoleEvent extends Event
    {
        /**
         * Properties contain name value pairs to set.
         */
        public SetRoleEvent(TungstenProperties props)
        {
            super(props);
        }
    }

    /**
     * Signals that replicator should set one or more dynamic properties.
     */
    class SetDynamicPropertiesEvent extends Event
    {
        /**
         * Properties contain name value pairs to set.
         */
        public SetDynamicPropertiesEvent(TungstenProperties props)
        {
            super(props);
        }
    }

    /**
     * Signals that replicator should clear dynamic properties.
     */
    class ClearDynamicPropertiesEvent extends Event
    {
        public ClearDynamicPropertiesEvent()
        {
            super(null);
        }
    }

    /**
     * Signals that the replicator should exit.
     */
    class StopEvent extends Event
    {
        public StopEvent()
        {
            super(null);
        }
    }

    /**
     * This class defines a FlushEvent, which processes a request to synchronize
     * the database with the replicator.
     */
    class FlushEvent extends Event
    {
        String             eventId;
        private final long timeout;

        public FlushEvent(long timeout)
        {
            super(null);
            this.timeout = timeout;
        }

        public void setEventFuture(String event)
        {
            eventId = event;
        }

        public long getTimeout()
        {
            return timeout;
        }
    }

    /**
     * This class defines a SeqnoAppliedEvent, which processes a request to wait
     * for the slave to wait for a particular sequence number to be processed.
     */
    class SeqnoWaitEvent extends Event
    {
        private final String     seqno;
        private final long       waitType;
        public static final long RECEIVED = 1;
        public static final long APPLIED  = 2;

        public SeqnoWaitEvent(String seqno, long waitType)
        {
            super(null);
            this.seqno = seqno;
            this.waitType = waitType;
        }

        public String getSeqno()
        {
            return seqno;
        }

        public long getWaitType()
        {
            return waitType;
        }
    }

    /**
     * This class defines a BackupEvent, which contains a request to run a
     * particular backup type.
     */
    class BackupEvent extends Event
    {
        private volatile Future<String> future;
        private final String            backupAgentName;
        private final String            storageAgentName;

        public BackupEvent(String backupAgentName, String storageAgentName)
        {
            super(null);
            this.backupAgentName = backupAgentName;
            this.storageAgentName = storageAgentName;
        }

        public Future<String> getFuture()
        {
            return future;
        }

        public void setFuture(Future<String> future)
        {
            this.future = future;
        }

        public String getBackupAgentName()
        {
            return backupAgentName;
        }

        public String getStorageAgentName()
        {
            return storageAgentName;
        }
    }

    /**
     * This class defines a restore event, which contains a request to run a
     * restore from backup.
     */
    class RestoreEvent extends Event
    {
        private volatile Future<String> future;
        private final String            uri;

        public RestoreEvent(String uri)
        {
            super(null);
            this.uri = uri;
        }

        public Future<String> getFuture()
        {
            return future;
        }

        public void setFuture(Future<String> future)
        {
            this.future = future;
        }

        public String getUri()
        {
            return uri;
        }
    }

    /**
     * This class defines a provision event, which contains a request to run a
     * provision operation from another database.
     */
    class ProvisionEvent extends Event
    {
        private boolean      result;
        private final String uri;

        public ProvisionEvent(String uri)
        {
            super(null);
            this.uri = uri;
        }

        public boolean getResult()
        {
            return result;
        }

        public void setResult(boolean result)
        {
            this.result = result;
        }

        public String getUri()
        {
            return uri;
        }
    }

    /**
     * Represents a request to create a heartbeat event with an associated
     * description.
     */
    class InsertHeartbeatEvent extends Event
    {
        private TungstenProperties params;

        public InsertHeartbeatEvent(TungstenProperties params)
        {
            super(null);
            this.params = params;
        }

        public TungstenProperties getParams()
        {
            return params;
        }
    }

    /**
     * Signals that the replicator should move to the online state.
     */
    class GoOnlineEvent extends Event
    {
        private TungstenProperties params;

        public GoOnlineEvent(TungstenProperties params)
        {
            super(null);
            this.params = params;
        }

        public TungstenProperties getParams()
        {
            return params;
        }
    }

    /**
     * Signals that the replicator should move to the provision state.
     */
    class GoOnlineEventProvisioning extends Event
    {
        private TungstenProperties params;

        public GoOnlineEventProvisioning(TungstenProperties params)
        {
            super(null);
            this.params = params;
        }

        public TungstenProperties getParams()
        {
            return params;
        }
    }

    /**
     * Request to send replicator offline at a later time.
     */
    class DeferredOfflineEvent extends Event
    {
        private TungstenProperties params;

        public DeferredOfflineEvent(TungstenProperties params)
        {
            super(null);
            this.params = params;
        }

        public TungstenProperties getParams()
        {
            return params;
        }
    }

    class ConfiguredNotification extends Event
    {
        public ConfiguredNotification()
        {
            super(null);
        }
    }

    /**
     * Guard for consistencyChecks.
     */
    class ConsistencyFailStopGuard implements Guard
    {
        public boolean accept(Event event, Entity entity, State state)
        {
            if (!(event instanceof ConsistencyCheckNotification))
            {
                return false;
            }
            if (isConsistencyFailureStop())
            {
                String msg = ((ConsistencyException) event.getData())
                        .getMessage();
                logger.error("ConsistencyTable check violation detected:" + msg);
                return true;
            }
            else
                return false;
        }
    };

    /**
     * Guard for consistency check failure. This guard succeeds if we have a
     * warning.
     */
    class ConsistencyWarningGuard implements Guard
    {
        public boolean accept(Event event, Entity entity, State state)
        {
            if (!(event instanceof ConsistencyCheckNotification))
            {
                return false;
            }
            if (isConsistencyFailureStop())
                return false;
            else
            {
                String msg = ((ConsistencyException) event.getData())
                        .getMessage();
                logger.warn("ConsistencyTable check violation detected:" + msg);
                return true;
            }
        }
    };

    /**
     * Guard for an extended event. We accept the event if the current state is
     * a match with the extended event pattern.
     */
    class ExtendedActionEventGuard implements Guard
    {
        public boolean accept(Event event, Entity entity, State state)
        {
            if (!(event instanceof ExtendedActionEvent))
            {
                return false;
            }
            ExtendedActionEvent extendedEvent = (ExtendedActionEvent) event;
            Matcher m = extendedEvent.getStatePattern()
                    .matcher(state.getName());
            return m.matches();
        }
    };

    /**
     * Action to process an error. This is used by a normal transition triggered
     * by receipt of an ErrorNotification. It extracts and stores the error
     * message.
     */
    class ErrorRecordingAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws InterruptedException
        {
            // Log the error condition.
            ErrorNotification en = (ErrorNotification) event;

            displayErrorMessages(en);
            String message = "Received error notification, shutting down services :\n"
                    + en.getUserMessage();
            if (en.getThrowable() instanceof ReplicatorException
                    && ((ReplicatorException) en.getThrowable()).getExtraData() != null)
            {
                message += "\n"
                        + ((ReplicatorException) en.getThrowable())
                                .getExtraData();
            }
            logger.error(message, en.getThrowable());

            // Store the user error message.
            pendingError = en.getUserMessage();
            pendingExceptionMessage = en.getThrowable().getMessage();
            if (en.getThrowable() instanceof ReplicatorException)
            {
                ReplicatorException exc = (ReplicatorException) en
                        .getThrowable();
                if (exc.getExtraData() != null)
                    pendingExceptionMessage += "\n" + exc.getExtraData();
            }
            pendingErrorSeqno = en.getSeqno();
            pendingErrorEventId = en.getEventId();

            // Now see if we want to try to go online again, which we do by
            // automatically posting a GoOnlineEvent. Such auto-recovery
            // makes sense only under specific conditions, so we first weed
            // out non-qualifying situations.
            if (autoRecoveryMaxAttempts > 0)
            {
                // Auto-recovery is enabled. Find our previous state, which
                // will be available if we are executing in a transition.
                String oldState = sm.getState().getBaseName();

                if (!"ONLINE".equals(oldState)
                        && !"SYNCHRONIZING".equals(oldState))
                {
                    // Auto-recovery only makes sense if we were previously
                    // online.
                    logger.info("No auto-recovery as previous state not ONLINE or SYNCHRONIZING: previous state="
                            + oldState);
                }
                else if (autoRecoveryCount >= autoRecoveryMaxAttempts)
                {
                    // After the max retries are exceeded we just log an error.
                    logger.info("No auto-recovery as max retry attempts have been exceeded");
                }
                else
                {
                    // We have not exhausted the number of retries, so
                    // enqueue an online request with an optional delay.
                    TungstenProperties params = new TungstenProperties();
                    params.setBoolean(OpenReplicatorParams.AUTO_RECOVERY, true);
                    if (autoRecoveryDelayMillis > 0)
                    {
                        params.setLong(
                                OpenReplicatorParams.ONLINE_DELAY_MILLIS,
                                autoRecoveryDelayMillis);
                    }
                    GoOnlineEvent goOnlineEvent = new GoOnlineEvent(params);
                    try
                    {
                        eventDispatcher.put(goOnlineEvent);
                    }
                    catch (InterruptedException e)
                    {
                        // Not much we can do here in the unlikely event of an
                        // interruption. Throwing an exception does not help,
                        // because we are already deep in error handling and
                        // should just continue.
                        logger.warn("Ignored interrupted exception while enqueuing auto-recovery online event");
                    }

                    // Explain what we are doing and update total number of
                    // times we have done auto recovery since starting.
                    logger.info("Scheduled auto-recovery as retry attempts are not exceeded: currrent count="
                            + autoRecoveryCount);
                    autoRecoveryTotal.incrementAndGet();
                }
            }
        }
    }

    private void displayErrorMessages(ErrorNotification event)
    {
        endUserLog.error(event.getUserMessage());
        Throwable error = event.getThrowable();
        if (error instanceof ReplicatorException
                && ((ReplicatorException) error).getExtraData() != null)
        {
            for (String line : ((ReplicatorException) error).getExtraData()
                    .split("\n"))
            {
                endUserLog.error(line);
            }
        }
    }

    // Echo error messages to the user log.
    private void displayErrorMessages(Exception exception)
    {
        String message = exception.getMessage();
        endUserLog.error(String.format("[%s] message: %s", exception.getClass()
                .getSimpleName(), message));

        Throwable error = exception.getCause();
        boolean stop = false;
        while (error != null)
        {
            if (!(error instanceof ReplicatorException))
            {
                // Break after the second non tungsten ReplicatorException in a
                // row is found
                if (stop)
                    break;

                stop = true;
            }
            else
                stop = false;

            String message2 = error.getMessage();
            endUserLog.error(String.format("[%s] message: %s", error.getClass()
                    .getSimpleName(), message2));
            error = error.getCause();
        }
    }

    /**
     * Action to shut down services following an error so that we can restart
     * cleanly.
     */
    class ErrorShutdownAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType)
        {
            // Close down services as cleanly as possible.
            logger.warn("Performing emergency service shutdown");
            try
            {
                if (openReplicator != null)
                    openReplicator.offline(new TungstenProperties());
            }
            catch (Throwable e)
            {
                logger.error(
                        "Service shutdown failed...Services may be active", e);
            }
            logger.info("All internal services are shut down; replicator ready for recovery");
        }
    }

    /* Action to clear pending error message. */
    class ErrorClearAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType)
        {
            pendingError = null;
            pendingExceptionMessage = null;
            pendingErrorSeqno = -1;
            pendingErrorEventId = null;
        }
    };

    /*
     * Action in transition from START to OFFLINE state.
     */
    class StartToOfflineAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException,
                TransitionFailureException
        {
            // Load properties file.
            loadProperties(event, entity, transition, actionType);

            // Clear backup subsystem.
            if (backupManager != null)
            {
                backupManager.release();
                backupManager = null;
            }

            // Run configuration.
            try
            {
                doConfigure();
            }
            catch (ReplicatorException e)
            {
                pendingError = "Replicator configuration failed";
                pendingExceptionMessage = e.getMessage();
                if (logger.isDebugEnabled())
                    logger.debug(pendingError, e);
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
            catch (Exception e)
            {
                pendingError = "Replicator configuration failed";
                pendingExceptionMessage = e.getMessage();
                if (logger.isDebugEnabled())
                    logger.debug(pendingError, e);
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    }

    /*
     * Action to configure properties by either rereading them or setting all
     * properties from outside.
     */
    class ConfigureAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException
        {
            TungstenProperties newProps = (TungstenProperties) ((ConfigureEvent) event)
                    .getData();
            // Load properties file.
            if (newProps == null)
                loadProperties(event, entity, transition, actionType);
            else
                properties = newProps;

            // Clear backup subsystem.
            if (backupManager != null)
            {
                backupManager.release();
                backupManager = null;
            }

            /* apply new configuration */
            try
            {
                doConfigure();
            }
            catch (ReplicatorException e)
            {
                logger.error("configuration failed for: " + e, e);
            }

            /*
             * signal directly that configuration is over. 
             */
            try
            {
                eventDispatcher.put(new ConfiguredNotification());
            }
            catch (InterruptedException e)
            {
                // TODO Log this?
            }
        }
    };

    /**
     * Action to set the replicator role.
     */
    class SetRoleAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException
        {
            TungstenProperties props = (TungstenProperties) ((SetRoleEvent) event)
                    .getData();
            try
            {
                // Tell the plugin and make sure it can set the role.
                String role = props.getProperty(ReplicatorConf.ROLE);
                String uri = props
                        .getProperty(ReplicatorConf.MASTER_CONNECT_URI);
                openReplicator.setRole(role, uri);

                // Record new properties in properties file.
                propertiesManager.setDynamicProperties(props);
                properties = propertiesManager.getProperties();
                doConfigure();
            }
            catch (ReplicatorException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Unable to set role", e);
                throw new TransitionRollbackException("Unable to set role",
                        event, entity, transition, actionType, e);
            }
        }
    };

    /**
     * Action to handle an extended action event, which is basically an enclosed
     * action.
     */
    class ExtendedAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException,
                TransitionFailureException, InterruptedException
        {
            // This is a pass-through to the enclosed action.
            ExtendedActionEvent extendedEvent = (ExtendedActionEvent) event;
            Action action = extendedEvent.getExtendedAction();
            action.doAction(event, entity, transition, actionType);
        }
    };

    /**
     * Action to clear dynamic properties.
     */
    class ClearDynamicPropertiesAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException
        {
            try
            {
                propertiesManager.clearDynamicProperties();
            }
            catch (ReplicatorException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Unable to clear dynamic properties", e);
                throw new TransitionRollbackException(
                        "Failed to clear dynamic properties: " + e.getMessage(),
                        event, entity, transition, actionType, e);
            }
            properties = propertiesManager.getProperties();
            try
            {
                doConfigure();
            }
            catch (ReplicatorException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug(
                            "Unable to re-configure after clearing dynamic properties",
                            e);
                throw new TransitionRollbackException(
                        "Unable to re-configure after clearing dynamic properties: "
                                + e.getMessage(), event, entity, transition,
                        actionType, e);
            }
        }
    };

    /*
     * Action in transition from OFFLINE to SYNCHRONIZING state.
     */
    class OfflineToSynchronizingAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException,
                TransitionFailureException, InterruptedException
        {
            try
            {
                TungstenProperties params;
                if (event instanceof GoOnlineEvent)
                {
                    GoOnlineEvent goOnlineEvent = (GoOnlineEvent) event;
                    params = goOnlineEvent.getParams();
                }
                else
                    params = new TungstenProperties();

                // If this is an auto-recovery operation, increment the retry
                // count.
                if (params.getBoolean(OpenReplicatorParams.AUTO_RECOVERY))
                {
                    autoRecoveryCount++;
                    if (logger.isDebugEnabled())
                        logger.debug("Processing online due to auto-recovery: count="
                                + autoRecoveryCount);
                }
                else if (autoRecoveryCount > 0)
                {
                    logger.info("Clearing auto-recovery count: count="
                            + autoRecoveryCount);
                    autoRecoveryCount = 0;
                }

                // If we are being asked to delay, do so now.
                if (params.get(OpenReplicatorParams.ONLINE_DELAY_MILLIS) != null)
                {
                    long onlineDelayMillis = params
                            .getInt(OpenReplicatorParams.ONLINE_DELAY_MILLIS);
                    if (onlineDelayMillis > 0)
                    {
                        logger.info("Delaying online operation on request: delay="
                                + ((double) onlineDelayMillis / 1000) + "s");
                        Thread.sleep(onlineDelayMillis);
                    }
                }

                // Issue the online operation.
                openReplicator.online(params);
            }
            catch (ReplicatorException e)
            {
                // Pending error is correctly set.
                pendingError = "Replicator unable to go online due to error";
                pendingExceptionMessage = e.getMessage();
                if (logger.isDebugEnabled())
                    logger.debug(pendingError, e);
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
            catch (InterruptedException e)
            {
                throw e;
            }
            catch (Throwable e)
            {
                pendingError = "Replicator service start-up failed due to underlying error";
                pendingExceptionMessage = e.toString();
                logger.error(String.format("%s, reason=%s", pendingError, e));
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    };

    /*
     * Action in transition from OFFLINE to PROVISIONING state.
     */
    class OfflineToProvisioningAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException,
                TransitionFailureException
        {
            try
            {
                TungstenProperties params;
                if (event instanceof GoOnlineEventProvisioning)
                {
                    GoOnlineEventProvisioning goOnlineEvent = (GoOnlineEventProvisioning) event;
                    params = goOnlineEvent.getParams();
                }
                else
                    params = new TungstenProperties();

                openReplicator.online(params);
            }
            catch (ReplicatorException e)
            {
                // Pending error is correctly set.
                pendingError = "Replicator unable to go online due to error";
                pendingExceptionMessage = e.getMessage();
                if (logger.isDebugEnabled())
                    logger.debug(pendingError, e);
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
            catch (Throwable e)
            {
                pendingError = "Replicator service start-up failed due to underlying error";
                pendingExceptionMessage = e.toString();
                logger.error(String.format("%s, reason=%s", pendingError, e));
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    };

    /*
     * Action for transition from any state to OFFLINE state.
     */
    class GoOfflineAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionFailureException
        {
            try
            {
                GoOfflineEvent goOfflineEvent = (GoOfflineEvent) event;
                openReplicator.offline(goOfflineEvent.getParams());
            }
            catch (Throwable e)
            {
                pendingError = "Replicator service shutdown failed due to underlying error";
                pendingExceptionMessage = e.toString();
                logger.error(pendingError, e);
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    }

    /*
     * Action for transition from PROVISIONING to ONLINE state.
     */
    class GoOnlineAfterProvisionAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionFailureException
        {
            try
            {
                // Force the pipeline to flush all pending events into THL
                String waitedEvent = String.valueOf(openReplicator
                        .getReplicatorRuntime().getPipeline()
                        .getLastExtractedSeqno());

                long timeout = 0;
                openReplicator.waitForAppliedEvent(waitedEvent, timeout);

                openReplicator.offline(new TungstenProperties());
                openReplicator.online(new TungstenProperties());
            }
            catch (Throwable e)
            {
                pendingError = "Replicator service shutdown failed due to underlying error";
                pendingExceptionMessage = e.toString();
                logger.error(pendingError, e);
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    }

    /*
     * Action for handling deferred offline action.
     */
    class DeferredOfflineAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionFailureException
        {
            try
            {
                DeferredOfflineEvent deferredOfflineEvent = (DeferredOfflineEvent) event;
                openReplicator
                        .offlineDeferred(deferredOfflineEvent.getParams());
            }
            catch (Throwable e)
            {
                pendingError = "Deferred offline request failed due to underlying error";
                pendingExceptionMessage = e.toString();
                logger.error(pendingError, e);
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    }

    /*
     * Action to run when leaving online state.
     */
    class LeaveOnlineAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException,
                TransitionFailureException
        {
            // See if we want to reset the auto-recovery count.
            if (autoRecoveryMaxAttempts > 0 && autoRecoveryCount > 0)
            {
                // If we have been online longer than the recover interval,
                // we can reset the count.
                double timeInStateSecs = getTimeInStateSeconds();
                double resetTimeSecs = ((double) autoRecoveryResetMillis) / 1000.0;

                if (timeInStateSecs > resetTimeSecs)
                {
                    logger.info("Resetting auto-recovery count: count="
                            + autoRecoveryCount + " autoRecoveryResetInterval="
                            + resetTimeSecs + "s timeInStateSecs="
                            + timeInStateSecs + "s");
                    autoRecoveryCount = 0;
                }
            }
        }
    };

    /*
     * Action to stop the replicator normally.
     */
    class StopAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionFailureException
        {
            try
            {
                JmxManager.unregisterMBean(OpenReplicatorManager.class,
                        serviceName);
            }
            catch (Throwable e)
            {
                pendingError = "Replicator service shutdown failed due to underlying error";
                pendingExceptionMessage = e.toString();
                logger.error(pendingError, e);
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    };

    /*
     * Action to start a backup.
     */
    class BackupAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionFailureException,
                TransitionRollbackException
        {
            // Ensure backups are initialized.
            initializeBackupSubsystem(event, entity, transition, actionType);

            // Spawn the backup.
            try
            {
                BackupEvent backupEvent = (BackupEvent) event;
                String backupAgentName = backupEvent.getBackupAgentName();
                String storageAgentName = backupEvent.getStorageAgentName();
                String inputState = transition.getInput().getName();

                Future<String> task = backupManager.spawnBackup(
                        backupAgentName, storageAgentName,
                        inputState.startsWith("ONLINE"));
                backupEvent.setFuture(task);
            }
            catch (UnsupportedCapabilityException e)
            {
                throw new TransitionRollbackException(
                        "Unsupported backup operation: " + e.getMessage(),
                        event, entity, transition, actionType, e);
            }
            catch (Exception e)
            {
                pendingError = "Unable to spawn backup request";
                pendingExceptionMessage = e.getMessage();
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    };

    /*
     * Action to start a restore.
     */
    class RestoreAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionFailureException,
                TransitionRollbackException
        {
            // Ensure backups are initialized.
            initializeBackupSubsystem(event, entity, transition, actionType);

            // Spawn the restore.
            try
            {
                RestoreEvent restoreEvent = (RestoreEvent) event;
                String uri = restoreEvent.getUri();
                Future<String> task = backupManager.spawnRestore(uri);
                restoreEvent.setFuture(task);
            }
            catch (Exception e)
            {
                pendingError = "Unable to spawn restore request";
                pendingExceptionMessage = e.getMessage();
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    };

    /*
     * Action to start a provisioning operation.
     */
    class ProvisionAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionFailureException,
                TransitionRollbackException
        {
            try
            {
                ProvisionEvent provisionEvent = (ProvisionEvent) event;
                String uri = provisionEvent.getUri();
                openReplicator.provision(uri);
                provisionEvent.setResult(true);
            }
            catch (Exception e)
            {
                pendingError = "Unable to spawn restore request";
                pendingExceptionMessage = e.getMessage();
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    };

    /*
     * Action to insert a heartbeat event.
     */
    class InsertHeartbeatAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionFailureException,
                TransitionRollbackException
        {
            try
            {
                InsertHeartbeatEvent hbEvent = (InsertHeartbeatEvent) event;
                TungstenProperties params = hbEvent.getParams();

                if (!doHeartbeat(params))
                {
                    throw new TransitionRollbackException(
                            "Heartbeat not supported for this source type",
                            event, entity, transition, actionType, null);
                }
            }
            catch (Exception e)
            {
                pendingError = "Unable to process heartbeat request";
                pendingExceptionMessage = e.getMessage();
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    };

    /*
     * Action to trigger a flush of the master. This may roll back if we are not
     * in the master role.
     */
    class FlushAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionFailureException,
                TransitionRollbackException
        {
            // Flush is only permitted for masters.
            if (ReplicatorConf.ROLE_MASTER.equals(getRole()))
            {
                try
                {
                    // Ask the plugin to perform a flush operation.
                    FlushEvent flushEvent = (FlushEvent) event;
                    long timeout = flushEvent.getTimeout();
                    String future = openReplicator.flush(timeout);
                    flushEvent.setEventFuture(future);
                }
                catch (Exception e)
                {
                    pendingError = "Unable to process flush request";
                    pendingExceptionMessage = e.getMessage();
                    throw new TransitionFailureException(pendingError, event,
                            entity, transition, actionType, e);
                }
            }
            else
            {
                throw new TransitionRollbackException(
                        "Flush operation is only allowed when in master role",
                        event, entity, transition, actionType, null);
            }

        }
    };

    /*
     * Action to wait for a particular sequence number.
     */
    class WaitAction implements Action
    {
        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionFailureException
        {
            try
            {
                SeqnoWaitEvent waitEvent = (SeqnoWaitEvent) event;
                String waitedEvent = "seqno: " + waitEvent.getSeqno();
                long timeout = 0;

                openReplicator.waitForAppliedEvent(waitedEvent, timeout);
            }
            catch (Exception e)
            {
                pendingError = "Unable to process wait request";
                pendingExceptionMessage = e.getMessage();
                throw new TransitionFailureException(pendingError, event,
                        entity, transition, actionType, e);
            }
        }
    };

    /*
     * OPENREPLICATORCONTEXT API STARTS HERE.
     */

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorContext#getEventDispatcher()
     */
    public EventDispatcher getEventDispatcher()
    {
        return this.eventDispatcher;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorContext#registerMBean(Object,
     *      Class, String)
     */
    public void registerMBean(Object mbean, Class<?> mbeanClass, String name)
    {
        JmxManager.registerMBean(mbean, mbeanClass);
        mbeans.put(name, mbean);
    }

    /*
     * MBEAN ATTRIBUTE API STARTS HERE
     */
    @MethodDesc(description = "Confirm service liveness", usage = "isAlive")
    public boolean isAlive()
    {
        return true;
    }

    @MethodDesc(description = "Gets replicator version", usage = "getVersion")
    public String getVersion()
    {
        return "No version available";
    }

    @MethodDesc(description = "Gets the site name for this replicator", usage = "getSiteName")
    public String getSiteName()
    {
        return siteName;
    }

    @MethodDesc(description = "Gets the cluster name for this replicator", usage = "getClusterName")
    public String getClusterName()
    {
        return clusterName;
    }

    @MethodDesc(description = "Gets the simplified service name for this replicator", usage = "getSimpleServiceName")
    public String getSimpleServiceName()
    {
        if (serviceName != null && clusterName != null && siteName != null)
        {
            String prefix = String.format("%s_%s_", siteName, clusterName);
            int indexPrefix = serviceName.indexOf(prefix);
            if (indexPrefix > -1)
                return serviceName.substring(serviceName.indexOf(prefix)
                        + prefix.length());
            else
                return serviceName;
        }

        return null;

    }

    @MethodDesc(description = "Gets the service name for this replicator", usage = "getServiceName")
    public String getServiceName()
    {
        return serviceName;
    }

    @MethodDesc(description = "Gets the replicator source ID", usage = "getSourceId")
    public String getSourceId()
    {
        // cannot use runtime.getSourceId() here since we are not sure it is
        // online
        return properties.getString(ReplicatorConf.SOURCE_ID);
    }

    @MethodDesc(description = "Gets the master connect URI, if any", usage = "getMasterConnectUri")
    public String getMasterConnectUri()
    {
        return properties.getString(ReplicatorConf.MASTER_CONNECT_URI);
    }

    @MethodDesc(description = "Gets the master listen URI, if any", usage = "getMasterListenUri")
    public String getMasterListenUri()
    {
        return properties.getString(ReplicatorConf.MASTER_LISTEN_URI);
    }

    @MethodDesc(description = "Indicates if the replicator uses an SSL connection", usage = "useSSLConnection")
    public Boolean getUseSSLConnection() throws URISyntaxException
    {
        URI uri = null;
        Boolean useSSL = false;
        // Uses the Master Listen URI to determine if a connection is using SSL
        // or not
        if (this.getMasterListenUri() != null)
        {
            uri = new URI(this.getMasterListenUri());
            String scheme = uri.getScheme();
            useSSL = THL.SSL_URI_SCHEME.equals(scheme) ? true : false;
        }
        return useSSL;
    }

    @MethodDesc(description = "Returns clients (slaves) of this server", usage = "getClients")
    public List<Map<String, String>> getClients() throws Exception
    {
        if (openReplicator instanceof TungstenPlugin)
        {
            TungstenPlugin tungsten = (TungstenPlugin) openReplicator;

            if (tungsten.getReplicatorRuntime() == null)
                throw new Exception("No runtime found. Is Replicator ONLINE?");

            // Drill down through the pipeline and collect clients of configured
            // THL store(s).
            List<Map<String, String>> clients = new ArrayList<Map<String, String>>();
            for (Store store : tungsten.getReplicatorRuntime().getStores())
            {
                if (store instanceof THL)
                {
                    THL thl = (THL) store;
                    for (ConnectorHandler handler : thl.getClients())
                    {
                        Map<String, String> client = new HashMap<String, String>();
                        client.put(ProtocolParams.RMI_HOST,
                                handler.getRmiHost());
                        client.put(ProtocolParams.RMI_PORT,
                                handler.getRmiPort());
                        clients.add(client);
                    }
                }
            }
            return clients;
        }
        else
            throw new Exception(
                    "Unable to retrieve clients, because Replicator is not a TungstenPlugin instance");
    }

    @MethodDesc(description = "Gets the replicator's current role.", usage = "getRole")
    public String getRole()
    {
        return properties.getString(ReplicatorConf.ROLE);
    }

    @MethodDesc(description = "Gets the uptime of this replicator", usage = "getUptimeSeconds")
    public double getUptimeSeconds()
    {
        return (System.currentTimeMillis() - this.startTimeMillis) / 1000.0;
    }

    @MethodDesc(description = "Get the current state of the replicator", usage = "getState")
    public String getState()
    {
        return this.sm.getState().getName();
    }

    @MethodDesc(description = "Get the prospective state of the replicator if there is a transition in progress", usage = "getPendingTransition")
    public String transitioningTo()
    {
        State pendingState = this.sm.getPendingState();
        if (pendingState == null)
            return "";
        else
            return pendingState.getName();
    }

    @MethodDesc(description = "Gets the time replicator has been in current state", usage = "getTimeInStateSeconds")
    public double getTimeInStateSeconds()
    {
        return (System.currentTimeMillis() - stateChangeTimeMillis) / 1000.0;
    }

    @MethodDesc(description = "Gets the time of last replicator state change", usage = "getStateChangeTimeMillis")
    public long getStateChangeTimeMillis()
    {
        return stateChangeTimeMillis;
    }

    @MethodDesc(description = "Gets the pending error code, if any", usage = "getPendingErrorCode")
    public String getPendingErrorCode()
    {
        return pendingErrorCode;
    }

    @MethodDesc(description = "Gets the pending error, if any", usage = "getPendingError")
    public String getPendingError()
    {
        return pendingError;
    }

    @MethodDesc(description = "Gets the pending error exception message, if any", usage = "getPendingExceptionMessage")
    public String getPendingExceptionMessage()
    {
        return pendingExceptionMessage;
    }

    @MethodDesc(description = "Gets the maximum sequence number, if available", usage = "getMaxSeqNo")
    public String getMaxSeqNo() throws Exception
    {
        return status().get(Replicator.APPLIED_LAST_SEQNO);
    }

    @MethodDesc(description = "Gets the minimum and maxmimum sequence number, if available", usage = "getMinMaxSeqNo")
    public String[] getMinMaxSeqNo() throws Exception
    {
        String[] pair = {getMaxSeqNo(), getMinSeqNo()};
        return pair;
    }

    @MethodDesc(description = "Gets the minimum sequence number, if available", usage = "getMinSeqNo")
    public String getMinSeqNo() throws Exception
    {
        return "-1";
    }

    /*
     * MBEAN MANAGEMENT API STARTS HERE
     */

    /**
     * Re-read configuration properties.
     */
    @MethodDesc(description = "Re-read configuration properties", usage = "configure <map of properties>")
    public void configure(
            @ParamDesc(name = "props", description = "Optional map of properties to replace replicator.properties") Map<String, String> props)
            throws Exception
    {
        try
        {
            TungstenProperties tp;
            if (props == null)
                tp = null;
            else
            {
                tp = new TungstenProperties(props);
                logger.info("Updating properties from remote client");
                if (logger.isDebugEnabled())
                {
                    logger.debug("New properties: " + props.toString());
                }
            }
            configure(tp);
        }
        catch (Exception e)
        {
            logger.error("Configure operation failed", e);
            throw new Exception("Configure operation failed: " + e.getMessage());
        }
    }

    /**
     * Clear dynamic properties. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#clearDynamicProperties()
     */
    @MethodDesc(description = "Clears the current dynamically-set properties.", usage = "clearDynamicProperties")
    public void clearDynamicProperties() throws Exception
    {
        try
        {
            handleEventSynchronous(new ClearDynamicPropertiesEvent());
        }
        catch (Exception e)
        {
            logger.error("Clear dynamic properties failed", e);
            throw new Exception("Clear dynamic properties failed: "
                    + e.toString());
        }
    }

    /**
     * Return a copy of current properties. {@inheritDoc}
     */
    @MethodDesc(description = "Gets the current properties.", usage = "properties [<key>]")
    public Map<String, String> properties(
            @ParamDesc(name = "key", description = "optional key of a single property") String key)
            throws Exception
    {
        Map<String, String> returnProps = propertiesManager.getProperties()
                .map();

        // First, anonymize any 'password' type properties
        for (String currentKey : returnProps.keySet())
        {
            if (currentKey.toLowerCase().contains("password"))
                returnProps.put(currentKey, "**********");
        }

        if (key != null)
        {
            Map<String, String> matchingItems = new HashMap<String, String>();

            for (String currentKey : returnProps.keySet())
            {
                if (currentKey.contains(key))
                    matchingItems.put(currentKey, returnProps.get(currentKey));
            }
            return matchingItems;
        }

        return returnProps;
    }

    /**
     * Return a copy of current dynamic properties. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#getDynamicProperties()
     */
    @MethodDesc(description = "Gets the current dynamically-set properties.", usage = "getDynamicProperties")
    public Map<String, String> getDynamicProperties() throws Exception
    {
        try
        {
            return propertiesManager.getDynamicProperties().map();
        }
        catch (ReplicatorException e)
        {
            logger.error("Failure while accessing dynamic properties", e);
            throw new Exception(
                    "Unable to access dynamic properties; see log for details");
        }
    }

    /**
     * Sets the replicator role. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#setRole(java.lang.String,
     *      java.lang.String)
     */
    @MethodDesc(description = "Sets the role of the replicator.", usage = "setRole {master | slave | standby} <uri>")
    public void setRole(
            @ParamDesc(name = "role", description = "The role that the replicator is to take, either 'master', 'slave', or 'standby'") String role,
            @ParamDesc(name = "uri", description = "Master connection URI (required for master)") String uri)
            throws Exception
    {
        try
        {
            TungstenProperties tp = new TungstenProperties();
            if (role == null)
            {
                throw new ReplicatorException(
                        "Role name is required to for a set role operation");
            }
            tp.setString(ReplicatorConf.ROLE, role);
            if (uri != null)
                tp.setString(ReplicatorConf.MASTER_CONNECT_URI, uri);
            handleEventSynchronous(new SetRoleEvent(tp));
        }
        catch (ReplicatorException e)
        {
            String message = "set role operation failed";

            logger.error(message, e);
            if (e.getOriginalErrorMessage() != null)
            {
                message += " (" + e.getOriginalErrorMessage() + ")";
            }
            else
                message += " (" + e.getMessage() + ")";
            throw new Exception(message);
        }
    }

    /**
     * Gets status variables from replicator. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#status()
     */
    @MethodDesc(description = "Gets the replicator's detailed status information.", usage = "status")
    public Map<String, String> status() throws Exception
    {
        try
        {
            /*
             * IMPORTANT: If you add a property to the status properties, be
             * sure to add the key property in the Replicator resource class in
             * commons.
             */

            // Get status from plugin
            HashMap<String, String> pluginStatus = openReplicator.status();

            // Convert old plugin values so we don't mess up existing script
            // plug-ins.
            convertOldValue(pluginStatus, Replicator.APPLIED_LAST_SEQNO,
                    OpenReplicatorPlugin.STATUS_LAST_APPLIED);
            convertOldValue(pluginStatus, Replicator.MAX_STORED_SEQNO,
                    OpenReplicatorPlugin.STATUS_LAST_RECEIVED);

            // Following are standard variables over and above values provided
            // by plugin.
            pluginStatus.put(Replicator.SITENAME, siteName);
            pluginStatus.put(Replicator.CLUSTERNAME, clusterName);
            pluginStatus.put(Replicator.SERVICE_NAME, serviceName);
            pluginStatus.put(Replicator.SIMPLE_SERVICE_NAME,
                    getSimpleServiceName());
            pluginStatus.put(Replicator.MASTER_CONNECT_URI,
                    getMasterConnectUri());
            pluginStatus
                    .put(Replicator.MASTER_LISTEN_URI, getMasterListenUri());
            pluginStatus.put(Replicator.USE_SSL_CONNECTION, this
                    .getUseSSLConnection().toString());
            pluginStatus.put(Replicator.SOURCEID, getSourceId());
            pluginStatus.put(Replicator.CLUSTERNAME, clusterName);
            pluginStatus.put(Replicator.ROLE, getRole());
            pluginStatus.put(Replicator.HOST, getSourceId());

            pluginStatus.put(Replicator.DATASERVER_HOST, properties
                    .getString(ReplicatorConf.RESOURCE_DATASERVER_HOST));
            pluginStatus.put(Replicator.UPTIME_SECONDS,
                    Double.toString(getUptimeSeconds()));
            pluginStatus.put(Replicator.TIME_IN_STATE_SECONDS,
                    Double.toString(getTimeInStateSeconds()));
            pluginStatus.put(Replicator.STATE, getState());
            pluginStatus.put(Replicator.TRANSITIONING_TO, transitioningTo());
            pluginStatus.put(Replicator.RMI_PORT,
                    Integer.toString(getRmiPort()));
            pluginStatus.put(Replicator.PENDING_EXCEPTION_MESSAGE,
                    (getPendingExceptionMessage() == null
                            ? "NONE"
                            : getPendingExceptionMessage()));
            pluginStatus.put(Replicator.PENDING_ERROR_CODE,
                    (getPendingErrorCode() == null
                            ? "NONE"
                            : getPendingErrorCode()));
            pluginStatus.put(Replicator.PENDING_ERROR,
                    (getPendingError() == null ? "NONE" : getPendingError()));
            pluginStatus.put(Replicator.PENDING_ERROR_SEQNO,
                    Long.toString(pendingErrorSeqno));
            pluginStatus
                    .put(Replicator.PENDING_ERROR_EVENTID,
                            (pendingErrorEventId == null
                                    ? "NONE"
                                    : pendingErrorEventId));
            pluginStatus.put(Replicator.RESOURCE_PRECEDENCE, properties
                    .getString(ReplicatorConf.RESOURCE_PRECEDENCE,
                            ReplicatorConf.RESOURCE_PRECEDENCE_DEFAULT, true));
            pluginStatus.put(Replicator.CURRENT_TIME_MILLIS,
                    Long.toString(System.currentTimeMillis()));
            pluginStatus.put(Replicator.AUTO_RECOVERY_TOTAL,
                    autoRecoveryTotal.toString());
            pluginStatus.put(Replicator.AUTO_RECOVERY_ENABLED, new Boolean(
                    autoRecoveryMaxAttempts > 0).toString());

            if (logger.isDebugEnabled())
                logger.debug("plugin status: " + pluginStatus.toString());

            // Return the finalized status values.
            return pluginStatus;
        }
        catch (Exception e)
        {
            logger.error("Status operation failed", e);
            throw new Exception("Status operation failed: " + e.getMessage());
        }
    }

    // Convert property from old to new name by removing old and re-inserting
    // with new name.
    private void convertOldValue(Map<String, String> pluginStatus,
            String newName, String oldName)
    {
        String value = pluginStatus.get(oldName);
        if (value != null)
        {
            pluginStatus.remove(oldName);
            pluginStatus.put(newName, value);
        }
    }

    /**
     * Returns detailed status in a single call. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#getStatus()
     */
    public TungstenProperties getStatus() throws Exception
    {
        return new TungstenProperties(status());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#statusList(java.lang.String)
     */
    @MethodDesc(description = "Provides a list of individual components", usage = "statusList <name>")
    public List<Map<String, String>> statusList(
            @ParamDesc(name = "name", description = "Name of the status list") String name)
            throws Exception
    {
        return openReplicator.statusList(name);
    }

    /**
     * Start Replicator Node Manager JMX service.
     */
    @MethodDesc(description = "Starts the replicator service", usage = "start")
    public void start(boolean forceOffline) throws Exception
    {
        try
        {
            handleEventSynchronous(new StartEvent());
            if (sm.getState().getName().equals("OFFLINE:NORMAL"))
            {
                // Runtime does not exist yet so we need to check properties
                // directly.
                boolean autoEnabled = new Boolean(
                        properties.getBoolean(ReplicatorConf.AUTO_ENABLE));
                if (!forceOffline && autoEnabled)
                {
                    logger.info("Replicator auto-enabling is engaged; going online automatically");
                    online();
                }
            }
        }
        catch (Exception e)
        {
            logger.error("Start operation failed", e);
            throw new Exception("Start operation failed: " + e.getMessage());
        }

        this.doneLatch = new CountDownLatch(1);
    }

    /**
     * Stop Replicator Node Manager JMX service.
     */
    @MethodDesc(description = "Stops the replicator service", usage = "stop")
    public void stop() throws Exception
    {
        try
        {
            // We make this synchronous so that the client can see operational
            // errors.
            handleEventSynchronous(new StopEvent());

            if (doneLatch != null)
            {
                doneLatch.countDown();
            }
        }
        catch (Exception e)
        {
            logger.error("Stop operation failed", e);
            throw new Exception(e.toString());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#online()
     */
    @MethodDesc(description = "Transitions the replicator into the online state.", usage = "online")
    public void online() throws Exception
    {
        online2(new HashMap<String, String>());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#online()
     */
    @MethodDesc(description = "Transitions the replicator into the online state.", usage = "online2")
    public void online2(
            @ParamDesc(name = "controlParams", description = "Control parameters for online operation") Map<String, String> controlParams)
            throws Exception
    {
        TungstenProperties params = new TungstenProperties(controlParams);
        GoOnlineEvent goOnlineEvent = new GoOnlineEvent(params);

        try
        {
            handleEventSynchronous(goOnlineEvent);
        }
        catch (ReplicatorException e)
        {
            String message = "Online operation failed";

            logger.error(message, e);
            if (e.getOriginalErrorMessage() != null)
            {
                message += " (" + e.getOriginalErrorMessage() + ")";
            }
            else
                message += " (" + e.getMessage() + ")";
            throw new Exception(message);
            // throw new Exception("Online operation failed", e);
        }
    }

    /**
     * Sends the replicator into the offline state. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#offline()
     */
    @MethodDesc(description = "Transitions the replicator into the offline state.", usage = "offline")
    public void offline() throws Exception
    {
        TungstenProperties params = new TungstenProperties();
        GoOfflineEvent goOfflineEvent = new GoOfflineEvent(params);

        try
        {
            handleEventSynchronous(goOfflineEvent);
        }
        catch (Exception e)
        {
            logger.error("Offline operation failed", e);
            throw new Exception("Offline operation failed: " + e.toString());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#online()
     */
    @MethodDesc(description = "Requests replicator to go offline", usage = "offlineDeferred")
    public void offlineDeferred(
            @ParamDesc(name = "controlParams", description = "Control parameters for offline operation") Map<String, String> controlParams)
            throws Exception
    {
        TungstenProperties params = new TungstenProperties(controlParams);
        DeferredOfflineEvent deferredOfflineEvent = new DeferredOfflineEvent(
                params);

        try
        {
            handleEventSynchronous(deferredOfflineEvent);
        }
        catch (Exception e)
        {
            logger.error("Online operation failed", e);
            throw new Exception("Online operation failed: " + e.toString());
        }
    }

    /**
     * Creates a flush event, which in turn causes us to wait for the database
     * to synchronize with THL. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#flush(long)
     */
    @MethodDesc(description = "Synchronizes the replicator log with the database as of the returned sequence number", usage = "flush")
    public String flush(
            @ParamDesc(name = "timeout", description = "Seconds to wait before timingout (0=infinity") long timeout)
            throws Exception
    {
        try
        {
            // First insert a heartbeat to ensure there is something in the
            // that we can wait for.
            Map<String, String> caps = capabilities();
            ReplicatorCapabilities capabilities = new ReplicatorCapabilities(
                    new TungstenProperties(caps));
            if (capabilities.isHeartbeat())
            {
                HashMap<String, String> params = new HashMap<String, String>();
                params.put(OpenReplicatorParams.HEARTBEAT_NAME, "FLUSH");
                heartbeat(params);
            }

            // Enqueue a flush event.
            FlushEvent flushEvent = new FlushEvent(timeout);
            handleEventSynchronous(flushEvent);

            return flushEvent.eventId;
        }
        catch (Exception e)
        {
            logger.error("Flush operation failed", e);
            throw new Exception("Flush operation failed: " + e.getMessage());
        }
    }

    /**
     * Invoke purge on underlying replicator plugin. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#purge(java.util.Map)
     */
    @MethodDesc(description = "Kill non-replication connections", usage = "purge")
    public int purge(
            @ParamDesc(name = "controlParams", description = "Control parameters for purge operation") Map<String, String> controlParams)
            throws Exception
    {
        TungstenProperties params = new TungstenProperties(controlParams);

        try
        {
            logger.info("Received connection purge request");
            return openReplicator.purge(params);
        }
        catch (Exception e)
        {
            logger.error("Purge request failed", e);
            throw new Exception("Purge request failed: " + e.getMessage());
        }
    }

    /**
     * Inserts a heartbeat event. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#heartbeat(Map)
     */
    @MethodDesc(description = "Waits for replicator to achieve a particular state.", usage = "waitForState <stateName> <timeToWait>")
    public void heartbeat(
            @ParamDesc(name = "controlParams", description = "Control parameters for heartbeat operation") Map<String, String> controlParams)
            throws Exception
    {
        try
        {
            TungstenProperties params = new TungstenProperties(controlParams);

            // TUC-228: Ensure that heartbeat name is ASCII-only.
            String name = params.getString(OpenReplicatorParams.HEARTBEAT_NAME,
                    "NONE", true);
            CharsetEncoder encoder = Charset.forName("US-ASCII").newEncoder();
            for (int i = 0; i < name.length(); i++)
            {
                if (!encoder.canEncode(name.charAt(i)))
                {
                    throw new Exception(
                            "Heartbeat name may only use ASCII characters: "
                                    + name);
                }
            }

            // Submit the heartbeat.
            InsertHeartbeatEvent event = new InsertHeartbeatEvent(params);
            handleEventSynchronous(event);
        }
        catch (Exception e)
        {
            logger.error("Heartbeat operation failed", e);
            throw new Exception("Heartbeat operation failed: " + e.getMessage());
        }
    }

    /**
     * Waits for replicator to achieve a particular state. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#waitForState(java.lang.String,
     *      long)
     */
    @MethodDesc(description = "Waits for replicator to achieve a particular state.", usage = "waitForState <stateName> <timeToWait>")
    public boolean waitForState(
            @ParamDesc(name = "stateName", description = "Name of the state to wait for") String stateName,
            @ParamDesc(name = "timeout", description = "The number of milliseconds to wait") long timeout)
            throws Exception
    {
        // Check arguments.
        State desiredState = stmap.getStateByName(stateName);
        if (desiredState == null)
        {
            throw new Exception("Unknown state name: " + stateName);
        }
        if (timeout == 0)
            timeout = 1800;
        else if (timeout < 0 || timeout > 1800)
            throw new Exception("Limit must be between 0 and 1800 seconds: "
                    + timeout);

        if (logger.isDebugEnabled())
            logger.debug("Waiting for state: state=" + desiredState
                    + " seconds=" + timeout);

        StateTransitionLatch latch = sm.createStateTransitionLatch(
                desiredState, true);
        State finalState = null;
        Future<State> result = adminThreadPool.submit(latch);
        try
        {
            finalState = result.get(timeout, TimeUnit.SECONDS);
        }
        catch (TimeoutException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("Timed out waiting for state: " + stateName);
            return false;
        }

        if (latch.isExpected() || desiredState.equals(finalState))
        {
            if (logger.isDebugEnabled())
                logger.debug("Wait operation concluded successfully; found expected state: "
                        + stateName);
            return true;
        }
        else if (latch.isError())
        {
            String message = "Replicator failed and is in error state: "
                    + finalState.getName();
            if (logger.isDebugEnabled())
                logger.debug(message);
            throw new Exception(message);
        }
        else
        {
            String message = "Replication reached unexpected state: "
                    + finalState.getName();
            if (logger.isDebugEnabled())
                logger.debug(message);
            throw new Exception(message);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#waitForAppliedSequenceNumber(java.lang.String,
     *      long)
     */
    @MethodDesc(description = "Waits for a sequence number to be applied", usage = "waitForAppliedSequenceNumber <seqno> <timeout>")
    public boolean waitForAppliedSequenceNumber(
            @ParamDesc(name = "seqno", description = "Sequence number to wait for") String seqno,
            @ParamDesc(name = "timeout", description = "Seconds to wait before timing out (0=infinity") long timeout)
            throws Exception
    {
        try
        {
            boolean success = openReplicator
                    .waitForAppliedEvent(seqno, timeout);
            return success;
        }
        catch (Exception e)
        {
            logger.error("Wait operation failed", e);
            throw new Exception("Wait operation failed: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#backup(java.lang.String,
     *      java.lang.String, long)
     */
    @MethodDesc(description = "Backs up the database", usage = "backup <backupAgent> <storageAgent> <timeout>")
    public String backup(
            @ParamDesc(name = "backupAgentName", description = "Backup agent to use or null for default") String backupAgentName,
            @ParamDesc(name = "storageAgentName", description = "Storage agent to use or null for default") String storageAgentName,
            @ParamDesc(name = "timeout", description = "Seconds to wait before timing out (0=infinity") long timeout)
            throws Exception
    {
        try
        {
            // Enqueue an event for the backup request.
            BackupEvent backupEvent = new BackupEvent(backupAgentName,
                    storageAgentName);
            handleEventSynchronous(backupEvent);

            // The event returns a Future on the backup task.
            Future<String> backupTask = backupEvent.getFuture();
            String uri = null;
            try
            {
                if (timeout <= 0)
                    uri = backupTask.get();
                else
                    uri = backupTask.get(timeout, TimeUnit.SECONDS);
            }
            catch (TimeoutException e)
            {
                logger.info("Backup request timed out: seconds=" + timeout);
                return uri = null;
            }

            // Return whatever we received.
            return uri;
        }
        catch (Exception e)
        {
            logger.error("Backup operation failed", e);
            throw new Exception("Backup operation failed: " + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#restore(java.lang.String,
     *      long)
     */
    @MethodDesc(description = "Restores the database", usage = "restore <uri> <timeout>")
    public String restore(
            @ParamDesc(name = "uri", description = "URI of backup to restore") String uri,
            @ParamDesc(name = "timeout", description = "Seconds to wait before timing out (0=infinity") long timeout)
            throws Exception
    {
        try
        {
            // Enqueue an event for the backup request.
            RestoreEvent restoreEvent = new RestoreEvent(uri);
            handleEventSynchronous(restoreEvent);

            // The event returns a Future on the backup task.
            Future<String> restoreTask = restoreEvent.getFuture();
            String completed = null;
            try
            {
                if (timeout <= 0)
                    completed = restoreTask.get();
                else
                    completed = restoreTask.get(timeout, TimeUnit.SECONDS);
            }
            catch (TimeoutException e)
            {
                logger.info("Restore timed out: seconds=" + timeout);
            }

            // Return whatever we received.
            return completed;
        }
        catch (Exception e)
        {
            logger.error("Restore operation failed", e);
            throw new Exception("Restore operation failed: " + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManager#createHelper()
     */
    @MethodDesc(description = "Returns a DynamicMBeanHelper to facilitate dynamic JMX calls", usage = "createHelper")
    public DynamicMBeanHelper createHelper() throws Exception
    {
        return JmxManager.createHelper(getClass(), serviceName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManager#createHelper()
     */
    @MethodDesc(description = "Returns an MBean for a replicator extension", usage = "getExtensionMBean")
    public Object getExtensionMBean(
            @ParamDesc(name = "name", description = "MBean name") String name)
            throws Exception
    {
        logger.warn("looking for MBean " + name);
        Object mbean = mbeans.get(name);
        logger.warn("Found = " + (mbean != null));
        return mbean;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#listExtensionMBeans()
     */
    @MethodDesc(description = "Returns a list of all extension MBean names", usage = "listExtensionMBeans")
    public List<String> listExtensionMBeans() throws Exception
    {
        return new ArrayList<String>(mbeans.keySet());
    }

    /**
     * Provisions current database from a donor. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#provision(java.lang.String,
     *      long)
     */
    @MethodDesc(description = "Provisions from another database", usage = "provision <replicatorUri> <timeout>")
    public boolean provision(
            @ParamDesc(name = "replicatorUri", description = "URI of replicator from which to provision") String replicatorUri,
            @ParamDesc(name = "timeout", description = "Seconds to wait before timing out (0=infinity") long timeout)
            throws Exception
    {
        try
        {
            // Enqueue an event for the provision request.
            ProvisionEvent provisionEvent = new ProvisionEvent(replicatorUri);
            handleEventSynchronous(provisionEvent);
            return provisionEvent.getResult();
        }
        catch (Exception e)
        {
            logger.error("Provision operation failed", e);
            throw new Exception("Provision operation failed: " + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#consistencyCheck(java.lang.String,
     *      java.lang.String, java.lang.String, int, int)
     */

    @MethodDesc(description = "Perform a cluster-wide consistency check", usage = "consistencyCheck <schema>[.{<table> | *}]")
    public int consistencyCheck(
            @ParamDesc(name = "method", description = "md5") String method,
            @ParamDesc(name = "schemaName", description = "schema to check") String schemaName,
            @ParamDesc(name = "tableName", description = "name of table to check") String tableName,
            @ParamDesc(name = "rowOffset", description = "row to start with") int rowOffset,
            @ParamDesc(name = "rowLimit", description = "maximum rows to check") int rowLimit)
            throws Exception
    {
        try
        {
            logger.info("Got consistency check request: " + method + ":"
                    + schemaName + "." + tableName + ":" + rowOffset + ","
                    + rowLimit);

            return openReplicator.consistencyCheck(method, schemaName,
                    tableName, rowOffset, rowLimit);
        }
        catch (Exception e)
        {
            logger.error("Consistency check failed", e);
            throw new Exception("Consistency check failed: " + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#signal(int,
     *      java.lang.String)
     */
    @MethodDesc(description = "sends a notification to the replicator manager about state changes", usage = "signal <signal_number> <message>")
    public void signal(
            @ParamDesc(name = "signal", description = "Signal number") int signal,
            @ParamDesc(name = "msg", description = "additional message passed along the signal") String msg)
            throws Exception
    {
        try
        {
            switch (signal)
            {
                case signalConfigured :
                    handleEventSynchronous(new ConfiguredNotification());
                    break;
                case signalShutdown :
                    handleEventSynchronous(new GoOfflineEvent());
                    break;
                case signalOfflineReached :
                    handleEventSynchronous(new OfflineNotification());
                    break;
                case signalRestored :
                    handleEventSynchronous(new RestoreCompletionNotification(
                            new URI(msg)));
                    break;
                case signalSynced :
                    handleEventSynchronous(new InSequenceNotification());
                    break;
                case signalConsistencyFail :
                    handleEventSynchronous(new ConsistencyCheckNotification());
                    break;
                case signalError :
                    handleEventSynchronous(new ErrorNotification(msg,
                            new PluginException(msg)));
                    break;
            }
        }
        catch (Exception e)
        {
            logger.error("Signal operation failed", e);
            throw new Exception("Signal operation failed: " + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#capabilities()
     */
    @MethodDesc(description = "Gets the replicator capabilities", usage = "getCapabilities")
    public Map<String, String> capabilities() throws Exception
    {
        try
        {
            ReplicatorCapabilities capabilities = openReplicator
                    .getCapabilities();
            return capabilities.asMap();
        }
        catch (Exception e)
        {
            logger.error("Capabilities call failed", e);
            throw new Exception("Capabilities call failed: " + e.getMessage());
        }
    }

    // END OF MBEAN API
    /**
     * Ensures that a required property has a default if unspecified.
     */
    protected String assertPropertyDefault(String key, String value)
    {
        if (properties.getString(key) == null)
        {
            logger.info("Assigning default global property value: key=" + key
                    + " default value=" + value);
            properties.setString(key, value);
        }
        return properties.getString(key);
    }

    protected String assertPropertySet(String key) throws ReplicatorException
    {
        String value = properties.getString(key);
        if (value == null)
            throw new ReplicatorException("Required property not set: key="
                    + key);
        else
            return value;
    }

    /**
     * Generic code to load and configure a plugin.
     */
    protected OpenReplicatorPlugin loadAndConfigurePlugin(String prefix,
            String name) throws ReplicatorException
    {
        String pluginPrefix = prefix + "." + name.trim();

        // Find the plug-in class name.
        String rawClassName = properties.getString(pluginPrefix);
        if (rawClassName == null)
            throw new ReplicatorException(
                    "Plugin class name property is missing or null:  key="
                            + pluginPrefix);
        String pluginClassName = rawClassName.trim();
        logger.info("Loading plugin: key=" + pluginPrefix + " class name="
                + pluginClassName);

        // Subset plug-in properties.
        TungstenProperties pluginProperties = properties.subset(pluginPrefix
                + ".", true);
        if (logger.isDebugEnabled())
            logger.debug("Plugin properties: " + pluginProperties.toString());

        // Load the plug-in class and configure its properties.
        OpenReplicatorPlugin plugin;
        try
        {
            plugin = (OpenReplicatorPlugin) Class.forName(pluginClassName)
                    .newInstance();
            if (plugin instanceof FilterManualProperties)
                ((FilterManualProperties) plugin).setConfigPrefix(pluginPrefix);
            else
                pluginProperties.applyProperties(plugin);
        }
        catch (PropertyException e)
        {
            throw new ReplicatorException(
                    "Unable to configure plugin properties: key="
                            + pluginPrefix + " class name=" + pluginClassName
                            + " : " + e.getMessage(), e);
        }
        catch (InstantiationException e)
        {
            throw new ReplicatorException("Unable to load plugin class: key="
                    + pluginPrefix + " class name=" + pluginClassName, e);
        }
        catch (IllegalAccessException e)
        {
            throw new ReplicatorException("Unable to load plugin class: key="
                    + pluginPrefix + " class name=" + pluginClassName, e);
        }
        catch (ClassNotFoundException e)
        {
            throw new ReplicatorException("Unable to load plugin class: key="
                    + pluginPrefix + " class name=" + pluginClassName, e);
        }

        // Plug-in is ready to go, so prepare it and call configure.
        try
        {
            plugin.prepare(this);
        }
        catch (ReplicatorException e)
        {
            throw new ReplicatorException("Unable to configure plugin: key="
                    + pluginPrefix + " class name=" + pluginClassName, e);
        }
        catch (Throwable t)
        {
            String message = "Unable to configure plugin: key=" + pluginPrefix
                    + " class name=" + pluginClassName;
            logger.error(message, t);
            throw new ReplicatorException(message, t);
        }

        // It worked. We have a configured plugin.
        logger.info("Plug-in configured successfully: key=" + pluginPrefix
                + " class name=" + pluginClassName);
        return plugin;
    }

    /**
     * Process configuration properties and instantiate/configure all plug-ins.
     * This method must be called before the configuration is usable.
     * 
     * @throws ReplicatorException Thrown if configuration fails
     */
    protected void doConfigure() throws ReplicatorException
    {
        // Determine the replicator role, providing a proper exception if the
        // role is not correctly set.
        String roleName = properties.getString(ReplicatorConf.ROLE);
        if (ReplicatorConf.ROLE_MASTER.equals(roleName)
                || ReplicatorConf.ROLE_SLAVE.equals(roleName))
        {
            // OK, do nothing
        }
        else
        {
            if (roleName == null)
            {
                throw new ReplicatorException(
                        "Property replicator.role is not set; must be the name of a pipeline");
            }
            else
            {
                logger.warn("Setting role to a value other than master or slave: "
                        + roleName);
            }
        }
        logger.info("Replicator role: " + roleName);

        // Ensure auto-enable property is valid.
        assertPropertyDefault(ReplicatorConf.AUTO_ENABLE,
                ReplicatorConf.AUTO_ENABLE_DEFAULT);

        // Check for auto-recovery and add delay/reset intervales if set.
        assertPropertyDefault(ReplicatorConf.AUTO_RECOVERY_MAX_ATTEMPTS,
                ReplicatorConf.AUTO_RECOVERY_MAX_ATTEMPTS_DEFAULT);
        autoRecoveryMaxAttempts = properties
                .getInt(ReplicatorConf.AUTO_RECOVERY_MAX_ATTEMPTS);

        if (autoRecoveryMaxAttempts > 0)
        {
            // Check the delay for automatic recovery.
            assertPropertyDefault(ReplicatorConf.AUTO_RECOVERY_DELAY_INTERVAL,
                    ReplicatorConf.AUTO_RECOVERY_DELAY_INTERVAL_DEFAULT);
            this.autoRecoveryDelayMillis = properties.getInterval(
                    ReplicatorConf.AUTO_RECOVERY_DELAY_INTERVAL).longValue();

            // Check the delay for resetting after being online for a
            // sufficiently long period of time.
            assertPropertyDefault(ReplicatorConf.AUTO_RECOVERY_RESET_INTERVAL,
                    ReplicatorConf.AUTO_RECOVERY_RESET_INTERVAL_DEFAULT);
            this.autoRecoveryResetMillis = properties.getInterval(
                    ReplicatorConf.AUTO_RECOVERY_RESET_INTERVAL).longValue();
        }

        // Ensure source ID is available.
        assertPropertyDefault(ReplicatorConf.SOURCE_ID,
                ReplicatorConf.SOURCE_ID_DEFAULT);

        // Ensure cluster name is available.
        siteName = assertPropertyDefault(ReplicatorConf.SITE_NAME,
                ReplicatorConf.SITE_NAME_DEFAULT);

        // Ensure cluster name is available.
        clusterName = assertPropertyDefault(ReplicatorConf.CLUSTER_NAME,
                ReplicatorConf.CLUSTER_NAME_DEFAULT);

        // Find and load open replicator plugin
        String replicatorName = assertPropertySet(ReplicatorConf.OPEN_REPLICATOR);
        if (openReplicator != null)
        {
            openReplicator.release();
        }
        openReplicator = loadAndConfigurePlugin(ReplicatorConf.OPEN_REPLICATOR,
                replicatorName);

        // Call configure method.
        openReplicator.configure(properties);
    }

    // Ensure backup manager is initialized.
    private void initializeBackupSubsystem(Event event, Entity entity,
            Transition transition, int actionType)
            throws TransitionRollbackException
    {
        if (backupManager == null)
        {
            try
            {
                BackupManager newManager = new BackupManager(eventDispatcher);
                newManager.initialize(properties);
                backupManager = newManager;
            }
            catch (BackupException e)
            {
                String message = "Unable to initialize backup manager: "
                        + e.getMessage();
                logger.error(message, e);
                throw new TransitionRollbackException(message, event, entity,
                        transition, actionType, e);
            }
        }
    }

    /**
     * Local wrapper of configure to help with unit testing.
     */
    @MethodDesc(description = "Configure properties by either rereading them or setting all properties from outside.", usage = "configure <properties>")
    public void configure(
            @ParamDesc(name = "tp", description = "Optional properties to replace replicator.properties") TungstenProperties tp)
            throws Exception
    {
        /* load new configuration in */
        handleEventSynchronous(new ConfigureEvent(tp));
    }

    /**
     * Wrapper method for methods that submits a synchronous event with proper
     * MBean error handling. This translates the various state machine
     * exceptions into a proper replicator exception.
     * 
     * @throws ReplicatorException
     */
    private void handleEventSynchronous(Event event) throws ReplicatorException
    {
        EventRequest request = null;
        try
        {
            request = eventDispatcher.put(event);
            request.get();
        }
        catch (InterruptedException e)
        {
            // Eat the exception and show that we were interrupted.
            logger.warn("Event processing was interrupted: "
                    + event.getClass().getName());
            Thread.currentThread().interrupt();
            return;
        }
        catch (ExecutionException e)
        {
            logger.warn("Event processing failed: "
                    + event.getClass().getName(), e);
            return;
        }

        // Check for errors.
        Object annotation = request.getAnnotation();
        if (annotation instanceof ReplicatorException)
        {
            ReplicatorException e = (ReplicatorException) annotation;
            if (logger.isDebugEnabled())
                logger.debug("Event processing failed", e);
            throw e;
        }
    }

    /**
     * Load properties from current replicator.properties location.
     */
    private void loadProperties(Event event, Entity entity,
            Transition transition, int actionType)
            throws TransitionRollbackException
    {
        try
        {
            propertiesManager.loadProperties();
            properties = propertiesManager.getProperties();
        }
        catch (ReplicatorException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("Unable to load properties", e);
            throw new TransitionRollbackException(
                    "Unable to load properties file: " + e.getMessage(), event,
                    entity, transition, actionType, e);
        }
    }

    /**
     * Implements a heartbeat call. This is meant to be called from within the
     * HeartbeatAction.
     */
    protected boolean doHeartbeat(TungstenProperties props) throws Exception
    {
        String initScript = properties
                .getString(ReplicatorConf.RESOURCE_JDBC_INIT_SCRIPT);
        if (initScript != null)
            props.setString(ReplicatorConf.RESOURCE_JDBC_INIT_SCRIPT,
                    initScript);
        return openReplicator.heartbeat(props);
    }

    /**
     * Returns the hostname to be used to bind ports for RMI use. This defaults
     * to 'localhost' for backwards compatibility.
     * 
     * @deprecated Not used by multi-service Replicator any more. See
     *             {@link com.continuent.tungsten.replicator.management.ReplicationServiceManager#getHostName(TungstenProperties)}
     */
    @Deprecated
    public static String getHostName()
    {
        String hostName = System.getProperty(ReplicatorConf.RMI_HOST,
                ReplicatorConf.RMI_DEFAULT_HOST);

        try
        {
            InetAddress addr = InetAddress.getLocalHost();

            // Get hostname
            hostName = addr.getHostName();
        }
        catch (UnknownHostException e)
        {
            logger.info("Exception when trying to get the host name from the environment, reason="
                    + e);
        }

        return hostName;
    }

    /**
     * Returns the listen port used by a master pipeline
     * 
     * @return the listen port as an integer
     */
    public int getMasterListenPort()
    {

        String listenURI = getMasterListenUri();
        if (listenURI == null)
            return -1;

        try
        {
            return Integer.parseInt(listenURI.substring(listenURI
                    .lastIndexOf(":") + 1));
        }
        catch (NumberFormatException n)
        {
            return -1;
        }
    }

    /**
     * Returns the doneLatch value.
     * 
     * @return Returns the doneLatch.
     */
    public CountDownLatch getDoneLatch()
    {
        return doneLatch;
    }

    /**
     * Sets the doneLatch value.
     * 
     * @param doneLatch The doneLatch to set.
     */
    public void setDoneLatch(CountDownLatch doneLatch)
    {
        this.doneLatch = doneLatch;
    }

    /**
     * Returns RMI port.
     */
    public String getRmiHost()
    {
        return rmiHost;
    }

    /**
     * Sets RMI host.
     */
    public void setRmiHost(String rmiHost)
    {
        this.rmiHost = rmiHost;
    }

    /**
     * Returns the rmiPort value.
     * 
     * @return Returns the rmiPort.
     */
    public int getRmiPort()
    {
        return rmiPort;
    }

    /**
     * Sets the rmiPort value.
     * 
     * @param rmiPort The rmiPort to set.
     */
    public void setRmiPort(int rmiPort)
    {
        this.rmiPort = rmiPort;
    }

    /**
     * Sets the host time zone.
     */
    public void setHostTimeZone(TimeZone hostTimeZone)
    {
        this.hostTimeZone = hostTimeZone;
    }

    /**
     * Returns the host time zone, which is the original time zone on the host
     * and may be different from the replicator, which uses GMT.
     */
    public TimeZone getHostTimeZone()
    {
        return hostTimeZone;
    }

    /**
     * Returns the host time zone.
     */
    public void setReplicatorTimeZone(TimeZone replicatorTimeZone)
    {
        this.replicatorTimeZone = replicatorTimeZone;
    }

    /**
     * Returns the replicator time zone, which is GMT unless overridden in
     * services.properties.
     */
    public TimeZone getReplicatorTimeZone()
    {
        return replicatorTimeZone;
    }

    public static TungstenProperties getConfigurationProperties(
            String serviceName) throws ReplicatorException
    {
        // Start the property manager.
        ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf
                .getConfiguration(serviceName);
        PropertiesManager propertiesManager = new PropertiesManager(
                runtimeConf.getReplicatorProperties(),
                runtimeConf.getReplicatorDynamicProperties(),
                runtimeConf.getReplicatorDynamicRole());
        propertiesManager.loadProperties();

        return propertiesManager.getProperties();

    }

    public void provisionOnline(Map<String, String> controlParams)
            throws Exception
    {
        TungstenProperties params = new TungstenProperties(controlParams);
        GoOnlineEventProvisioning goOnlineProvisioningEvent = new GoOnlineEventProvisioning(
                params);

        try
        {
            handleEventSynchronous(goOnlineProvisioningEvent);
        }
        catch (ReplicatorException e)
        {
            String message = "Online operation failed";

            logger.error(message, e);
            if (e.getOriginalErrorMessage() != null)
            {
                message += " (" + e.getOriginalErrorMessage() + ")";
            }
            else
                message += " (" + e.getMessage() + ")";
            throw new Exception(message);
            // throw new Exception("Online operation failed", e);
        }

    }
}
