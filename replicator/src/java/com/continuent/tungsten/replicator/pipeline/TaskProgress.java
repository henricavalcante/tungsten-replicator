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

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Tracks statistics for an individual task, which is identified by a task ID.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TaskProgress
{
    private final String   stageName;
    private final int      taskId;
    private ReplDBMSHeader lastProcessedEvent       = null;
    private ReplDBMSHeader lastCommittedEvent       = null;
    private boolean        cancelled                = false;
    private long           eventCount               = 0;
    private long           blockCount               = 0;
    private long           lastCommittedBlockSize   = -1;
    private long           lastCommittedBlockMillis = -1;
    private long           applyLatencyMillis       = 0;
    private long           startMillis;
    private long           totalExtractMillis       = 0;
    private long           totalFilterMillis        = 0;
    private long           totalApplyMillis         = 0;
    private TaskState      state                    = TaskState.other;

    // Used to mark the beginning of a timing interval.
    private long           intervalStartMillis      = 0;

    // Used to mark the end of the last known interval so we can
    // accurately compute elapsed time.
    private long           endMillis                = 0;

    // Used to compute the number of events for last block as
    // well as the length of time before commit.
    private long           eventCountAtLastCommit   = -1;
    private long           lastCommitMillis         = -1;

    /**
     * Defines a new task progress tracker for the given task ID.
     * 
     * @param stageName Name of stage to which task belongs
     * @param taskId Task ID number
     */
    TaskProgress(String stageName, int taskId)
    {
        this.stageName = stageName;
        this.taskId = taskId;
    }

    /**
     * Create a clone of a current instance.
     */
    public TaskProgress(TaskProgress other)
    {
        this.stageName = other.getStageName();
        this.taskId = other.getTaskId();
        this.applyLatencyMillis = other.getApplyLatencyMillis();
        this.cancelled = other.isCancelled();
        this.blockCount = other.getBlockCount();
        this.endMillis = other.getEndMillis();
        this.eventCount = other.getEventCount();
        this.eventCountAtLastCommit = other.getEventCountAtLastCommit();
        this.lastProcessedEvent = other.getLastProcessedEvent();
        this.lastCommittedEvent = other.getLastCommittedEvent();
        this.lastCommittedBlockSize = other.getLastCommittedBlockSize();
        this.lastCommittedBlockMillis = other.getLastCommittedBlockMillis();
        this.lastCommitMillis = other.getLastCommitMillis();
        this.startMillis = other.getStartMillis();
        this.state = other.getState();
        this.totalApplyMillis = other.getTotalApplyMillis();
        this.totalExtractMillis = other.getTotalExtractMillis();
        this.totalFilterMillis = other.getTotalFilterMillis();
    }

    /**
     * Start the task progress timer. Should be called when a task thread begins
     * processing.
     */
    public void begin()
    {
        startMillis = System.currentTimeMillis();
        endMillis = startMillis;
    }

    public String getStageName()
    {
        return this.stageName;
    }

    public int getTaskId()
    {
        return this.taskId;
    }

    public ReplDBMSHeader getLastProcessedEvent()
    {
        return lastProcessedEvent;
    }

    public void setLastProcessedEvent(ReplDBMSHeader lastEvent)
    {
        this.lastProcessedEvent = lastEvent;
    }

    public ReplDBMSHeader getLastCommittedEvent()
    {
        return lastCommittedEvent;
    }

    public void setLastCommittedEvent(ReplDBMSHeader lastCommittedEvent)
    {
        if (lastCommittedEvent == null)
            throw new RuntimeException("BUG: Attempt to commit a null event!");
        this.lastCommittedEvent = lastCommittedEvent;
        this.blockCount++;

        // Store the current block size, which is computed from the event
        // count at previous commit.
        lastCommittedBlockSize = getCurrentBlockSize();
        eventCountAtLastCommit = eventCount;

        // Store the commit interval length, assuming we have a valid beginning
        // value.
        if (lastCommitMillis > 1)
        {
            lastCommittedBlockMillis = System.currentTimeMillis()
                    - lastCommitMillis;
        }
        lastCommitMillis = System.currentTimeMillis();
    }

    public long getLastCommittedBlockMillis()
    {
        return lastCommittedBlockMillis;
    }

    public long getLastCommitMillis()
    {
        return lastCommitMillis;
    }

    public boolean isCancelled()
    {
        return cancelled;
    }

    public void setCancelled(boolean cancelled)
    {
        this.cancelled = cancelled;
    }

    public long getEventCount()
    {
        return eventCount;
    }

    public void setEventCount(long eventCount)
    {
        this.eventCount = eventCount;
    }

    public long getEventCountAtLastCommit()
    {
        return eventCountAtLastCommit;
    }

    public void setEventCountAtLastCommit(long eventCountAtLastCommit)
    {
        this.eventCountAtLastCommit = eventCountAtLastCommit;
    }

    public void incrementEventCount()
    {
        this.eventCount++;
    }

    public long getBlockCount()
    {
        return blockCount;
    }

    public double getAverageBlockSize()
    {
        if (blockCount > 0)
            return (double) getEventCount() / blockCount;
        else
            return 0.0;
    }

    public long getCurrentBlockSize()
    {
        if (this.eventCountAtLastCommit > -1)
            return eventCount - eventCountAtLastCommit;
        else
            return -1;
    }

    public long getLastCommittedBlockSize()
    {
        return lastCommittedBlockSize;
    }

    public void setLastCommittedBlockSize(long lastCommittedBlockSize)
    {
        this.lastCommittedBlockSize = lastCommittedBlockSize;
    }

    /** Return time in seconds of last committed block. */
    public double getLastCommittedBlockTime()
    {
        if (this.lastCommittedBlockMillis > 0)
            return lastCommittedBlockMillis / 1000.0;
        else 
            return 0.0;
    }

    /** Return apply latency in milliseconds. Sub-zero values are rounded to 0. */
    public long getApplyLatencyMillis()
    {
        // Latency may be sub-zero due to clock differences.
        if (applyLatencyMillis < 0)
            return 0;
        else
            return applyLatencyMillis;
    }

    /** Return apply latency in seconds. */
    public double getApplyLatencySeconds()
    {
        long applyLatencyMillis = getApplyLatencyMillis();
        return applyLatencyMillis / 1000.0;
    }

    public void setApplyLatencyMillis(long applyLatencyMillis)
    {
        this.applyLatencyMillis = applyLatencyMillis;
    }

    /** Returns the start time of the task. */
    public long getStartMillis()
    {
        return startMillis;
    }

    /** Returns the current end time for measuring intervals. */
    public long getEndMillis()
    {
        return endMillis;
    }

    /** Returns cumulative extract time in milliseconds. */
    public long getTotalExtractMillis()
    {
        return totalExtractMillis;
    }

    /** Return extract time in seconds. */
    public double getTotalExtractSeconds()
    {
        return getTotalExtractMillis() / 1000.0;
    }

    /** Start an extract interval. */
    public void beginExtractInterval()
    {
        intervalStartMillis = System.currentTimeMillis();
        endMillis = intervalStartMillis;
        state = TaskState.extract;
    }

    /** Add time for an extract operation interval. */
    public void endExtractInterval()
    {
        endMillis = System.currentTimeMillis();
        totalExtractMillis += (endMillis - intervalStartMillis);
        state = TaskState.other;
    }

    /** Returns cumulative filter time in milliseconds */
    public long getTotalFilterMillis()
    {
        return totalFilterMillis;
    }

    /** Return filter time in seconds. */
    public double getTotalFilterSeconds()
    {
        return getTotalFilterMillis() / 1000.0;
    }

    /** Start a filter interval. */
    public void beginFilterInterval()
    {
        intervalStartMillis = System.currentTimeMillis();
        endMillis = intervalStartMillis;
        state = TaskState.filter;
    }

    /** Add time for a filter operation interval. */
    public void endFilterInterval()
    {
        endMillis = System.currentTimeMillis();
        totalFilterMillis += (endMillis - intervalStartMillis);
        state = TaskState.other;
    }

    /** Returns cumulative extract time in milliseconds. */
    public long getTotalApplyMillis()
    {
        return totalApplyMillis;
    }

    /** Return apply time in seconds. */
    public double getTotalApplySeconds()
    {
        return getTotalApplyMillis() / 1000.0;
    }

    /** Start an apply interval. */
    public void beginApplyInterval()
    {
        intervalStartMillis = System.currentTimeMillis();
        endMillis = intervalStartMillis;
        state = TaskState.apply;
    }

    /** Add time for an apply operation interval. */
    public void endApplyInterval()
    {
        endMillis = System.currentTimeMillis();
        totalApplyMillis += (endMillis - intervalStartMillis);
        state = TaskState.other;
    }

    /** Returns remaining wall-clock time outside of extract/filter/apply. */
    public long getTotalOtherMillis()
    {
        long remaining = endMillis - startMillis - totalExtractMillis
                - totalFilterMillis - totalApplyMillis;
        return remaining;
    }

    /** Return other time in seconds. */
    public double getTotalOtherSeconds()
    {
        return getTotalOtherMillis() / 1000.0;
    }

    /** Returns the current task state. */
    public TaskState getState()
    {
        return state;
    }

    /**
     * Returns a shallow copy of this instance.
     */
    public TaskProgress clone()
    {
        TaskProgress clone = new TaskProgress(this);
        return clone;
    }
}
