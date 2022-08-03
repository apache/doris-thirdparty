/*-
 * Copyright (C) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */


package com.sleepycat.je.utilint;

import java.util.Set;
import java.util.logging.Logger;

/**
 * The subclass that introduces tasks specific to JE.
 */
public class JETaskCoordinator extends TaskCoordinator {

    /* Cooperating JE tasks. */

    /*
     * This Task is just present as an example and is not actually used at
     * present. We expect RNTaskCoordinator to subclass this class.
     */
    public static final Task JE_STORAGE_STATS_TASK =
        new Task("JENetworkRestore", 1);


    /**
     * The task coordinator that supplies JE tasks to the coordinator.
     *
     * @param logger the logger to be used
     *
     * @param tasks additional housekeeping tasks that will share permits with
     * JE tasks
     *
     * @param active determines whether the coordinator is active
     *
     * @see TaskCoordinator#TaskCoordinator(Logger, Set, boolean)
     */
    public JETaskCoordinator(Logger logger,
                             Set<Task> tasks,
                             boolean active) {
        super(logger, tasks, active);
    }

}
