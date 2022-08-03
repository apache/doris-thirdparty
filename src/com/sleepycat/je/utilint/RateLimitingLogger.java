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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * A simple logger used to limit the rate at which messages related to a
 * specific object are logged to at most once within the configured time
 * period. The effect of the rate limited logging is to sample log messages
 * associated with the object.
 *
 * This type of logging is suitable for informational messages about the state
 * of some entity that may persist over some extended period of time, e.g. a
 * repeated problem communicating with a specific node, where the nature of the
 * problem may change over time.
 *
 * @param <T> the type of the object associated with the log message.
 */
public class RateLimitingLogger<T> {
    /**
     * Contains the objects that had messages last logged for them and the
     * associated time that it was last logged.
     */
    private final Map<T, Long> logEvents;

    /**
     *  The log message sampling period.
     */
    private final int logSamplePeriodMs;

    /* The number of log messages that were actually written. */
    private long limitedMessageCount = 0;

    private final Logger logger;

    /**
     * Constructs a configured RateLimitingLoggerInstance.
     *
     * @param logSamplePeriodMs used to compute the max rate of
     *         1 message/logSamplePeriodMs
     * @param maxObjects the max number of MRU objects to track
     *
     * @param logger the rate limited messages are written to this logger
     */
    @SuppressWarnings("serial")
    public RateLimitingLogger(final int logSamplePeriodMs,
                              final int maxObjects,
                              final Logger logger) {

        this.logSamplePeriodMs = logSamplePeriodMs;
        this.logger = logger;

        logEvents = new LinkedHashMap<T,Long>(9) {
            @Override
            protected boolean
            removeEldestEntry(Map.Entry<T, Long> eldest) {

              return size() > maxObjects;
            }
          };
    }

    /* For testing */
    public synchronized long getLimitedMessageCount() {
        return limitedMessageCount;
    }


    /* For testing */
    int getMapSize() {
        return logEvents.size();
    }

    /**
     * Logs the message, if one has not already been logged for the object
     * in the current time interval.
     *
     * @param object the object associated with the log message
     *
     * @param level the level to be used for logging
     *
     * @param string the log message string
     */
    public synchronized void log(T object, Level level, String string) {

        if (object == null) {
            logger.log(level, string);
            return;
        }

        final Long timeMs = logEvents.get(object);

        final long now = System.currentTimeMillis();
        if ((timeMs == null) ||
            (now > (timeMs + logSamplePeriodMs))) {
            limitedMessageCount++;
            logEvents.put(object, now);
            logger.log(level, string);
        }
    }

    public Logger getInternalLogger() {
        return logger;
    }
}
