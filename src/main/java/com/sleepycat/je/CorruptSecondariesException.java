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

package com.sleepycat.je;

import java.util.Map;

/**
 * Thrown by {@link Environment#verify Environment.openDatabase} if one or
 * more corrupt {@link SecondaryDatabase}s are detected.
 *
 * @since 18.1.0
 */
public class CorruptSecondariesException  extends OperationFailureException {

    private static final long serialVersionUID = 1;

    private final Map<String, SecondaryIntegrityException> exceptions;

    /**
     * For internal use only.
     * @hidden
     */
    public CorruptSecondariesException(
        Map<String, SecondaryIntegrityException> exceptions) {

        super(null /*locker*/, false /*abortOnly*/,
            makeMessage(exceptions),
            null /*cause*/);

        this.exceptions = exceptions;
    }

    private static String makeMessage(
        Map<String, SecondaryIntegrityException> exceptions) {

        final StringBuilder s = new StringBuilder();
        s.append("Detected ").append(exceptions.size());
        s.append(" corrupt SecondaryDatabases.");

        for (SecondaryIntegrityException e : exceptions.values()) {
            s.append(" [").append(e).append("]");
        }

        return s.toString();
    }

    /**
     * For internal use only.
     * @hidden
     */
    private CorruptSecondariesException(String message,
                                        CorruptSecondariesException cause) {
        super(message, cause);
        exceptions = cause.exceptions;
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new CorruptSecondariesException(msg, this);
    }

    /**
     * Returns a map containing one exception for each corrupt
     * {@link SecondaryDatabase} that was detected. The map key is the name of
     * the {@code SecondaryDatabase}.
     */
    public Map<String, SecondaryIntegrityException> getSecondaryExceptions() {
        return exceptions;
    }
}
