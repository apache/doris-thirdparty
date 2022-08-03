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

package com.sleepycat.je.dbi;

import com.sleepycat.je.utilint.TestHook;

/**
 * Store information about a DatabaseImpl that will have to be deleted or
 * updated at transaction commit or abort. This handles cleanup after
 * operations like Environment.truncateDatabase, removeDatabase and
 * renameDatabase. Cleanup like this is done outside the usual transaction
 * commit or node undo processing, because the mapping tree is always
 * auto-committed to avoid deadlock and is essentially non-transactional.
 */
public class DbCleanup {

    public enum Action {DELETE, RENAME}

    private final DatabaseImpl dbImpl;
    private final Action action;
    private final boolean atCommit;
    private final String newName;

    DbCleanup(final DatabaseImpl dbImpl,
              final Action action,
              final boolean atCommit) {
        this(dbImpl, action, atCommit, null);
    }

    DbCleanup(final DatabaseImpl dbImpl,
              final Action action,
              final boolean atCommit,
              final String newName) {

        assert (action == Action.RENAME) == (newName != null);

        this.dbImpl = dbImpl;
        this.action = action;
        this.atCommit = atCommit;
        this.newName = newName;
    }

    public static TestHook modifyDbRootHook;

    /**
     * Make sure that a set of DbCleanup only has one entry per
     * databaseImpl/action tuple.
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DbCleanup)) {
            return false;
        }

        final DbCleanup other = (DbCleanup) obj;

        return dbImpl.equals(other.dbImpl) &&
            action == other.action &&
            atCommit == other.atCommit;
    }

    @Override
    public int hashCode() {
        return dbImpl.hashCode();
    }

    /**
     * Do any preparation needed for a cleanup task _before_ the txn's write
     * locks have been released.
     */
    public static void setState(final DbCleanup cleanup,
                                final boolean isCommit) {

        final DatabaseImpl dbImpl = cleanup.dbImpl;

        switch (cleanup.action) {
        case RENAME:
            if (cleanup.atCommit == isCommit) {
                dbImpl.setNameAndType(cleanup.newName);
            }
            break;
        case DELETE:
            if (cleanup.atCommit == isCommit) {
                dbImpl.setDeleteStarted();
            }
            break;
        }
    }

    /**
     * Perform and complete the cleanup task _after_ the txn's write locks
     * have been released.
     */
    public static void execute(final EnvironmentImpl envImpl,
                               final DbCleanup[] cleanupArray,
                               final boolean isCommit) {

        final DbTree dbTree = envImpl.getDbTree();

        for (final DbCleanup cleanup : cleanupArray) {

            final DatabaseImpl dbImpl = cleanup.dbImpl;

            switch (cleanup.action) {
            case RENAME:
                /*
                 * We do not even attempt to support updates (renaming) along
                 * with truncate or remove for the same DB in the same txn.
                 */
                try {
                    if (cleanup.atCommit == isCommit) {

                        if (modifyDbRootHook != null) {
                            modifyDbRootHook.doHook();
                        }

                        dbTree.modifyDbRoot(dbImpl);
                    }
                } finally {
                    dbTree.releaseDb(dbImpl);
                }
                break;
            case DELETE:
                if (cleanup.atCommit == isCommit) {
                    /*
                     * If dbCleanupSet contains same databases with different
                     * atCommit, first release the database, then delete it.
                     * [#19636]
                     */
                    if (checkRepeatedDeletedDB(cleanupArray, cleanup)) {
                        dbTree.releaseDb(dbImpl);
                    }

                    /* releaseDb will be called by startDbExtinction. */
                    envImpl.getExtinctionScanner().startDbExtinction(dbImpl);

                } else {
                    /*
                     * If dbCleanupSet contains same databases with different
                     * atCommit, do nothing. [#19636]
                     */
                    if (!checkRepeatedDeletedDB(cleanupArray, cleanup)) {
                        dbTree.releaseDb(dbImpl);
                    }
                }
                break;
            }
        }
    }

    private static boolean checkRepeatedDeletedDB(final DbCleanup[] array,
                                                  final DbCleanup cleanup) {
        for (final DbCleanup element : array) {
            if (element.action == Action.DELETE &&
                element.dbImpl.getId().equals(cleanup.dbImpl.getId()) &&
                element.atCommit != cleanup.atCommit){
                return true;
            }
        }
        return false;
    }

    /**
     * Process cleanup task when a non-transactional DB operation is complete
     * (meaning that the NameLN was logged).
     */
    public static void setStateAndExecute(final EnvironmentImpl envImpl,
                                          final DbCleanup cleanup) {

        final DbTree dbTree = envImpl.getDbTree();
        final DatabaseImpl dbImpl = cleanup.dbImpl;

        switch (cleanup.action) {
        case RENAME:
            try {
                if (cleanup.atCommit) {
                    dbImpl.setNameAndType(cleanup.newName);

                    if (modifyDbRootHook != null) {
                        modifyDbRootHook.doHook();
                    }

                    dbTree.modifyDbRoot(cleanup.dbImpl);
                }
            } finally {
                dbTree.releaseDb(dbImpl);
            }
            break;
        case DELETE:
            if (cleanup.atCommit) {
                /* releaseDb will be called by startDbExtinction. */
                dbImpl.setDeleteStarted();
                envImpl.getExtinctionScanner().startDbExtinction(dbImpl);
            } else {
                dbTree.releaseDb(dbImpl);
            }
            break;
        }
    }
}
