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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.je.BinaryEqualityComparator;
import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseComparator;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PartialComparator;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.PreloadStats;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.cleaner.BaseUtilizationTracker;
import com.sleepycat.je.cleaner.DbFileSummary;
import com.sleepycat.je.cleaner.DbFileSummaryMap;
import com.sleepycat.je.cleaner.ExtinctionScanner;
import com.sleepycat.je.cleaner.FileProtector;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.log.DbOpReplicationContext;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.DbOperationType;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeUtils;
import com.sleepycat.je.trigger.PersistentTrigger;
import com.sleepycat.je.trigger.Trigger;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.util.verify.BtreeVerifier;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.util.ClassResolver;

/**
 * The underlying object for a given database.
 */
public class DatabaseImpl implements Loggable, Cloneable {

    /*
     * Delete processing states. See design note on database deletion and
     * truncation
     */
    private static final short NOT_DELETED = 1;
    private static final short DELETE_STARTED = 2;
    private static final short DELETE_FINISHED = 3;

    /*
     * Flag bits are the persistent representation of boolean properties
     * for this database.  The DUPS_ENABLED value is 1 for compatibility
     * with earlier log entry versions where it was stored as a boolean.
     *
     * Two bits are used to indicate whether this database is replicated or
     * not.
     * isReplicated = 0, notReplicated = 0 means replication status is
     *   unknown, because the db was created in an standalone environment.
     * isReplicated = 1, notReplicated = 0 means the db is replicated.
     * isReplicated = 0, notReplicated = 1 means the db is not replicated.
     * isReplicated = 1, notReplicated = 1 is an illegal combination.
     */
    private byte flags;
    private static final byte DUPS_ENABLED = 0x1;      // getSortedDuplicates()
    private static final byte TEMPORARY_BIT = 0x2;     // isTemporary()
    private static final byte IS_REPLICATED_BIT = 0x4; // isReplicated()
    private static final byte NOT_REPLICATED_BIT = 0x8;// notReplicated()
    private static final byte PREFIXING_ENABLED = 0x10;// getKeyPrefixing()
    /* 0x20 was used for UTILIZATION_REPAIR_DONE prior to log version 16. */
    private static final byte DUPS_CONVERTED = 0x40;   // getKeyPrefixing()

    private DatabaseId id;             // unique id
    private Tree tree;
    private EnvironmentImpl envImpl;   // Tree operations find the env this way
    private boolean transactional;     // All open handles are transactional
    private boolean durableDeferredWrite;  // Durable deferred write mode set
    private volatile boolean dirty;    // Utilization, root LSN, etc., changed
    private Set<Database> referringHandles; // Set of open Database handles
    private long eofLsn;          // Logical EOF LSN for range locking
    private volatile short deleteState;    // one of four delete states.
    private AtomicInteger useCount = new AtomicInteger();
                                  // If non-zero, eviction is prohibited
    /*
     * Tracks the number of write handle references to this impl. It's used
     * to determine the when the Trigger.open/close methods must be invoked.
     */
    private final AtomicInteger writeCount = new AtomicInteger();

    /*
     * Used in the past to hold per-file utilization counts for this DB.
     * It is no longer used, now that we scan INs to count utilization.
     * It is non-null only for old deleted MapLNs, for use in recovery replay.
     * Because it is very rarely used, and only for a short time period, its
     * memory is not budgeted.
     */
    private DbFileSummaryMap dbFileSummaries = null;

    /**
     * Log version when DB was created, or 0 if created prior to log version 6.
     */
    private byte createdAtLogVersion;

    /*
     * The user defined Btree and duplicate comparison functions, if specified.
     */
    private Comparator<byte[]> btreeComparator = null;
    private Comparator<byte[]> duplicateComparator = null;
    private byte[] btreeComparatorBytes = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
    private byte[] duplicateComparatorBytes = LogUtils.ZERO_LENGTH_BYTE_ARRAY;

    private boolean btreeComparatorByClassName = false;
    private boolean duplicateComparatorByClassName = false;
    private boolean btreePartialComparator = false;
    private boolean duplicatePartialComparator = false;
    private boolean btreeBinaryEqualityComparator = true;
    private boolean duplicateBinaryEqualityComparator = true;

    /* Key comparator uses the btree and dup comparators as needed. */
    private Comparator<byte[]> keyComparator = null;

    /*
     * The user defined triggers associated with this database.
     *
     * The triggers reference value contains all known triggers, persistent and
     * transient, or null if it has not yet been constructed, which is done
     * lazily.  It is constructed by unmarshalling the triggerBytes (persistent
     * triggers) and adding them to the transientTriggers.
     *
     * transientTriggers is null if there are none, and never an empty list.
     */
    private AtomicReference<List<Trigger>> triggers =
        new AtomicReference<>(null);
    private List<Trigger> transientTriggers = null;
    private byte[][] triggerBytes = null;

    /*
     * Cache some configuration values.
     */
    private int binDeltaPercent;
    private int maxTreeEntriesPerNode;

    /*
     * DB name is stored here for efficient access. It is kept in sync with
     * the NameLN's key that currently refers to this DB ID. When a DB is
     * renamed, this field is changed after the commit of the NameLN, but
     * before releasing the NameLN write lock.
     */
    private volatile String name;

    /* Set to true when opened as a secondary DB. */
    private volatile boolean knownSecondary = false;

    /*
     * The DbType of this DatabaseImpl.  Is determined lazily, so getDbType
     * should always be called rather than referencing the field directly.
     */
    private DbType dbType;

    private CacheMode cacheMode;

    /*
     * For debugging -- this gives the ability to force all non-internal
     * databases to use key prefixing.
     *
     * Note that doing
     *     ant -Dje.forceKeyPrefixing=true test
     * does not work because ant does not pass the parameter down to JE.
     */
    private static final boolean forceKeyPrefixing;
    static {
        String forceKeyPrefixingProp =
            System.getProperty("je.forceKeyPrefixing");
        if ("true".equals(forceKeyPrefixingProp)) {
            forceKeyPrefixing = true;
        } else {
            forceKeyPrefixing = false;
        }
    }

    /**
     * Create a database object for a new database.
     */
    public DatabaseImpl(Locker locker,
                        String dbName,
                        DatabaseId id,
                        EnvironmentImpl envImpl,
                        DatabaseConfig dbConfig)
        throws DatabaseException {

        this.id = id;
        this.envImpl = envImpl;

        setConfigProperties(locker, dbName, dbConfig, envImpl);
        cacheMode = dbConfig.getCacheMode();

        createdAtLogVersion = LogEntryType.LOG_VERSION;

        /* A new DB is implicitly converted to the new dups format. */
        if (getSortedDuplicates()) {
            setDupsConverted();
        }

        commonInit();

        initWithEnvironment();

        /*
         * The tree needs the env, make sure we assign it before
         * allocating the tree.
         */
        tree = new Tree(this);

        setNameAndType(dbName);
    }

    /**
     * Create an empty database object for initialization from the log.  Note
     * that the rest of the initialization comes from readFromLog().
     */
    public DatabaseImpl() {
        id = new DatabaseId();
        envImpl = null;

        tree = new Tree();

        commonInit();

        /* initWithEnvironment is called after reading and envImpl is set.  */
    }

    /* Set the DatabaseConfig properties for a DatabaseImpl. */
    public void setConfigProperties(Locker locker,
                                    String dbName,
                                    DatabaseConfig dbConfig,
                                    EnvironmentImpl envImpl) {
        setBtreeComparator(dbConfig.getBtreeComparator(),
                           dbConfig.getBtreeComparatorByClassName());
        setDuplicateComparator(dbConfig.getDuplicateComparator(),
                               dbConfig.getDuplicateComparatorByClassName());

        setTriggers(locker, dbName, dbConfig.getTriggers(),
                    true /*overridePersistentTriggers*/);

        if (dbConfig.getSortedDuplicates()) {
            setSortedDuplicates();
        }

        if (dbConfig.getKeyPrefixing() ||
            forceKeyPrefixing) {
            setKeyPrefixing();
        } else {
            clearKeyPrefixing();
        }

        if (dbConfig.getTemporary()) {
            setTemporary();
        }

        if (envImpl.isReplicated()) {
            if (dbConfig.getReplicated()) {
                setIsReplicatedBit();
            } else {
                setNotReplicatedBit();
            }
        }

        transactional = dbConfig.getTransactional();
        durableDeferredWrite = dbConfig.getDeferredWrite();
        maxTreeEntriesPerNode = dbConfig.getNodeMaxEntries();
    }

    private void commonInit() {

        deleteState = NOT_DELETED;
        referringHandles =
            Collections.synchronizedSet(new HashSet<Database>());
    }

    public void setNameAndType(String name) {
        this.name = name;
        /*
         * The type cannot change when a DB is renamed, so it is harmless to
         * always derive it from the name.
         */
        dbType = DbTree.typeForDbName(name);
    }

    /*
     * Returns true if this DB has been opened as a secondary DB. Currently,
     * secondary DB metadata is not persistent, so this is the best we can do.
     */
    boolean isKnownSecondary() {
        return knownSecondary;
    }

    public void setKnownSecondary() {
        knownSecondary = true;
    }

    /**
     * Initialize configuration settings when creating a new instance or after
     * reading an instance from the log.  The envImpl field must be set before
     * calling this method.
     */
    private void initWithEnvironment() {
        /* The eof LSN must be unique for each database in memory. */
        eofLsn = envImpl.getNodeSequence().getNextTransientLsn();

        assert !(replicatedBitSet() && notReplicatedBitSet()) :
            "The replicated AND notReplicated bits should never be set "+
            " together";

        /*
         * We'd like to assert that neither replication bit is set if
         * the environmentImpl is not replicated, but can't do that.
         * EnvironmentImpl.isReplicated() is not yet initialized if this
         * environment is undergoing recovery during replication setup.

        assert !((!envImpl.isReplicated() &&
                 (replicatedBitSet() || notReplicatedBitSet()))) :
            "Neither the replicated nor notReplicated bits should be set " +
            " in a non-replicated environment" +
            " replicatedBitSet=" + replicatedBitSet() +
            " notRepBitSet=" + notReplicatedBitSet();
        */

        DbConfigManager configMgr = envImpl.getConfigManager();

        binDeltaPercent =
            configMgr.getInt(EnvironmentParams.BIN_DELTA_PERCENT);

        /*
         * If maxTreeEntriesPerNode is zero (for a newly created database),
         * set it to the default config value.  When we write the DatabaseImpl
         * to the log, we'll store the default.  That way, if the default
         * changes, the fan out for existing databases won't change.
         */
        if (maxTreeEntriesPerNode == 0) {
            maxTreeEntriesPerNode =
                configMgr.getInt(EnvironmentParams.NODE_MAX);
        }

        /* Don't instantiate if comparators are unnecessary (DbPrintLog). */
        if (!envImpl.getNoComparators()) {

            ComparatorReader reader = new ComparatorReader(
                btreeComparatorBytes, "BtreeComparator",
                envImpl.getClassLoader());
            btreeComparator = reader.getComparator();
            btreeComparatorByClassName = reader.isClass();
            btreePartialComparator =
                btreeComparator instanceof PartialComparator;
            btreeBinaryEqualityComparator =
                (btreeComparator == null ||
                 btreeComparator instanceof BinaryEqualityComparator);

            reader = new ComparatorReader(
                duplicateComparatorBytes, "DuplicateComparator",
                envImpl.getClassLoader());
            duplicateComparator = reader.getComparator();
            duplicateComparatorByClassName = reader.isClass();
            duplicatePartialComparator =
                duplicateComparator instanceof PartialComparator;
            duplicateBinaryEqualityComparator =
                (duplicateComparator == null ||
                 duplicateComparator instanceof BinaryEqualityComparator);

            /* Key comparator is derived from dup and btree comparators. */
            resetKeyComparator();
        }
    }

    /**
     * Create a clone of this database that can be used as the new empty
     * database when truncating this database.  setId and setTree must be
     * called on the returned database.
     */
    DatabaseImpl cloneDatabase() {
        DatabaseImpl newDb;
        try {
            newDb = (DatabaseImpl) super.clone();
        } catch (CloneNotSupportedException e) {
            assert false : e;
            return null;
        }

        /* Re-initialize fields that should not be shared by the new DB. */
        newDb.id = null;
        newDb.tree = null;
        newDb.createdAtLogVersion = LogEntryType.LOG_VERSION;
        newDb.useCount = new AtomicInteger();
        return newDb;
    }

    /**
     * @return the database tree.
     */
    public Tree getTree() {
        return tree;
    }

    void setTree(Tree tree) {
        this.tree = tree;
    }

    /**
     * @return the database id.
     */
    public DatabaseId getId() {
        return id;
    }

    void setId(DatabaseId id) {
        this.id = id;
    }

    long getEofLsn() {
        return eofLsn;
    }

    /**
     * @return true if this database is transactional.
     */
    public boolean isTransactional() {
        return transactional;
    }

    /**
     * Sets the transactional property for the first opened handle.
     */
    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    /**
     * @return true if this database is temporary.
     */
    public boolean isTemporary() {
        return ((flags & TEMPORARY_BIT) != 0);
    }

    public static boolean isTemporary(byte flagVal) {
        return ((flagVal & TEMPORARY_BIT) != 0);
    }

    public boolean isInternalDb() {
        return getDbType().isInternal();
    }

    public DbType getDbType() {
        return dbType;
    }

    private void setTemporary() {
        flags |= TEMPORARY_BIT;
    }

    /**
     * @return true if this database was user configured for durable deferred
     * write mode.
     */
    public boolean isDurableDeferredWrite() {
        return durableDeferredWrite;
    }

    /**
     * @return true if write operations are not logged immediately.  This is
     * true if the user configured a durable DW database or a temporary
     * database.
     */
    public boolean isDeferredWriteMode() {
        return isDurableDeferredWrite() || isTemporary();
    }

    /**
     * Sets the deferred write property for the first opened handle.
     */
    public void setDeferredWrite(boolean durableDeferredWrite) {
        this.durableDeferredWrite = durableDeferredWrite;
    }

    /**
     * @return true if duplicates are allowed in this database.
     */
    public boolean getSortedDuplicates() {
        return (flags & DUPS_ENABLED) != 0;
    }

    public static boolean getSortedDuplicates(byte flagVal) {
        return (flagVal & DUPS_ENABLED) != 0;
    }

    public void setSortedDuplicates() {
        flags |= DUPS_ENABLED;
    }

    public boolean getDupsConverted() {
        return (flags & DUPS_CONVERTED) != 0;
    }

    public void setDupsConverted() {
        flags |= DUPS_CONVERTED;
    }

    /**
     * Returns whether all LNs in this DB are "immediately obsolete", meaning
     * two things:
     * 1) They are counted obsolete when logged and can be ignored by the
     *    cleaner entirely.
     * 2) As a consequence, they cannot be fetched by LSN, except under special
     *    circumstances where they are known to exist.
     *
     * Currently, this is synonymous with whether all LNs in this DB must have
     * zero length data, and partial comparators are not used.  Currently only
     * duplicate DBs are known to have zero length LNs, since there is no way
     * in the API to specify that LNs are immutable.  In the future we will
     * also support "immediately obsolete" LNs that are mutable and embedded
     * in the BIN in other ways, e.g., tiny data may be stored with the key.
     *
     * Note that deleted LNs (the logged deletion, not the prior version) are
     * always immediately obsolete also.  See LNLogEntry.isImmediatelyObsolete.
     */
    public boolean isLNImmediatelyObsolete() {
        return getSortedDuplicates() &&
            !btreePartialComparator &&
            !duplicatePartialComparator;
    }

    /**
     * This method should be the only method used to obtain triggers after
     * reading the MapLN from the log.  It unmarshalls the triggers lazily
     * here to avoid a call to getName() during recovery, when the DbTree is
     * not yet instantiated.
     */
    public List<Trigger> getTriggers() {

        /* When comparators are not needed, neither are triggers. */
        if (envImpl == null || envImpl.getNoComparators()) {
            return null;
        }

        /* If no transient or persistent triggers, return null. */
        if (triggerBytes == null && transientTriggers == null) {
            return null;
        }

        /* Just return them, if already constructed. */
        List<Trigger> myTriggers = triggers.get();
        if (myTriggers != null) {
            return myTriggers;
        }

        /*
         * Unmarshall triggers, add transient triggers, and update the
         * reference atomically. If another thread unmarshalls and updates it
         * first, use the value set by the other thread.  This ensures that a
         * single instance is always used.
         */
        myTriggers = TriggerUtils.unmarshallTriggers(getName(), triggerBytes,
                                                     envImpl.getClassLoader());
        if (myTriggers == null) {
            myTriggers = new LinkedList<>();
        }
        if (transientTriggers != null) {
            myTriggers.addAll(transientTriggers);
        }
        if (triggers.compareAndSet(null, myTriggers)) {
            return myTriggers;
        }
        myTriggers = triggers.get();
        assert myTriggers != null;
        return myTriggers;
    }

    public boolean hasUserTriggers() {
        return (triggerBytes != null) || (transientTriggers != null);
    }

    /**
     * @return true if key prefixing is enabled in this database.
     */
    public boolean getKeyPrefixing() {
        return (flags & PREFIXING_ENABLED) != 0;
    }

    /**
     * Returns true if the flagVal enables the KeyPrefixing, used to create
     * ReplicatedDatabaseConfig after reading a NameLNLogEntry.
     */
    static boolean getKeyPrefixing(byte flagVal) {
        return (flagVal & PREFIXING_ENABLED) != 0;
    }

    public void setKeyPrefixing() {
        flags |= PREFIXING_ENABLED;
    }

    public void clearKeyPrefixing() {
        if (forceKeyPrefixing) {
            return;
        }
        flags &= ~PREFIXING_ENABLED;
    }

    /**
     * @return true if this database is replicated. Note that we only need to
     * check the IS_REPLICATED_BIT, because we require that we never have both
     * IS_REPLICATED and NOT_REPLICATED set at the same time.
     */
    public boolean isReplicated() {
        return replicatedBitSet();
    }

    /**
     * @return true if this database is replicated.
     */
    public boolean unknownReplicated() {
        return ((flags & IS_REPLICATED_BIT) == 0) &&
            ((flags & NOT_REPLICATED_BIT) == 0);
    }

    private boolean replicatedBitSet() {
        return (flags & IS_REPLICATED_BIT) != 0;
    }

    public void setIsReplicatedBit() {
        flags |= IS_REPLICATED_BIT;
    }

    /**
     * @return true if this database's not replicated bit is set.
     */
    private boolean notReplicatedBitSet() {
        return (flags & NOT_REPLICATED_BIT) != 0;
    }

    private void setNotReplicatedBit() {
        flags |= NOT_REPLICATED_BIT;
    }

    public int getNodeMaxTreeEntries() {
        return maxTreeEntriesPerNode;
    }

    public void setNodeMaxTreeEntries(int newNodeMaxTreeEntries) {
        maxTreeEntriesPerNode = newNodeMaxTreeEntries;
    }

    /**
     * Used to determine whether to throw ReplicaWriteException when a write to
     * this database is attempted.  For the most part, writes on a replica are
     * not allowed to any replicated DB.  However, an exception is the DB
     * naming DB.  The naming DB contains a mixture of LNs for replicated and
     * non-replicated databases.  Here, we allow all writes to the naming DB.
     * DB naming operations for replicated databases on a replica, such as the
     * creation of a replicated DB on a replica, are prohibited by DbTree
     * methods (dbCreate, dbRemove, etc). [#20543]
     */
    public boolean allowReplicaWrite() {
        return !isReplicated() || getDbType().isMixedReplication();
    }

    /**
     * Sets the default mode for this database (all handles).  May be null to
     * use Environment default.
     */
    public void setCacheMode(CacheMode mode) {
        cacheMode = mode;
    }

    /**
     * Returns the default cache mode for this database. If the database has a
     * null cache mode and is not an internal database, the Environment default
     * is returned.  Null is never returned. CacheMode.DYNAMIC may be returned.
     */
    public CacheMode getDefaultCacheMode() {
        if (cacheMode != null) {
            return cacheMode;
        }
        if (isInternalDb()) {
            return CacheMode.DEFAULT;
        }
        return envImpl.getDefaultCacheMode();
    }

    /**
     * Set the duplicate comparison function for this database.
     *
     * @return true if the comparator was actually changed
     *
     * @param comparator - The Duplicate Comparison function.
     */
    public boolean setDuplicateComparator(
        Comparator<byte[]> comparator,
        boolean byClassName)
        throws DatabaseException {

        final byte[] newBytes =
            comparatorToBytes(comparator, byClassName, "DuplicateComparator");

        final boolean changed =
            !Arrays.equals(newBytes, duplicateComparatorBytes) ||
            ((comparator instanceof PartialComparator) !=
             (duplicateComparator instanceof PartialComparator)) ||
            ((comparator instanceof BinaryEqualityComparator) !=
             (duplicateComparator instanceof BinaryEqualityComparator));

        duplicateComparator = comparator;
        duplicateComparatorBytes = newBytes;
        duplicateComparatorByClassName = byClassName;

        duplicatePartialComparator =
            duplicateComparator instanceof PartialComparator;

        duplicateBinaryEqualityComparator =
            (duplicateComparator == null ||
             duplicateComparator instanceof BinaryEqualityComparator);

        if (changed) {
            /* Key comparator is derived from dup and btree comparators. */
            resetKeyComparator();
        }
        return changed;
    }

    /**
     * Sets the list of triggers associated with the database.
     *
     * @param dbName pass it in since it may not be available during database
     * creation
     * @param newTriggers the triggers to associate with the database
     * @param overridePersistentTriggers whether to overwrite persistent
     * triggers
     *
     * @return true if a {@link PersistentTrigger} was changed, and therefore
     * may need to be stored.
     */
    public boolean setTriggers(Locker locker,
                               String dbName,
                               List<Trigger> newTriggers,
                               boolean overridePersistentTriggers) {

        if ((newTriggers != null) && (newTriggers.size() == 0)) {
            newTriggers = null;
        }

        /* Construct new persistent triggers. */
        final byte newTriggerBytes[][];
        final boolean persistentChange;

        if (overridePersistentTriggers) {
            if (newTriggers == null) {
                newTriggerBytes = null;
                persistentChange = (this.triggerBytes != null);
            } else {
                /* Create the new trigger bytes. */
                int nTriggers = 0;
                for (Trigger trigger : newTriggers) {
                    if (trigger instanceof PersistentTrigger) {
                        nTriggers += 1;
                    }
                }
                if (nTriggers == 0) {
                    newTriggerBytes = null;
                    persistentChange = (this.triggerBytes != null);
                } else {
                    newTriggerBytes = new byte[nTriggers][];
                    int i=0;
                    for (Trigger trigger : newTriggers) {
                        if (trigger instanceof PersistentTrigger) {
                            newTriggerBytes[i++] = objectToBytes
                                (trigger, "Trigger-" + trigger.getName());
                            trigger.setDatabaseName(dbName);
                        }
                    }
                    persistentChange =
                        !Arrays.equals(triggerBytes, newTriggerBytes);
                }
            }
        } else {
            newTriggerBytes = triggerBytes;
            persistentChange = false;
        }

        /* Add transient triggers. */
        final List<Trigger> newTransientTriggers;
        final boolean transientChange;

        if (newTriggers == null) {
            newTransientTriggers = null;
            transientChange = (transientTriggers != null);
        } else {
            newTransientTriggers = new LinkedList<>();
            final Map<Trigger, Object> diffs = new IdentityHashMap<>();
            for (Trigger trigger : newTriggers) {
                if (!(trigger instanceof PersistentTrigger)) {
                    diffs.put(trigger, null);
                    newTransientTriggers.add(trigger);
                    trigger.setDatabaseName(dbName);
                }
            }
            if (transientTriggers == null) {
                transientChange = (newTransientTriggers.size() > 0);
            } else if (transientTriggers.size() !=
                       newTransientTriggers.size()) {
                transientChange = true;
            } else {
                for (Trigger trigger : transientTriggers) {
                    diffs.remove(trigger);
                }
                transientChange = (diffs.size() > 0);
            }
        }

        if (persistentChange || transientChange) {
            TriggerManager.invokeAddRemoveTriggers(locker,
                                                   getTriggers(),
                                                   newTriggers);
            /* Don't change fields until after getTriggers() call above. */
            triggerBytes = newTriggerBytes;
            transientTriggers =
                ((newTransientTriggers != null) &&
                 (newTransientTriggers.size() > 0)) ?
                newTransientTriggers :
                null;
            this.triggers.set(newTriggers);
        }

        return persistentChange;
    }

    /**
     * Called when a database is closed to clear all transient triggers and
     * call their 'removeTrigger' methods.
     */
    private void clearTransientTriggers() {
        final List<Trigger> oldTriggers = getTriggers();
        if (oldTriggers == null) {
            return;
        }
        final List<Trigger> newTriggers = new LinkedList<>(oldTriggers);
        final Iterator<Trigger> iter = newTriggers.iterator();
        while (iter.hasNext()) {
            final Trigger trigger = iter.next();
            if (!(trigger instanceof PersistentTrigger)) {
                iter.remove();
            }
        }
        /* The dbName param can be null because it is not used. */
        setTriggers(null /*locker*/, null /*dbName*/, newTriggers,
                    false /*overridePersistentTriggers*/);
    }

    /**
     * Set the btree comparison function for this database.
     *
     * @return true if the comparator was actually changed
     *
     * @param comparator - The btree Comparison function.
     */
    public boolean setBtreeComparator(
        Comparator<byte[]> comparator,
        boolean byClassName)
        throws DatabaseException {

        final byte[] newBytes =
            comparatorToBytes(comparator, byClassName, "BtreeComparator");

        final boolean changed =
            !Arrays.equals(newBytes, btreeComparatorBytes) ||
            ((btreeComparator instanceof PartialComparator) !=
             (comparator instanceof PartialComparator)) ||
            ((btreeComparator instanceof BinaryEqualityComparator) !=
             (comparator instanceof BinaryEqualityComparator));

        btreeComparator = comparator;
        btreeComparatorBytes = newBytes;
        btreeComparatorByClassName = byClassName;

        btreePartialComparator =
            btreeComparator instanceof PartialComparator;

        btreeBinaryEqualityComparator =
            (btreeComparator == null ||
             btreeComparator instanceof BinaryEqualityComparator);

        if (changed) {
            /* Key comparator is derived from dup and btree comparators. */
            resetKeyComparator();
        }
        return changed;
    }

    /**
     * This comparator should not be used directly for comparisons.  Use
     * getKeyComparator instead.
     *
     * @return the btree Comparator object.
     */
    public Comparator<byte[]> getBtreeComparator() {
        return btreeComparator;
    }

    /**
     * This comparator should not be used directly for comparisons.  Use
     * getKeyComparator instead.
     *
     * @return the duplicate Comparator object.
     */
    public Comparator<byte[]> getDuplicateComparator() {
        return duplicateComparator;
    }

    /**
     * Key comparator is derived from the duplicate and btree comparator
     */
    private void resetKeyComparator() {

        /* Initialize comparators. */
        if (btreeComparator instanceof DatabaseComparator) {
            ((DatabaseComparator) btreeComparator).initialize
                (envImpl.getClassLoader());
        }
        if (duplicateComparator instanceof DatabaseComparator) {
            ((DatabaseComparator) duplicateComparator).initialize
                (envImpl.getClassLoader());
        }

        /* Create derived comparator for duplicate database. */
        if (getSortedDuplicates()) {
            keyComparator = new DupKeyData.TwoPartKeyComparator
                (btreeComparator, duplicateComparator);
        } else {
            keyComparator = btreeComparator;
        }
    }

    /**
     * Should always be used when comparing keys for this database.
     *
     * For a duplicates database, the data is part two of the two-part database
     * key.  Therefore, the duplicates comparator and btree comparator are used
     * for comparing keys.  This synthetic comparator will call both of the
     * other two user-defined comparators as necessary.
     */
    public Comparator<byte[]> getKeyComparator() {
        return keyComparator;
    }

    /**
     * @return whether Comparator is set by class name, not by serializable
     * Comparator object.
     */
    public boolean getBtreeComparatorByClass() {
        return btreeComparatorByClassName;
    }

    /**
     * @return whether Comparator is set by class name, not by serializable
     * Comparator object.
     */
    public boolean getDuplicateComparatorByClass() {
        return duplicateComparatorByClassName;
    }

    public boolean allowsKeyUpdates() {
        return (btreePartialComparator || duplicatePartialComparator);
    }

    /**
     * @return whether Comparator implements BinaryEqualityComparator.
     */
    public boolean hasBtreeBinaryEqualityComparator() {
        return btreeBinaryEqualityComparator;
    }

    /**
     * @return whether Comparator implements BinaryEqualityComparator.
     */
    public boolean hasDuplicateBinaryEqualityComparator() {
        return duplicateBinaryEqualityComparator;
    }

    /**
     * Set the db environment after reading in the DatabaseImpl from the log.
     */
    public void setEnvironmentImpl(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
        initWithEnvironment();
        tree.setDatabase(this);
    }

    public EnvironmentImpl getEnv() {
        return envImpl;
    }

    /**
     * Returns whether one or more handles are open.
     */
    public boolean hasOpenHandles() {
        return referringHandles.size() > 0;
    }

    /**
     * Add a referring handle
     */
    public void addReferringHandle(Database db) {
        referringHandles.add(db);
    }

    /**
     * Decrement the reference count.
     */
    public void removeReferringHandle(Database db) {
        referringHandles.remove(db);
    }

    /**
     * Returns a copy of the referring database handles.
     */
    public Set<Database> getReferringHandles() {
        HashSet<Database> copy = new HashSet<>();
        synchronized (referringHandles) {
            copy.addAll(referringHandles);
        }
        return copy;
    }

    /**
     * Called after a handle onto this DB is closed.
     */
    public void handleClosed(boolean doSyncDw, boolean deleteTempDb)
        throws DatabaseException {

        if (referringHandles.isEmpty()) {

            /*
             * Transient triggers are discarded when the last handle is
             * closed.
             */
            clearTransientTriggers();

            /*
             * Remove a temporary database with no handles open.
             *
             * We are not synchronized here in any way that would prevent
             * another thread from opening a handle during this process, before
             * the NameLN is locked.  So we use noWait locking.  If a lock is
             * not granted, then another handle was opened and we cannot remove
             * the database until later.
             *
             * We pass the database ID to dbRemove in order to remove the
             * database only if the name matches the ID.  This accounts for the
             * remote possibility that the database is renamed or another
             * database is created with the same name during this process,
             * before the NameLN is locked.
             *
             * We can use a BasicLocker because temporary databases are always
             * non-transactional.
             */
            if (deleteTempDb && isTemporary()) {
                Locker locker =
                    BasicLocker.createBasicLocker(envImpl, true /* noWait */);
                boolean operationOk = false;
                try {
                    envImpl.getDbTree().dbRemove(locker, getName(), getId());
                    operationOk = true;
                } catch (DbTree.NeedRepLockerException e) {
                    /* Should never happen; a temp db is never replicated. */
                    throw EnvironmentFailureException.unexpectedException(
                        envImpl, e);
                } catch (DatabaseNotFoundException e) {
                    /* Do nothing if DB was removed or renamed. */
                } catch (LockConflictException e) {
                    /*
                     * We will have to remove this database later.  Note that
                     * we catch LockConflictException for simplicity but we
                     * expect either LockNotAvailableException or
                     * LockNotGrantedException.
                     */
                } catch (Error E) {
                    envImpl.invalidate(E);
                    throw E;
                } finally {
                    locker.operationEnd(operationOk);
                }
            }

            /*
             * Sync a durable deferred write database with no handles open.  If
             * a handle is opened during this process, then the sync may be
             * unnecessary but it will not cause a problem.
             */
            if (doSyncDw && isDurableDeferredWrite()) {
                sync(true);
            }
        }
    }

    /**
     * @return the referring handle count.
     */
    int getReferringHandleCount() {
        return referringHandles.size();
    }

    /**
     * Increments the use count of this DB to prevent it from being evicted.
     * Called by the DbTree.createDb/getDb methods that return a DatabaseImpl.
     * Must be called while holding a lock on the MapLN. See isInUse. [#13415]
     */
    void incrementUseCount() {
        /*
         * Throw an EFE when the DELETE_FINISHED state is set on a DB to be
         * returned by DbTree.getDb. This state should only be set after
         * deleting the MapLN. And deleting the MapLN requires that the use
         * count is LTE one. And after that, getDb will return null.
         */
        if (isDeleteFinished()) {
            throw EnvironmentFailureException.unexpectedState(
                "DeleteFinished state for active DB id=" + id +
                    " name=" + name + " useCount=" + useCount.get());
        }
        useCount.incrementAndGet();
    }

    /**
     * Increments the write count and returns the updated value.
     * @return updated write count
     */
    public int noteWriteHandleOpen() {
        return writeCount.incrementAndGet();
    }

    /**
     * Decrements the write count and returns the updated value.
     * @return updated write count
     */
    public int noteWriteHandleClose() {
        int count = writeCount.decrementAndGet();
        assert count >= 0;
        return count;
    }

    /**
     * Decrements the use count of this DB, allowing it to be evicted if the
     * use count reaches zero.  Called via DbTree.releaseDb to release a
     * DatabaseImpl that was returned by a DbTree.createDb/getDb method. See
     * isInUse. [#13415]
     */
    void decrementUseCount() {
        assert useCount.get() > 0;
        useCount.decrementAndGet();
    }

    /**
     * Returns whether this DB is in use and cannot be evicted.  Called by
     * MapLN.isEvictable while holding a write-lock on the MapLN and a latch on
     * its parent BIN. [#13415]
     *
     * When isInUse returns false (while holding a write-lock on the MapLN and
     * a latch on the parent BIN), it guarantees that the database object
     * is not in use and cannot be acquired by another thread (via
     * DbTree.createDb/getDb) until both the MapLN lock and BIN latch are
     * released.  This guarantee is due to the fact that DbTree.createDb/getDb
     * only increment the use count while holding a read-lock on the MapLN.
     * Therefore, it is safe to evict the MapLN when isInUse returns false.
     *
     * When isInUse returns true, it is possible that another thread may
     * decrement the use count at any time, since no locking or latching is
     * performed when calling DbTree.releaseDb (which calls decrementUseCount).
     * Therefore, it is not guaranteed that the MapLN is in use when isInUse
     * returns true.  A true result means: the DB may be in use, so it is not
     * safe to evict it.
     */
    public boolean isInUse() {
        return (useCount.get() > 0);
    }

    /**
     * Checks whether a database is in use during a remove or truncate database
     * operation.
     */
    boolean isInUseDuringDbRemove() {

        /*
         * The use count is at least one here, because remove/truncate has
         * called getDb but releaseDb has not yet been called.  Note that the
         * database must be closed in order to remove or truncate.
         */
        return useCount.get() > 1;
    }

    /**
     * Flush all dirty nodes for this database to disk.
     *
     * @throws UnsupportedOperationException via Database.sync.
     */
    public synchronized void sync(boolean flushLog)
        throws DatabaseException {

        if (!isDurableDeferredWrite()) {
            throw new UnsupportedOperationException
                ("Database.sync() is only supported " +
                                        "for deferred-write databases");
        }

        if (tree.rootExists()) {
            envImpl.getCheckpointer().syncDatabase(envImpl, this, flushLog);
        }
    }

    /**
     * For this secondary database return the primary that it is associated
     * with, or null if not associated with any primary.  Note that not all
     * handles need be associated with a primary.
     */
    public Database findPrimaryDatabase() {
        synchronized (referringHandles) {
            for (Database obj : referringHandles) {
                if (obj instanceof SecondaryDatabase) {
                    return ((SecondaryDatabase) obj).getPrimaryDatabase();
                }
            }
        }
        return null;
    }

    /**
     * Returns the DB name. If a rename txn is open, the old (last committed)
     * value is returned; i.e., an uncommitted value is never returned. Null
     * may be returned, but only during recovery before upgrading the MapLNs
     * to log version 16.
     */
    public String getName() {
        return name;
    }

    /**
     * If {@link #readFromLog} has read the file summaries from an old MapLN,
     * we only need it if the MapLN is deleted (for recovery replay). In other
     * cases, call this method to clear it.
     */
    public void clearDbFileSummaries() {
        dbFileSummaries = null;
    }

    /**
     * Returns whether this database has new (unflushed) utilization info or
     * the root LSN was modified after it was last logged.
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Sets dirty in order to force the MapLN to be flushed later.
     *
     * This flag is used when utilization is changed, the root LSN is changed,
     * etc, in order to cause the MapLN to be flushed during the next
     * checkpoint, or when utilization info is logged.
     */
    public void setDirty() {
        dirty = true;
    }

    /**
     * Returns whether this database's MapLN must be flushed during a
     * checkpoint.
     */
    public boolean isCheckpointNeeded() {
        return isDirty() || isTemporary();
    }

    /**
     * Returns true if this database is being deleted. The NameLN for
     * this DB has been deleted so it is inaccessible to applications.
     * However, delete cleanup/extinction may still be in progress.
     *
     * Note that the checkpointer should flush a node even if isDeleting might
     * return true for the node's DB. This is necessary to avoid loss of
     * obsolete info. See {@code ExtinctionScanner.DatabaseExtinction}.
     */
    public boolean isDeleting() {
        return !(deleteState == NOT_DELETED);
    }

    /**
     * Returns true if this database is deleted and all cleanup/extinction is
     * finished. This method is intended for assertions in the Evictor, to
     * check that no IN for such a DB is on the INList. This method never
     * returns true for a DB that is {@link #isInUse()}, i.e., for which
     * {@link DbTree#getDb} has been called and {@link DbTree#releaseDb} has
     * not yet been called. See check in {@link #incrementUseCount}.
     */
    public boolean isDeleteFinished() {
        return (deleteState == DELETE_FINISHED);
    }

    /**
     * The delete cleanup is starting. Set this before releasing any
     * write locks held for a db operation.
     */
    public void setDeleteStarted() {
        assert (deleteState == NOT_DELETED);

        deleteState = DELETE_STARTED;
    }

    /**
     * The delete cleanup is finished. Set this after the async extinction
     * process is complete and the MapLN is deleted.
     */
    public void setDeleteFinished() {
        assert (deleteState == DELETE_STARTED);

        deleteState = DELETE_FINISHED;
    }

    /**
     * When an old version (version LT 16) MapLN deletion is replayed by
     * recovery, we must count it obsolete using the DbFileSummary metadata
     * that was maintained in this older version. For version 16 and above,
     * DB extinction is used instead.
     */
    public void countObsoleteOldVersionDb(
        final BaseUtilizationTracker tracker,
        final long mapLnLsn) {

        if (dbFileSummaries != null) {
            tracker.countObsoleteDb(dbFileSummaries, mapLnLsn);
        }
    }

    public BtreeStats stat(StatsConfig config)
        throws DatabaseException {

        if (tree == null) {
            return new BtreeStats();
        }

        final BtreeStats stats;

        if (config.getFast()) {
            stats = new BtreeStats();
        } else {
            /*
             * Use verify() to get stats. This is fairly inexpensive and the
             * performance of this method is not critical. In the future we
             * could optimize a little by disabling basic btree verification.
             */
            final VerifyConfig verifyConfig = new VerifyConfig();

            verifyConfig.setShowProgressInterval(
                config.getShowProgressInterval());

            verifyConfig.setShowProgressStream(
                config.getShowProgressStream());

            stats = verify(verifyConfig);
        }

        return stats;
    }

    public BtreeStats verify(VerifyConfig config)
        throws DatabaseException {

        if (tree == null) {
            return new BtreeStats();
        }

        final BtreeVerifier verifier = new BtreeVerifier(envImpl);
        verifier.setBtreeVerifyConfig(config);

        return verifier.verifyDatabase(getName(), getId());
    }

    /**
     * Preload the cache, using up to maxBytes bytes or maxMillsecs msec.
     *
     * @throws IllegalArgumentException via Database.preload
     */
    public PreloadStats preload(PreloadConfig config)
        throws DatabaseException {

        return envImpl.preload(new DatabaseImpl[] { this }, config);
    }

    /**
     * The processLSN() code for CountProcessor.
     */
    private static class DOSCountCallback
        implements DiskOrderedScanner.RecordProcessor {

        public long count = 0;

        @Override
        public void process(
            int dbIdx,
            byte[] key,
            byte[] data,
            int expiration,
            boolean expirationInHours) {

            assert(key == null);
            assert(data == null);
            ++count;
        }

        @Override
        public boolean canProcessWithoutBlocking(int nRecords) {
            return true;
        }

        @Override
        public int getCapacity() {
            return Integer.MAX_VALUE;
        }

        @Override
        public void checkShutdown() {
        }
    }

    /**
     * Count entries in the database including dups, but don't dirty the cache.
     */
    public long count(long memoryLimit)
        throws DatabaseException {

        try {
            MemoryBudget mb = envImpl.getMemoryBudget();

            /*
             * Must have at least 1MB of memory to be used by DOS (1MB is
             * chosen rather arbitrarely).
             */
            long minMem = 1024 * 1024;

            /*
             * Use a heuristic to calculate the memory limit if none was
             * provided by the user. This heuristic makes sure that the
             * JE cache will not be affected, but otherwise, it is also
             * rather arbitrary.
             */
            if (memoryLimit <= 0) {
                memoryLimit =
                    (JVMSystemUtils.getRuntimeMaxMemory() - mb.getMaxMemory()) / 10;
            }

            if (memoryLimit < minMem) {
                //System.out.println("Using skip-base Database.count()");
                return count(null, true, null, true);
            }

            DOSCountCallback counter = new DOSCountCallback();

            DatabaseImpl[] dbs = new DatabaseImpl[1];
            dbs[0] = this;

            DiskOrderedScanner scanner = new DiskOrderedScanner(
                dbs, counter, true/*serialDBScan*/,
                true/*binsOnly*/, true /*keyOnly*/, true/*countOnly*/,
                Long.MAX_VALUE/*lsnBatchSize*/, memoryLimit,
                false/*debug*/);

            scanner.scan(
                FileProtector.DATABASE_COUNT_NAME,
                envImpl.getNodeSequence().getNextDatabaseCountId());

            if (LatchSupport.TRACK_LATCHES) {
                LatchSupport.expectBtreeLatchesHeld(0);
            }

            return counter.count;

        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * For future use as API method.  Implementation is incomplete.
     *
     * Counts entries in a key range by positioning a cursor on the beginning
     * key and skipping entries until the ending key is encountered.
     */
    private long count(
        DatabaseEntry beginKey,
        boolean beginInclusive,
        DatabaseEntry endKey,
        boolean endInclusive) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry noData = new DatabaseEntry();
        noData.setPartial(0, 0, true);

        final Locker locker = BasicLocker.createBasicLocker(envImpl);
        final LockMode lockMode = LockMode.READ_UNCOMMITTED;

        try {
            final Cursor c = DbInternal.makeCursor(
                this, locker, null /*cursorConfig*/);

            try {
                /* Position cursor on beginning key. */
                if (beginKey != null) {

                    key.setData(beginKey.getData(), beginKey.getOffset(),
                                beginKey.getSize());

                    if (c.getSearchKeyRange(key, noData, lockMode) !=
                        OperationStatus.SUCCESS) {
                        return 0;
                    }

                    if (!beginInclusive && key.equals(beginKey)) {
                        if (c.getNext(key, noData, lockMode) !=
                            OperationStatus.SUCCESS) {
                            return 0;
                        }
                    }
                } else {
                    if (c.getFirst(key, noData, lockMode) !=
                        OperationStatus.SUCCESS) {
                        return 0;
                    }
                }

                /* Create RangeConstraint for ending key. */
                RangeConstraint rangeConstraint = null; // INCOMPLETE

                /* Skip entries to get count. */
                return 1 + DbInternal.getCursorImpl(c).skip(
                    true /*forward*/, 0 /*maxCount*/, rangeConstraint);

            } finally {
                c.close();
            }
        } finally {
            locker.operationEnd(true);
        }
    }

    /*
     * Dumping
     */
    public String dumpString(int nSpaces) {
        StringBuilder sb = new StringBuilder();
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("<database id=\"" );
        sb.append(id.toString());
        sb.append("\"");
        sb.append(" deleteState=\"");
        sb.append(deleteState);
        sb.append("\"");
        sb.append(" useCount=\"");
        sb.append(useCount.get());
        sb.append("\"");
        sb.append(" dupsort=\"");
        sb.append(getSortedDuplicates());
        sb.append("\"");
        sb.append(" temporary=\"");
        sb.append(isTemporary());
        sb.append("\"");
        sb.append(" deferredWrite=\"");
        sb.append(isDurableDeferredWrite());
        sb.append("\"");
        sb.append(" keyPrefixing=\"");
        sb.append(getKeyPrefixing());
        sb.append("\"");
        if (btreeComparator != null) {
            sb.append(" btc=\"");
            sb.append(getComparatorClassName(btreeComparator,
                                             btreeComparatorBytes));
            sb.append("\"");
            sb.append(" btcPartial=\"");
            sb.append(btreePartialComparator);
            sb.append("\"");
        }
        if (duplicateComparator != null) {
            sb.append(" dupc=\"");
            sb.append(getComparatorClassName(duplicateComparator,
                                             duplicateComparatorBytes));
            sb.append("\"");
            sb.append(" dupcPartial=\"");
            sb.append(duplicatePartialComparator);
            sb.append("\"");
        }
        sb.append(">");
        if (dbFileSummaries != null) {
            for (Map.Entry<Long, DbFileSummary> entry :
                    dbFileSummaries.entrySet()) {
                Long fileNum = entry.getKey();
                DbFileSummary summary = entry.getValue();
                sb.append("<file file=\"").append(fileNum);
                sb.append("\">");
                sb.append(summary);
                sb.append("/file>");
            }
        }
        sb.append("</database>");
        return sb.toString();
    }

    /*
     * Logging support
     */

    /**
     * This log entry type is configured to perform marshaling (getLogSize and
     * writeToLog) under the write log mutex.  Otherwise, the size could change
     * in between calls to these two methods as the result of utilizaton
     * tracking.
     *
     * @see Loggable#getLogSize
     */
    @Override
    public int getLogSize() {

        int nameSize =
            !id.equals(DbTree.ID_DB_ID) &&
            !id.equals(DbTree.NAME_DB_ID) ?
                LogUtils.getStringLogSize(name) : 0;

        int size =
            id.getLogSize() +
            nameSize +
            tree.getLogSize() +
            1 + // flags, 1 byte
            LogUtils.getByteArrayLogSize(btreeComparatorBytes) +
            LogUtils.getByteArrayLogSize(duplicateComparatorBytes) +
            LogUtils.getPackedIntLogSize(maxTreeEntriesPerNode) +
            1;  // createdAtLogVersion

        size += TriggerUtils.logSize(triggerBytes);
        return size;
    }

    /**
     * @see Loggable#writeToLog
     */
    @Override
    public void writeToLog(ByteBuffer logBuffer) {

        id.writeToLog(logBuffer);

        if (!id.equals(DbTree.ID_DB_ID) &&
            !id.equals(DbTree.NAME_DB_ID)) {
            LogUtils.writeString(logBuffer, name);
        }

        tree.writeToLog(logBuffer);

        logBuffer.put(flags);

        LogUtils.writeByteArray(logBuffer, btreeComparatorBytes);
        LogUtils.writeByteArray(logBuffer, duplicateComparatorBytes);

        LogUtils.writePackedInt(logBuffer, maxTreeEntriesPerNode);

        logBuffer.put(createdAtLogVersion);

        TriggerUtils.writeTriggers(logBuffer, triggerBytes);

        dirty = false;
    }

    /**
     * @see Loggable#readFromLog
     */
    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {

        boolean version6OrLater = (entryVersion >= 6);

        id.readFromLog(itemBuffer, entryVersion);

        if (id.equals(DbTree.ID_DB_ID)) {
            setNameAndType(DbType.ID.getInternalName());
        } else if (id.equals(DbTree.NAME_DB_ID)) {
            setNameAndType(DbType.NAME.getInternalName());
        } else {
            if (entryVersion >= 16) {
                setNameAndType(
                    LogUtils.readString(itemBuffer, false, entryVersion));
            } else {
                /* Recovery will call setNameAndType. */
                name = null;
            }
        }

        tree.readFromLog(itemBuffer, entryVersion);

        /*
         * Versions < 6 have the duplicatesAllowed boolean rather than a flags
         * byte here, but we don't need a special case because the old boolean
         * value is 1 and replacement flag value is 1.
         */
        flags = itemBuffer.get();

        if (forceKeyPrefixing) {
            setKeyPrefixing();
        }

        if (entryVersion >= 2) {
            btreeComparatorBytes =
                LogUtils.readByteArray(itemBuffer, !version6OrLater);
            duplicateComparatorBytes =
                LogUtils.readByteArray(itemBuffer, !version6OrLater);
        } else {
            String btreeClassName = LogUtils.readString
                (itemBuffer, !version6OrLater, entryVersion);
            String dupClassName = LogUtils.readString
                (itemBuffer, !version6OrLater, entryVersion);
            if (btreeClassName.length() == 0) {
                btreeComparatorBytes = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
            } else {
                btreeComparatorBytes =
                    objectToBytes(btreeClassName, "BtreeComparator");
            }
            if (dupClassName.length() == 0) {
                duplicateComparatorBytes = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
            } else {
                duplicateComparatorBytes =
                    objectToBytes(dupClassName, "DuplicateComparator");
            }
        }

        if (entryVersion >= 1) {
            maxTreeEntriesPerNode =
                LogUtils.readInt(itemBuffer, !version6OrLater);
            if (entryVersion < 8) {
                /* Discard maxDupTreeEntriesPerNode. */
                LogUtils.readInt(itemBuffer, !version6OrLater);
            }
        }

        if (version6OrLater) {
            createdAtLogVersion = itemBuffer.get();
        }

        if (version6OrLater && entryVersion < 16) {
            dbFileSummaries = new DbFileSummaryMap();
            int nFiles = LogUtils.readPackedInt(itemBuffer);
            for (int i = 0; i < nFiles; i += 1) {
                long fileNum = LogUtils.readPackedLong(itemBuffer);
                DbFileSummary summary = dbFileSummaries.get(fileNum);
                summary.readFromLog(itemBuffer, entryVersion);
            }
        }

        triggerBytes = (entryVersion < 8) ?
                    null :
                    TriggerUtils.readTriggers(itemBuffer, entryVersion);
        /* Trigger list is unmarshalled lazily by getTriggers. */
    }

    /**
     * @see Loggable#dumpLog
     */
    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<database");
        sb.append(" name=\"");
        sb.append(name);
        sb.append("\"");
        dumpFlags(sb, verbose, flags);
        sb.append(" btcmp=\"");
        sb.append(getComparatorClassName(btreeComparator,
                                         btreeComparatorBytes));
        sb.append("\"");
        sb.append(" dupcmp=\"");
        sb.append(getComparatorClassName(duplicateComparator,
                                         duplicateComparatorBytes));
        sb.append("\" > ");
        id.dumpLog(sb, verbose);
        tree.dumpLog(sb, verbose);
        if (verbose && dbFileSummaries != null) {

            for (Map.Entry<Long, DbFileSummary> entry :
                    dbFileSummaries.entrySet()) {
                Long fileNum = entry.getKey();
                DbFileSummary summary = entry.getValue();
                sb.append("<file file=\"").append(fileNum);
                sb.append("\">");
                sb.append(summary);
                sb.append("</file>");
            }
        }
        TriggerUtils.dumpTriggers(sb, triggerBytes, getTriggers());
        sb.append("</database>");
    }

    static void dumpFlags(StringBuilder sb,
                          @SuppressWarnings("unused") boolean verbose,
                          byte flags) {
        sb.append(" dupsort=\"").append((flags & DUPS_ENABLED) != 0);
        sb.append("\" replicated=\"").append((flags & IS_REPLICATED_BIT) != 0);
        sb.append("\" temp=\"").append((flags & TEMPORARY_BIT)
                                       != 0).append("\" ");
    }

    /**
     * @see Loggable#getTransactionId
     */
    @Override
    public long getTransactionId() {
        return 0;
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    @Override
    public boolean logicalEquals(@SuppressWarnings("unused") Loggable other) {
        return false;
    }

    /**
     * Used for log dumping.
     */
    private static String
        getComparatorClassName(Comparator<byte[]> comparator,
                               byte[] comparatorBytes) {

        if (comparator != null) {
            return comparator.getClass().getName();
        } else if (comparatorBytes != null &&
                   comparatorBytes.length > 0) {

            /*
             * Output something for DbPrintLog when
             * EnvironmentImpl.getNoComparators.
             */
            return "byteLen: " + comparatorBytes.length;
        } else {
            return "";
        }
    }

    /**
     * Used both to read from the log and to validate a comparator when set in
     * DatabaseConfig.
     */
    public static Comparator<byte[]>
        instantiateComparator(Class<? extends Comparator<byte[]>>
                              comparatorClass,
                              String comparatorType) {
        if (comparatorClass == null) {
            return null;
        }

        try {
            return comparatorClass.newInstance();
        } catch (Exception e) {
            throw EnvironmentFailureException.unexpectedException
                ("Exception while trying to load " + comparatorType +
                 " Comparator class.", e);
        }
    }

    /**
     * Converts a comparator object to a serialized byte array, converting to
     * a class name String object if byClassName is true.
     *
     * @throws EnvironmentFailureException if the object cannot be serialized.
     */
    public static byte[] comparatorToBytes(Comparator<byte[]> comparator,
                                           boolean byClassName,
                                           String comparatorType) {
        if (comparator == null) {
            return LogUtils.ZERO_LENGTH_BYTE_ARRAY;
        }

        final Object obj =
            byClassName ? comparator.getClass().getName() : comparator;

        return objectToBytes(obj, comparatorType);
    }

    /**
     * Converts an arbitrary object to a serialized byte array.  Assumes that
     * the object given is non-null.
     */
    public static byte[] objectToBytes(Object obj,
                                       String label) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            return baos.toByteArray();
        } catch (IOException e) {
            throw EnvironmentFailureException.unexpectedException
                ("Exception while trying to store " + label, e);
        }
    }

    /**
     * Converts an arbitrary serialized byte array to an object.  Assumes that
     * the byte array given is non-null and has a non-zero length.
     */
    public static Object bytesToObject(byte[] bytes,
                                       String label,
                                       ClassLoader loader) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ClassResolver.Stream ois = new ClassResolver.Stream(bais, loader);
            return ois.readObject();
        } catch (Exception e) {
            throw EnvironmentFailureException.unexpectedException
                ("Exception while trying to load " + label, e);
        }
    }

    public int compareEntries(DatabaseEntry entry1,
                              DatabaseEntry entry2,
                              boolean duplicates) {
        return Key.compareKeys
            (entry1.getData(), entry1.getOffset(), entry1.getSize(),
             entry2.getData(), entry2.getOffset(), entry2.getSize(),
             (duplicates ? duplicateComparator : btreeComparator));
    }

    /**
     * Utility class for converting bytes to compartor or Class.
     */
    static class ComparatorReader {

        /*
         * True if comparator type is Class,
         * false if comparator type is Comparator.
         */
        private final boolean isClass;

        /*
         * Record the Class type for this Comparator,
         * used by ReplicatedDatabaseConfig.
         */
        private final Class<? extends Comparator<byte[]>> comparatorClass;
        private final Comparator<byte[]> comparator;

        @SuppressWarnings("unchecked")
        ComparatorReader(byte[] comparatorBytes,
                         String type,
                         ClassLoader loader) {

            /* No comparator. */
            if (comparatorBytes.length == 0) {
                comparatorClass = null;
                comparator = null;
                isClass = false;
                return;
            }

            /* Deserialize String class name or Comparator instance. */
            final Object obj = bytesToObject(comparatorBytes, type, loader);

            /* Comparator is specified as a class name. */
            if (obj instanceof String) {
                final String className = (String)obj;
                try {
                    comparatorClass = (Class<? extends Comparator<byte[]>>)
                        ClassResolver.resolveClass(className, loader);
                } catch (ClassNotFoundException ee) {
                    throw EnvironmentFailureException.
                        unexpectedException(ee);
                }
                comparator = instantiateComparator(comparatorClass, type);
                isClass = true;
                return;
            }

            /* Comparator is specified as an instance. */
            if (obj instanceof Comparator) {
                comparatorClass = null;
                comparator = (Comparator<byte[]>) obj;
                isClass = false;
                return;
            }

            /* Should never happen. */
            throw EnvironmentFailureException.unexpectedState
                ("Expected class name or Comparator instance, got: " +
                 obj.getClass().getName());
        }

        public boolean isClass() {
            return isClass;
        }

        Class<? extends Comparator<byte[]>> getComparatorClass() {
            return comparatorClass;
        }

        public Comparator<byte[]> getComparator() {
            return comparator;
        }
    }

    public int getBinDeltaPercent() {
        return binDeltaPercent;
    }

    /**
     * Return a ReplicationContext that will indicate if this operation
     * should broadcast data records for this database as part the replication
     * stream.
     */
    public ReplicationContext getRepContext() {

        /*
         * It's sufficient to base the decision on what to return solely on the
         * isReplicated() value. We're guaranteed that the environment is
         * currently opened w/replication. That's because we refuse to open
         * rep'ed environments in standalone mode and we couldn't have created
         * this db w/replication specified in a standalone environment.
         *
         * We also don't have to check if this is a client or master. If this
         * method is called, we're executing a write operation that was
         * instigated an API call on this node (as opposed to a write operation
         * that was instigated by an incoming replication message). We enforce
         * elsewhere that write operations are only conducted by the master.
         *
         * Writes provoked by incoming replication messages are executed
         * through the putReplicatedLN and deleteReplicatedLN methods.
         */
        return isReplicated() ?
               ReplicationContext.MASTER :
               ReplicationContext.NO_REPLICATE;
    }

    /**
     * Return a ReplicationContext that includes information on how to
     * logically replicate database operations. This kind of replication
     * context must be used for any api call which logging a NameLN for that
     * represents a database operation. However, NameLNs which are logged for
     * other reasons, such as cleaner migration, don't need this special
     * replication context.
     */
    DbOpReplicationContext
        getOperationRepContext(DbOperationType operationType,
                               DatabaseId oldDbId) {

        /*
         * If this method is called, we're executing a write operation that was
         * instigated by an API call on this node (as opposed to a write
         * operation that was instigated by an incoming replication
         * message). We enforce elsewhere that write operations are only
         * conducted by the master.
         */
        DbOpReplicationContext context =
            new DbOpReplicationContext(isReplicated(), operationType);

        if (DbOperationType.isWriteConfigType(operationType)) {
            assert(oldDbId == null);
            context.setCreateConfig
                (new ReplicatedDatabaseConfig(flags,
                                              maxTreeEntriesPerNode,
                                              btreeComparatorBytes,
                                              duplicateComparatorBytes,
                                              triggerBytes));
        } else if (operationType == DbOperationType.TRUNCATE) {
            assert(oldDbId != null);
            context.setTruncateOldDbId(oldDbId);
        }
        return context;
    }

    /**
     * Convenience overloading.
     *
     * @see #getOperationRepContext(DbOperationType, DatabaseId)
     */
    DbOpReplicationContext
        getOperationRepContext(DbOperationType operationType) {

        assert(operationType != DbOperationType.TRUNCATE);
        return getOperationRepContext(operationType, null);
    }
}
