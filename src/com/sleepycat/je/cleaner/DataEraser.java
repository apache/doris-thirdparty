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
package com.sleepycat.je.cleaner;

import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.EXTINCT;
import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.MAYBE_EXTINCT;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.bind.tuple.TupleBase;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.ExtinctionFilter;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MetadataStore;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.FileReader;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.ErasedLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.StringStat;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TracerFormatter;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.utilint.FormatUtil;

/**
 * Erases obsolete data from disk during a configured interval. [#26954]
 *
 * Basics
 * ======
 * - The purpose of the erasure feature is to erase obsolete user data within
 *   some reasonable period after becoming obsolete, and to do this without
 *   adding significant overhead that might impact application performance.
 *
 * - There are no strict guarantees about when erasure is complete. This
 *   means it is best to think of erasure as a "best effort". Some examples
 *   where data is not always erased according to the erasure period are:
 *
 *   1. If a node is down for an extended time, obviously no erasure will
 *      occur until it comes back up. After it comes up, there may not be
 *      enough time left in the cycle for erasure to be completed. The rate
 *      of erasure is not increased in this situation. The erasure rate is
 *      kept constant throughout each cycle to avoid resource usage spikes.
 *
 *   2. Data at the tail end of the data log will not be erased because it is
 *      needed for recovery and replication. In this case, data will be
 *      eventually erased as more writing eventually occurs.
 *
 *   3. UINs (upper internal nodes in the Btree) are not erased, and
 *      therefore keys in UINs, which can be considered user data, are not
 *      erased. Erasing UINs would be very difficult because ancestor slot
 *      keys would have to be adjusted and zero'ing the entire log entry
 *      would not be possible. For now this is simply a limitation due to
 *      operational considerations.
 *
 *   4. From a performance viewpoint, erasure is designed so that each erasure
 *      cycle will be completed over a multi-day period. If the erasure period
 *      (see below) is set to a small value for testing, the cycle may not
 *      be completed and this is acceptable behavior. The minimum time
 *      needed to complete the cycle is dependent on the total size of the
 *      files to be erased in the cycle, the load on the system, and other
 *      factors.
 *
 * - Currently the erasure feature is hidden in the JE API. It may be exposed
 *   in the future.
 *
 * - The eraser overall may be enabled or disabled using the following param.
 *   It is disabled by default.
 *
 *     je.env.runEraser -- boolean, default: false, mutable: yes
 *
 * - Currently only erasure of data in deleted DBs (removed or truncated) and
 *   erasure of extinct records are supported. These are enabled via the
 *   following two params. These are both false by default.
 *
 *     je.erase.deletedDatabases -- boolean, default: false, mutable: yes
 *     je.erase.extinctRecords -- boolean, default: false, mutable: yes
 *
 *   By setting both of these parameters to true in NoSQL DB, obsolete data
 *   will be removed for dropped tables and dropped indexes.
 *
 * - In the future we may add the ability to erase expired data (via TTL) and
 *   data that has been updated or deleted. This will be more expensive and
 *   complex, and will probably be enabled via additional config params.
 *
 * - During each N day cycle, we erase all files that were N days old (or
 *   older) at the start of the cycle. Therefore, the first erasure for a file
 *   occurs when it is between N and N*2 days old. Thereafter, the file is
 *   erased every N days. The erasure period is set using the following param,
 *   which is zero by default; the period must be explicitly set to perform
 *   erasure.
 *
 *     je.erase.period -- duration, default: 0, min: 0, max: none, mutable: yes
 *
 * - Here's an example for how the erasure period works in practice.
 *
 *     Let's say the erasure period is 30 min. At the start of each 30 min
 *     erasure cycle, the data files older than 30 min are selected. Those
 *     files should normally be erased by the end of the cycle. Then a new
 *     cycle starts and a new set of files older than 30 minutes is
 *     selected.
 *
 *     Let's assume that writing is continuous and the checkpoint interval
 *     is fairly small, and therefore we won't run into the problem that
 *     data at the end of the log cannot be erased. (This problem is
 *     described in more detail in a later section.)
 *
 *     Then at any time, roughly speaking, all data that became obsolete more
 *     than 60 minutes ago should be erased. The erasure period is meant to be
 *     half of the desired interval after which obsolete data should disappear.
 *
 *     The erasure doesn't have to be complete exactly at the end of each
 *     erasure period. In production we expect the erasure period to be set to
 *     less than half of the desired interval to allow for impreciseness,
 *     problems, etc. For example, if the desired interval is 60 days in
 *     production, the erasure period would normally be set to around 25 days.
 *
 *  - Here's an example of how params could be set to ensure that obsolete
 *    data is removed within 60 days of becoming obsolete:
 *
 *      je.env.runEraser=true
 *      je.erase.deletedDatabases=true
 *      je.erase.extinctRecords=true
 *      je.erase.period=25 days
 *
 * - WARNING: Erasure cannot be used for applications that rely on the usual
 *   immutable nature of JE data files. For example, if DbBackup is used to
 *   create a snapshot using file links, the assumption that the files will
 *   not be modified by JE no longer holds when erasure is enabled. For
 *   example, NoSQL DB snapshots (which are just links to data files) are
 *   invalidated by data erasure. To perform a valid backup, NoSQL DB must
 *   actually copy the files while they're protected by DbBackup (between
 *   the calls to startBackup and endBackup).
 *
 * Trace Logging
 * =============
 * Trace logging is performed for the following erasure cycle events. All
 * messages are INFO-level messages except when stated otherwise.
 *
 *  - A fresh cycle starts. This occurs at startup, or after the prior
 *    cycle finishes and its end time passes, or when the prior cycle is
 *    aborted because its end time has passed.
 *
 *  - A cycle finishes because erasure of all files is complete. The
 *    message is logged immediately after all files have been erased, even
 *    if the cycle end time has not yet passed. The eraser will then be
 *    idle until the cycle end time passes.
 *
 *  - A cycle aborts because its end time has passed but is incomplete
 *    because not all files were erased. A fresh cycle is then started.
 *    This is a WARNING-level message when files cannot be erased because
 *    they are protected for reasons that are expected to be short lived
 *    (backup, replication) but have persisted until the end of the cycle.
 *    In other situations this is a INFO-level message, for example:
 *    - when the files are needed for recovery,
 *    - when the files are needed to retain at least 1000 VLSNs (or
 *      {@link com.sleepycat.je.rep.impl.RepParams#MIN_VLSN_INDEX_SIZE}),
 *    - when the files do not contain any VLSNs and therefore the VLSNIndex
 *      cannot be truncated at any specific point (this is a corner case).
 *
 *  - A cycle is suspended because the JE Environment is closed and not
 *    all files were erased. The cycle will be resumed at the next
 *    Environment open, if the cycle end time has not passed.
 *
 *  - An incomplete cycle is resumed at startup. The cycle was incomplete
 *    at the last Environment close, and its end time has not yet passed.
 *
 *  - An incomplete cycle cannot be resumed at startup because its end
 *    time has now passed. A fresh cycle is then started.
 *
 * All messages start with ERASER and include the cycle start/end times, the
 * files processed, and the files that are yet to be processed.
 *
 * Stats
 * =====
 * Stat Group: Eraser
 *
 * eraserCycleStart - Erasure cycle start time (UTC).
 * eraserCycleEnd - Erasure cycle end time (UTC).
 * eraserFilesRemaining - Number of files still to be processed in erasure
 *                        cycle.
 * eraserFilesErased - Number of files erased by overwriting obsolete entries.
 * eraserFilesDeleted - Number of reserved files deleted by the eraser.
 * eraserFilesAlreadyDeleted - Number of reserved files deleted coincidentally
 *                             by the cleaner.
 * eraserFSyncs - Number of fsyncs performed by the eraser.
 * eraserReads - Number of file reads performed by the eraser.
 * eraserReadBytes - Number of bytes read by the eraser.
 * eraserWrites - Number of file writes performed by the eraser.
 * eraserWriteBytes - Number of bytes written by the eraser.
 *
 * Erasing data at the end of log
 * ==============================
 * When writing stops, the user data at the tail end of the log may need
 * erasure at some point. This would happen if a table is dropped and then
 * there is no writing for a long time (or very little writing). This is a
 * corner case and not one we expect in production, but it can happen in
 * production and it certainly happens in tests.
 *
 * Currently JE does not allow cleaning of any file in the recovery interval.
 * This is to ensure that recovery works, of course. The same restriction
 * probably applies to erasure. If we were to treat any slot referencing an
 * erased LSN as if the slot were deleted, we might get recovery to work.
 * But this would be complex to analyze and test thoroughly, especially for IN
 * replay. Therefore if we need to erase items in the recovery interval, we
 * would need to detect this situation and force a checkpoint before erasing.
 *
 * In addition, to erase all entries at the end of the log means that the
 * VLSNIndex would need to be completely truncated, i.e., made empty. This
 * means the node could not be used as a feeder when functioning as a master,
 * and could not perform syncup when functioning as a replica. Rather than
 * emptying the VLSNIndex completely we could log a benign replicated entry
 * so there is at least one entry. But that doesn't address the broader
 * problem of syncup. Perhaps this doesn't matter when writing has stopped
 * for an extended period because the replicas will be up-to-date. And
 * perhaps network restore is fine for other unusual cases. But it is a risk.
 * Right now we always leave at least 1,000 VLSNs in the VLSNIndex to guard
 * against problems, but we don't really know what problems might arise. So
 * it would take some work to figure this out and test it.
 *
 * Therefore, we simply do not erase obsolete data when it appears at the tail
 * end of the log. One way to explain this is to say that we can't erase
 * data at the very end of the transaction log, because this would prevent
 * recovery in certain situations. We do log a warning message in this
 * situation.
 *
 * Aborting Erasure
 * ================
 * Erasure of a file is aborted in the following cases.
 *
 * - The file is needed for a backup or network restore. In both cases, it is
 *   DbBackup.startBackup that aborts the erasure. The file aborted will then
 *   be protected and won't be selected again for erasure until the backup or
 *   network restore is finished.
 *
 * - The extinction filter returns EXTINCT_MAYBE. This is due to a temporary
 *   situation at startup time, while NoSQL DB (or another app) has not yet
 *   initialized its metadata (table metadata in the case of NoSQL DB). It
 *   will be retried repeatedly until this no longer occurs.
 *
 *     Discarded idea: We could add a way to know that the filter is fully
 *     initialized and the eraser thread could delay starting until then. But
 *     we would still have to abort if MAYBE_EXTINCT is returned after that
 *     point. So for simplicity we just do the abort.
 *
 * Reserved Files, VLSNIndex, Cleaning
 * ===================================
 * - Reserved files are an exception. Because an erased file cannot be used for
 *   replication, reserved files are deleted rather than erasing them.
 *   Therefore, reserved files are never older than N*2 days.
 *
 * - Before erasing a file covered by the VLSNIndex, we truncate the
 *   VLSNIndex to remove the file from its range. Because the VLSNIndex range
 *   never retreats (only advances), files protected by the VLSNIndex will
 *   never have been erased.
 *
 * - However, other files may be erased and subsequently become protected. This
 *   is OK, because such protection only needs to guarantee that files are not
 *   changed while they are protected. These include:
 *    - Backup and network restore.
 *    - DiskOrderedCursor and Database.count.
 *
 * - Cleaning is not coordinated with erasure (except for the treatment of
 *   reserved files discussed above). A given file may be erased and cleaned
 *   concurrently, and this should not cause problems. This is a waste of
 *   resources, but since it is unlikely we do not try to detect it or
 *   optimize for it. Such coordination would add a lot of complexity.
 *
 * Throttling
 * ==========
 * Throttling is performed by estimating the total amount of work in the
 * cycle at the beginning of the cycle, dividing up the total cycle time
 * by this work amount, and throttling (waiting) at various points in
 * order to spread the work out fairly evenly over the cycle period.
 * The {@link WorkThrottle} class calculates the wait time based on work
 * done so far.
 *
 * It is possible that we cannot complete the work within the cycle period,
 * for example, because a node is down for an extended period. In such
 * cases we do _not_ attempt to catch up by speeding up the rate of work,
 * since this could cause performance spikes. Instead we intentionally
 * overestimate the amount of work, leaving spare time to account for such
 * problems. In the end, if erasure of all selected files cannot be
 * completed by the end of the cycle, the cycle will be aborted and a new
 * cycle is started with a recalculated set of files. This is acceptable
 * behavior in unusual conditions.
 *
 *   Note: Such problems could be addressed differently by integrating
 *   with the TaskCoordinator. In that case we would use a different
 *   approach entirely: we would simply work at the maximum rate allowed by
 *   the TaskCoordinator. But for this to work well, other JE components would
 *   also need to be integrated with the TaskCoordinator. So for now we simply
 *   perform work at a fixed rate within each cycle.
 *
 * Before the cycle starts we have to open each file to get its creation
 * time, which has a cost. Throttling for that one time task is performed
 * separately in {@link #startCycle}. The remaining time in the cycle is also
 * allocated by {@link #startCycle}, which initializes {@link #cycleThrottle}
 * and related fields. Work is divided as described below.
 *
 * For each file to be erased there are several components of work:
 *
 * 1. We may have to read file, or parts of files, for two reasons that
 *    are in addition to the file erasure process itself:
 *
 *    a. We may have to read the file to find its last VLSN, to determine
 *       where to truncate the VLSNIndex. In the worst case scenario we have
 *       to do this for every file, but it is much more likely that it will
 *       only have to be done for a small fraction of the files, and only a
 *       fraction (the end) of each file will normally be read. In addition,
 *       each time we truncate the VLSNIndex we perform an fsync; however,
 *       in the normal case we do this only once per cycle.
 *
 *    b. We read a file redundantly when erasure of a file is aborted and
 *       restarted later. See Aborting Erasure above.
 *
 * 2. Read through the file and overwrite the type byte for each entry
 *    that should be erased. We know the length of each file the read cost
 *    is known. We don't know how many erasures will take place, but the
 *    worst case is that every entry is erased.
 *
 *    Note that reserved files are simply deleted rather than erasing them
 *    and this is cheaper than erasure. So when there are reserved files,
 *    we will overestimate the amount of work.
 *
 * 3. Before any overwriting, at the time we determine that at least one entry
 *    must be erased, touch the file and perform an fsync to ensure that the
 *    lastModifiedTime is updated persistently. The fsync is assumed to be
 *    expensive.
 *
 * 4. After overwriting all type bytes, perform a second fsync to make the
 *    type changes persistent. The fsync is assumed to be expensive.
 *
 * 5. Overwrite the item in each entry that was erased. This cost is
 *    unknown, but the worst case is that every entry is erased.
 *
 * 6. Perform the third and final fsync to make the erasure persistent. The
 *    fsync is assumed to be expensive.
 *
 * Work units are defined as follows.
 *
 * - For component 1 we assign a work unit to each byte read (the file
 *   length). This is a very large overestimate and is intended to
 *   account for processing delays, such as when a node is down.
 *
 * - For component 2 we also assign a work unit to each byte read (the file
 *   length). The overwrite of type bytes is variable and included in this
 *   cost for simplicity.
 *
 * - For components 3, 4, 5 and 6 together we also assign the file length as
 *   a very rough estimate, and this work is divided between these components
 *   as follows:
 *     18% for step 3
 *     16% for step 4
 *     50% for step 5
 *     16% for step 6
 *
 * Therefore the total amount of work is simply three times the length of the
 * files to be erased.
 *
 * Other
 * =====
 * - To support network restore, before erasing a file we remove its cached
 *   info (which includes a checksum) from the response cache in LogFileFeeder.
 *   See {@link
 *   com.sleepycat.je.rep.impl.RepImpl#clearedCachedFileChecksum(String)}.
 *
 * - Prohibiting erasure of protected files prevents changes to the file while
 *   it is being read by the protecting entity. Even so, if we read from the
 *   log buffer cache or tip cache, we could also get different results than
 *   reading directly from the file. To be safe, erasure could clear any
 *   cached data for the file. But is this necessary? No, because reading
 *   from the tip cache and log buffer cache is done only by feeders, and
 *   the files read by feeders are protected from erasure.
 *
 * - BtreeVerifier, when configured to read LNs, checks for LSN references to
 *   erased entries since it simply reads the LNs via a cursor. If the LN has
 *   been erased and it it is not extinct, the environment is invalidated as
 *   usual.
 *
 * - The checksum for the LOG_ERASED type cannot be verified, and its
 *   {@link LogEntryHeader#hasChecksum()} method will return false. Because
 *   an entry may be erased in the middle of checksum calculation, the header
 *   may have to be re-read from disk in rare cases. See
 *   {@link com.sleepycat.je.log.ChecksumValidator#validate(long, long)}.
 *
 * - The LOG_ERASED type is not counted as an LN or IN, which could throw off
 *   utilization counts. This may only impact tests and debugging. A thorough
 *   analysis of this issue has not been performed.
 */
public class DataEraser extends StoppableThread implements EnvConfigObserver {

    private static final int MAX_FILE_INFO_MS = 1000;
    private static final int MIN_WORK_DELAY_MS = 5;
    private static final int MAX_SLEEP_MS = 100;
    private static final int FOREVER_TIMEOUT_MS = 5 * 60 * 1000;
    private static final int NO_UNPROTECTED_FILES_DELAY_MS = 1000;
    private static final String TEST_ERASE_PERIOD = "test.erasePeriod";
    private static TestHook<TestEvent> testEventHook;

    private static final DateFormat DATE_FORMAT =
        TracerFormatter.makeDateFormat();

    private static final int WRITE_WORK_PCT = 50;
    private static final int FSYNC1_WORK_PCT = 18;
    private static final int FSYNC2_WORK_PCT = 16;
    private static final int FSYNC3_WORK_PCT = 16;

    private final Cleaner cleaner;
    private final FileProtector fileProtector;
    private final FileManager fileManager;
    private final Logger logger;
    private volatile boolean shutdownRequested = false;
    private volatile boolean enabled = false;
    private int terminateMillis;
    private volatile long cycleMs = 0;
    private boolean eraseDeletedDbs = false;
    private boolean eraseExtinctRecords = false;
    private int pollCheckMs;
    private long[] eraseOffsets = new long[5 * 1024];
    private int[] eraseSizes = new int[eraseOffsets.length];
    private byte[] zeros = new byte[10 * 1024];
    private final Object pollMutex = new Object();
    private final NavigableMap<Long, FileInfo> fileInfoCache = new TreeMap<>();
    private WorkThrottle cycleThrottle;
    private long totalCycleWork;
    private String lastProtectedFilesMsg;
    private Level lastProtectedFilesMsgLevel;

    /**
     * The eraser is single-threaded but there is occasional multi-threaded
     * access to the following fields due to stat loading.
     */
    private volatile long startTime;
    private volatile long endTime;
    private volatile long completionTime;
    private NavigableSet<Long> filesRemaining = Collections.emptyNavigableSet();
    private NavigableSet<Long> filesCompleted = Collections.emptyNavigableSet();
    private final AtomicInteger filesErased = new AtomicInteger();
    private final AtomicInteger filesDeleted = new AtomicInteger();
    private final AtomicInteger filesAlreadyDeleted = new AtomicInteger();
    private final AtomicInteger fSyncs = new AtomicInteger();
    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();
    private final AtomicLong writeBytes = new AtomicLong();

    /**
     * currentFileMutex protects currentFile and abortCurrentFile.
     */
    private final Object currentFileMutex = new Object();
    private volatile Long currentFile;
    private boolean abortCurrentFile;
    private int abortTimeoutMs;

    public DataEraser(final EnvironmentImpl envImpl) {

        super(envImpl, "JEErasure");
        cleaner = envImpl.getCleaner();
        fileProtector = envImpl.getFileProtector();
        fileManager = envImpl.getFileManager();
        logger = LoggerUtils.getLogger(getClass());

        envConfigUpdate(envImpl.getConfigManager(), null);
        envImpl.addConfigObserver(this);
    }

    @Override
    public void envConfigUpdate(
        final DbConfigManager configManager,
        final EnvironmentMutableConfig ignore) {

        /*
         * If the TEST_ERASE_PERIOD system property is specified and
         * ENV_RUN_ERASER is not specified, enable erasure and use the test
         * period.
         */
        final boolean runErase;
        final String testErasePeriod = System.getProperty(TEST_ERASE_PERIOD);

        if (testErasePeriod != null &&
            !configManager.isSpecified(EnvironmentParams.ENV_RUN_ERASER)) {

            runErase = true;
            cycleMs = PropUtil.parseDuration(testErasePeriod);
            eraseDeletedDbs = true;
            eraseExtinctRecords = true;
        } else {

            runErase = configManager.getBoolean(
                EnvironmentParams.ENV_RUN_ERASER);

            cycleMs = configManager.getDuration(
                EnvironmentParams.ERASE_PERIOD);

            eraseDeletedDbs = configManager.getBoolean(
                EnvironmentParams.ERASE_DELETED_DATABASES);

            eraseExtinctRecords = configManager.getBoolean(
                EnvironmentParams.ERASE_EXTINCT_RECORDS);
        }

        enabled = runErase && cycleMs > 0 &&
            (eraseDeletedDbs || eraseExtinctRecords);

        terminateMillis = configManager.getDuration(
            EnvironmentParams.EVICTOR_TERMINATE_TIMEOUT);

        pollCheckMs = Math.min(MAX_SLEEP_MS, terminateMillis / 4);

        abortTimeoutMs = configManager.getDuration(
            EnvironmentParams.ERASE_ABORT_TIMEOUT);
    }

    public StatGroup loadStats(StatsConfig config) {

        final StatGroup statGroup = new StatGroup(
            EraserStatDefinition.GROUP_NAME, EraserStatDefinition.GROUP_DESC);

        /* Add CUMULATIVE stats. */

        final DateFormat dateFormat = TracerFormatter.makeDateFormat();

        new StringStat(
            statGroup, EraserStatDefinition.ERASER_CYCLE_START,
            (startTime == 0) ? "" : dateFormat.format(new Date(startTime)));

        new StringStat(
            statGroup, EraserStatDefinition.ERASER_CYCLE_END,
            (endTime == 0) ? "" : dateFormat.format(new Date(endTime)));

        new IntStat(
            statGroup, EraserStatDefinition.ERASER_FILES_REMAINING,
            filesRemaining.size());

        /* INCREMENTAL stats must be cleared. */

        final boolean clear = config.getClear();

        new IntStat(
            statGroup, EraserStatDefinition.ERASER_FILES_ERASED,
            getStat(filesErased, clear));

        new IntStat(
            statGroup, EraserStatDefinition.ERASER_FILES_DELETED,
            getStat(filesDeleted, clear));

        new IntStat(
            statGroup, EraserStatDefinition.ERASER_FILES_ALREADY_DELETED,
            getStat(filesAlreadyDeleted, clear));

        new IntStat(
            statGroup, EraserStatDefinition.ERASER_FSYNCS,
            getStat(fSyncs, clear));

        new LongStat(
            statGroup, EraserStatDefinition.ERASER_READS,
            getStat(reads, clear));

        new LongStat(
            statGroup, EraserStatDefinition.ERASER_READ_BYTES,
            getStat(readBytes, clear));

        new LongStat(
            statGroup, EraserStatDefinition.ERASER_WRITES,
            getStat(writes, clear));

        new LongStat(
            statGroup, EraserStatDefinition.ERASER_WRITE_BYTES,
            getStat(writeBytes, clear));

        return statGroup;
    }

    private long getStat(final AtomicLong val, final boolean clear) {
        return clear ? val.getAndSet(0) : val.get();
    }

    private int getStat(final AtomicInteger val, final boolean clear) {
        return clear ? val.getAndSet(0) : val.get();
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public int initiateSoftShutdown() {
        shutdownRequested = true;
        synchronized (pollMutex) {
            pollMutex.notify();
        }
        return terminateMillis;
    }

    public void startThread() {

        if (enabled &&
            !envImpl.isMemOnly() &&
            !envImpl.isReadOnly() &&
            !isAlive()) {

            /*
             * Ensure metadata DB is opened before Environment ctor finishes,
             * to avoid timing problems in tests that are sensitive to the
             * number of open DBs.
             */
            envImpl.getMetadataStore().openDb();

            start();
        }
    }

    @Override
    public void run() {
        boolean isInitialized = false;
        boolean isStarted = false;

        /*
         * Internal exceptions are used to signal state changes, and they are
         * handled in this method. Other than ShutdownRequestedException, all
         * such exceptions must be handled inside the while loop.
         */
        try {
            while (true) {
                checkShutdown();
                try {
                    if (!isInitialized) {
                        isInitialized = true;
                        if (resumeCycle()) {
                            isStarted = true;
                        }
                    }
                    if (!isStarted) {
                        startCycle();
                        isStarted = true;
                    }
                    if (checkForCycleEnd()) {
                        waitForEnabled();
                        isStarted = false;
                        continue;
                    }
                    final Long file = getNextFile();
                    if (file != null) {
                        try {
                            eraseFile(file);
                            filesCompleted.add(file);
                            filesRemaining.remove(file);
                            storeState();
                        } finally {
                            clearCurrentFile();
                        }
                    } else {
                        waitForCycleEnd();
                        waitForEnabled();
                        isStarted = false;
                    }
                } catch (NoUnprotectedFilesException e) {
                    waitForUnprotectedFiles(e);
                } catch (ErasureDisabledException e) {
                    waitForEnabled();
                } catch (PeriodChangedException e) {
                    logPeriodChanged();
                    isStarted = false;
                } catch (AbortCurrentFileException e) {
                    /* Continue. */
                }
            }
        } catch (ShutdownRequestedException e) {
            if (!filesRemaining.isEmpty()) {
                logCycleSuspend();
            }
        }
    }

    /**
     * If we can load the last stored state and the endTime of the last cycle
     * has not yet arrived, resume execution and return true.
     * Otherwise, return false.
     */
    private boolean resumeCycle() {

        if (!loadState()) {
            return false;
        }

        if (System.currentTimeMillis() >= endTime) {
            if (!filesRemaining.isEmpty()) {
                logCycleCannotResume();
            }
            return false;
        }

        logCycleResume();

        final WorkThrottle throttle =
            createFileInfoThrottle(filesRemaining.size(), cycleMs);

        /* Populate file info cache for files remaining in cycle. */
        for (final Long file : filesRemaining) {

            fileInfoCache.put(
                file,
                new FileInfo(getFileCreationTime(file), getFileLength(file)));

            throttle.throttle(1);
        }

        cycleThrottle = new WorkThrottle(totalCycleWork, endTime - startTime);
        return true;
    }

    /**
     * Resets per-cycle info, including determining the set of files to be
     * erased in the next cycle.
     */
    private void startCycle() {

        /* Ignore config changes during calculations. */
        final long localCycleMs = cycleMs;

        startTime = System.currentTimeMillis();
        endTime = startTime + localCycleMs;
        final long fileAgeCutoff = startTime - localCycleMs;
        filesCompleted = Collections.synchronizedNavigableSet(new TreeSet<>());
        filesRemaining = Collections.synchronizedNavigableSet(new TreeSet<>());
        completionTime = 0;

        final NavigableSet<Long> allFiles =
            fileProtector.getAllCompletedFiles();

        final WorkThrottle throttle =
            createFileInfoThrottle(allFiles.size(), localCycleMs);

        logCycleInit(allFiles.size());

        /*
         * Iterate through all files except for the last file. We can't erase
         * the last file anyway, because it is in the recovery interval.
         */
        for (final Long file : allFiles) {

            /*
             * Trying using cached file info from previous cycles. A file in a
             * previous cycle will normally have a creationTime that qualifies,
             * but we check again in case the erasure period has changed.
             */
            final FileInfo prevInfo = fileInfoCache.get(file);
            if (prevInfo != null) {
                if (prevInfo.creationTime <= fileAgeCutoff) {
                    filesRemaining.add(file);
                }
                continue;
            }

            final long creationTime = getFileCreationTime(file);

            throttle.throttle(1);

            if (creationTime > fileAgeCutoff) {
                continue;
            }

            filesRemaining.add(file);

            fileInfoCache.put(
                file, new FileInfo(creationTime, getFileLength(file)));
        }

        /*
         * Prevent the file cache from growing without bounds. The cache need
         * only contain an entry for all filesToErase.
         */
        fileInfoCache.navigableKeySet().retainAll(filesRemaining);

        /*
         * Use three times the sum of the file lengths as the work for the
         * remaining time in the cycle, as described in the class comments.
         */
        totalCycleWork = filesRemaining.stream()
            .mapToLong(file -> fileInfoCache.get(file).length)
            .sum() * 3;

        cycleThrottle = new WorkThrottle(totalCycleWork, endTime - startTime);
        logCycleStart();
    }

    /**
     * Creates a throttle for reading file info when a cycle is started or
     * restored.
     *
     * Use only a small portion of the cycle for collecting file info.
     * Use at most 0.1% of the cycle time and at most MAX_FILE_INFO_MS
     * per file. The idea is just to do a reasonable amount of throttling
     * between calls to getFileCreationTime.
     */
    private WorkThrottle createFileInfoThrottle(final int files,
                                                final long localCycleMs) {
        final long workTime = Math.min(
            localCycleMs / 1000,
            files * MAX_FILE_INFO_MS);

        return new WorkThrottle(files, workTime);
    }

    /**
     * Returns the file creation time by opening the file and reading its
     * header entry.
     *
     * @return creation time, or Long.MAX_VALUE if the file does not exist.
     */
    private long getFileCreationTime(final long file) {
        try {
            return fileManager.getFileHeaderTimestamp(file).getTime();

        } catch (ChecksumException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_CHECKSUM,
                "Exception erasing file 0x" + Long.toHexString(file),
                e);

        } catch (FileNotFoundException e) {
            /* File was deleted by the cleaner. */
            return Long.MAX_VALUE;
        }
    }

    private long getFileLength(final long file) {
        return new File(fileManager.getFullFileName(file)).length();
    }

    /**
     * Return the next unprotected file to be processed.
     *
     * <p>First return files that are unprotected, to defer VLSNIndex
     * truncation until it is necessary.</p>
     *
     * <p>If all files are protected, truncate the VLSNIndex and try again.
     * This is the same approach used by {@link Cleaner#manageDiskUsage}.</p>
     *
     * <p>A WARNING level is specified in the NoUnprotectedFilesException
     * when files are protected for reasons that are expected to be short
     * lived (backup, replication). The WARNING is actually logged only if
     * these reasons persist until the end of the erasure cycle.</p>
     *
     * @return the next file to be processed, or null if all files have been
     * processed.
     *
     * @throws NoUnprotectedFilesException if all remaining files are
     * protected.
     */
    private Long getNextFile() {

        lastProtectedFilesMsg = null;
        lastProtectedFilesMsgLevel = null;

        if (filesRemaining.isEmpty()) {
            return null;
        }

        Long file = getNextUnprotectedFile();
        if (file != null) {
            return file;
        }

        if (!cleaner.isFileDeletionEnabled()) {
            throw new NoUnprotectedFilesException(
                "Test mode prohibits VLSNIndex truncation.",
                Level.INFO);
        }

        final long lastRecoveryFile = getFirstFileInRecoveryInterval();

        if (filesRemaining.first() >= lastRecoveryFile) {
            throw new NoUnprotectedFilesException(
                "All remaining files are in the recovery interval," +
                    " lastRecoveryFile=0x" +
                    Long.toHexString(lastRecoveryFile) + ".",
                Level.INFO);
        }

        /*
         * Determine whether it is worthwhile and possible to truncate the
         * VLSNIndex.
         */
        if (!envImpl.isReplicated()) {
            /* VLSNIndex does not exist in a non-replicated env. */
            throw new NoUnprotectedFilesException(
                "Protected files are not in the recovery interval.",
                Level.WARNING);
        }

        final long vlsnIndexStartFile = fileProtector.getVLSNIndexStartFile();

        if (vlsnIndexStartFile > filesRemaining.last()) {
            throw new NoUnprotectedFilesException(
                "Protected files are not in the recovery interval and not " +
                "protected by the VLSNIndex vlsnIndexStartFile=0x" +
                    Long.toHexString(vlsnIndexStartFile) + ".",
                Level.WARNING);
        }

        final Pair<VLSN, Long> truncateInfo = getVLSNIndexTruncationInfo();

        if (truncateInfo == null) {
            /* This is a corner case and seems unlikely to ever happen. */
            throw new NoUnprotectedFilesException(
                "Cannot truncate VLSNIndex because no remaining files " +
                    "contain VLSNs.",
                Level.INFO);
        }

        /*
         * Truncate the VLSNIndex, then try again to get an unprotected file.
         */
        if (!envImpl.tryTruncateVlsnHead(
                truncateInfo.first(), truncateInfo.second())) {
            /*
             * There are several reasons for tryTruncateVlsnHead to return
             * false as explained by VLSNTracker.tryTruncateFromHead(VLSN,
             * long, LogItemCache) javadoc.  Only one of them -- that the file
             * is protected -- should cause a WARNING message. To avoid false
             * alarms we log an INFO message here for now. TODO: Return more
             * info from tryTruncateVlsnHead so we can log a WARNING when
             * appropriate.
             */
            throw new NoUnprotectedFilesException(
                "VLSNIndex already truncated or cannot be truncated further.",
                Level.INFO);
        }

        file = getNextUnprotectedFile();
        if (file != null) {
            return file;
        }

        throw new NoUnprotectedFilesException(
            "Protected files are not in the recovery interval and " +
                "VLSNIndex was successfully truncated.",
            Level.WARNING);
    }

    /**
     * Returns a human readable description of what may be preventing erasure.
     * These are the reasons that {@link NoUnprotectedFilesException} may be
     * thrown by {@link #getNextFile()}.
     */
    private String getFileProtectionMessage() {

        final long firstRecoveryFile = getFirstFileInRecoveryInterval();

        return "Files protected by the current recovery interval: [" +
            FormatUtil.asHexString(
                filesRemaining.tailSet(firstRecoveryFile)) +
            "]. Other protected files: " +
            fileProtector.getProtectedFileMap(filesRemaining) +
            ". FirstRecoveryFile: 0x" + Long.toHexString(firstRecoveryFile) +
            ". FirstVLSNIndexFile: 0x" +
            Long.toHexString(fileProtector.getVLSNIndexStartFile()) + ".";
    }

    /**
     * Returns the first file remaining that is unprotected. If a file is
     * selected, the currentFile field is updated to support aborting.
     * This method synchronizes with {@link #abortErase} to ensure that a
     * file passed to abortErase will not be selected again for erasure.
     */
    private Long getNextUnprotectedFile() {

        synchronized (currentFileMutex) {

            final Long file =
                fileProtector.getFirstUnprotectedFile(filesRemaining);

            if (file == null) {
                return null;
            }

            if (file >= getFirstFileInRecoveryInterval()) {
                return null;
            }

            currentFile = file;
            abortCurrentFile = false;
            return file;
        }
    }

    /**
     * Called after a file returned by {@link #getNextUnprotectedFile()} is no
     * longer being processed.
     */
    private void clearCurrentFile() {

        synchronized (currentFileMutex) {
            currentFile = null;
            abortCurrentFile = false;
        }
    }

    /**
     * Returns the file being processed or null. A volatile field is accessed
     * without synchronization.
     */
    Long getCurrentFile() {
        return currentFile;
    }

    /**
     * Used to ensure that erasure of a file stops before coping that file
     * during a backup or network restore. A short wait may be performed to
     * ensure that the eraser thread notices the abort and any in-process
     * writes are completed.
     *
     * @param fileSet erasure of the current file is aborted if the current
     * file is protected by this protected file set. The given fileSet must
     * be protected at the time this method is called.
     *
     * @throws EraserAbortException if we can't abort erasure of a target
     * file within {@link EnvironmentParams#ERASE_ABORT_TIMEOUT}. The
     * timeout is long, so this should not happen unless the eraser thread
     * is wedged or starved.
     */
    public void abortErase(final FileProtector.ProtectedFileSet fileSet) {

        final Long abortFile;

        /*
         * Synchronize with getNextUnprotectedFile to ensure that a file
         * passed to abortErase will not be selected again for erasure.
         */
        synchronized (currentFileMutex) {

            /*
             * The check for isReservedFile is used to avoid aborting when we
             * will not erase the file. This is a minor optimization and is
             * not required for correctness.
             */
            if (currentFile != null &&
                fileSet.isProtected(currentFile, null) &&
                !fileProtector.isReservedFile(currentFile)) {

                abortCurrentFile = true;
                abortFile = currentFile;
            } else {
                return;
            }
        }

        /*
         * Wait for the eraser thread to notice that the abortCurrentFile flag
         * is set, at which time it will set currentFile to null. The wait is
         * necessary to ensure that a write will not be performed after this
         * method returns.
         *
         * It is important that we return ASAP, so we use Object.notify to
         * wake up any waiters. All other polling is done with Object.wait on
         * the pollMutex object, to ensure we can abort promptly.
         */
        if (PollCondition.await(1, abortTimeoutMs,
            () -> {
                synchronized (pollMutex) {
                    pollMutex.notify();
                }
                synchronized (currentFileMutex) {
                    return !abortFile.equals(currentFile);
                }
            })) {
            return; /* Aborted. */
        }

        /*
         * We don't expect this to ever occur, because the timeout is quite
         * long and all erasure operations are checking the abortCurrentFile
         * flag quite frequently. EraserAbortException is thrown (rather than
         * EnvironmentWedgedException) because we don't want to shut down the
         * environment when DbBackup.start fails.
         */
        final String msg = "Unable to abort erasure of file 0x" +
            Long.toHexString(abortFile) + " within " +
            abortTimeoutMs + "ms.";

        LoggerUtils.warning(logger, envImpl, msg);
        throw new EraserAbortException(msg);
    }

    /**
     * Returns the file number of the firstActiveLsn for the last completed
     * checkpoint. Files GTE this file would be in the recovery interval if
     * a crash occurs, and are protected from erasure.
     */
    private long getFirstFileInRecoveryInterval() {

        return DbLsn.getFileNumber(
            envImpl.getCheckpointer().getLastCheckpointFirstActiveLsn());
    }

    /**
     * Returns the {endVLSN,endFile} pair that is needed to truncate the
     * VLSNIndex.
     *
     * Returns null if no remaining files-to-erase contain VLSNs. A file that
     * does not contain VLSNs (is "barren" may contain BINs or migrated LNs
     * for data that needs erasure. However, this is expected to be a rare
     * condition so we tolerate it. It will be corrected during a subsequent
     * cycle if any file following this cycle's files need erasure and do
     * contain VLSNs, or the VLSNIndex is truncated for other reasons, for
     * example, to reclaim disk space.
     *
     * Selects the last file-to-erase containing a VLSN. This reduces the
     * number of (fairly expensive) VLSNIndex truncations to the minimum.
     * However, it means that the index will be truncated for a large number
     * of files at once, rather than truncating it incrementally.
     */
    private Pair<VLSN, Long> getVLSNIndexTruncationInfo() {

        for (final Long file : filesRemaining.descendingSet()) {

            checkContinue();

            /*
             * We avoid searching the file if it happens to be a reserved
             * file, since the last VLSN is known to the FileProtector.
             */
            VLSN lastVlsn = fileProtector.getReservedFileLastVLSN(file);

            if (lastVlsn != null) {
                if (lastVlsn.isNull()) {
                    continue;
                }
                return new Pair<>(lastVlsn, file);
            }

            /* Must search. */
            lastVlsn = searchFileForLastVLSN(file);

            if (lastVlsn != null) {
                if (lastVlsn.isNull()) {
                    continue;
                }
                return new Pair<>(lastVlsn, file);
            }
        }

        return null;
    }

    /**
     * Returns the last VLSN in the given file, or NULL_VLSN if the file does
     * contain any VLSNs, or null if the file does not exist.
     *
     * Only VLSNs in replicated entries are considered. VLSNs in migrated LNs
     * are ignored.
     */
    private VLSN searchFileForLastVLSN(final Long file) {

        final FileInfo fileInfo = fileInfoCache.get(file);

        if (fileInfo != null && fileInfo.lastVlsn != null) {
            return fileInfo.lastVlsn;
        }

        final long fileLength = (fileInfo != null) ?
            fileInfo.length : getFileLength(file);

        boolean forward;
        long startLsn;
        long finishLsn;
        long endOfFileLsn;

        try {
            /*
             * If file+1 exists, read its header to get the offset of the
             * previous entry, and then read the file backwards to find the
             * last VLSN.
             */
            final long prevOffset =
                fileManager.getFileHeaderPrevOffset(file + 1);

            forward = false;
            startLsn = DbLsn.makeLsn(file, prevOffset);
            finishLsn = DbLsn.makeLsn(file, 0);
            endOfFileLsn = DbLsn.makeLsn(file, fileLength);

        } catch (FileNotFoundException e) {
            /*
             * If file+1 does not exist, read the entire file from the start.
             */
            forward = true;
            startLsn = DbLsn.makeLsn(file, 0);
            finishLsn = DbLsn.NULL_LSN;
            endOfFileLsn = DbLsn.NULL_LSN;

        } catch (ChecksumException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_CHECKSUM, e);
        }

        final FileReader reader = new FileReader(
            envImpl, cleaner.readBufferSize, forward, startLsn,
            file /*singleFileNumber*/, endOfFileLsn, finishLsn) {

            @Override
            protected boolean processEntry(ByteBuffer entryBuffer) {

                final int readOps = getAndResetNReads();
                reads.addAndGet(readOps);
                readBytes.addAndGet(readOps * cleaner.readBufferSize);

                cycleThrottle.throttle(currentEntryHeader.getEntrySize());
                checkContinue();

                /* Skip the data, no need to materialize the entry. */
                skipEntry(entryBuffer);

                /*
                 * Only consider replicated entries. Note that this includes
                 * Erased entries.
                 */
                return currentEntryHeader.getReplicated();
            }
        };

        VLSN lastVlsn = VLSN.NULL_VLSN;

        try {
            while (reader.readNextEntryAllowExceptions()) {
                lastVlsn = reader.getLastVlsn();

                if (lastVlsn == null || lastVlsn.isNull()) {
                    throw EnvironmentFailureException.unexpectedState(
                        "Replicated entries must have a VLSN.");
                }

                if (!forward) {
                    break;
                }
            }
        } catch (ChecksumException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_CHECKSUM, e);

        } catch (FileNotFoundException e) {
            return null;
        }

        if (fileInfo != null) {
            fileInfo.lastVlsn = lastVlsn;
        }

        return lastVlsn;
    }

    /**
     * Erases the targeted entries in the given file and makes the changes
     * persistent using fsync.
     */
    private void eraseFile(final Long file) {

        /*
         * If a reserved file can be deleted, this is cheaper than erasure.
         * Use 25% of the file size for throttling.
         *
         * If cleaner.deleteReservedFile returns false then we proceed with
         * erasure. This is safe because the VLSNIndex has been truncated.
         */
        final long fileLength = fileInfoCache.get(file).length;

        if (fileProtector.isReservedFile(file) &&
            cleaner.deleteReservedFile(file, "ERASER")) {

            filesDeleted.incrementAndGet();
            cycleThrottle.throttle((fileLength * 25) / 100);
            return;
        }

        final EraserReader reader = new EraserReader(file);
        final DbCache dbCache = new DbCache(envImpl, cleaner);

        final ExtinctionScanner extinctionScanner =
            envImpl.getExtinctionScanner();

        int entriesToErase = 0;
        long firstWriteTime = 0;
        boolean completed = false;

        final PackedOffsets obsoleteOffsets =
            envImpl.getUtilizationProfile().getObsoleteDetailPacked(
                file, false /*logUpdate*/, this::checkContinue);

        final PackedOffsets.Iterator obsoleteIter = obsoleteOffsets.iterator();
        long nextObsolete = -1;

        final String fullFileName = fileManager.getFullFileName(file);
        RandomAccessFile raf = null;
        try {
            /* Clear checksum saved by network restore. */
            envImpl.clearedCachedFileChecksum(
                new File(fullFileName).getName());

            while (reader.readNextEntryAllowExceptions()) {

                dbCache.clearCachePeriodically();

                final long logLsn = reader.getLastLsn();
                final long fileOffset = DbLsn.getFileOffset(logLsn);

                while (nextObsolete < fileOffset && obsoleteIter.hasNext()) {
                    nextObsolete = obsoleteIter.next();
                }
                final boolean isKnownObsolete = (nextObsolete == fileOffset);

                boolean doErase = false;

                if (reader.isErased) {

                    final ErasedLogEntry erasedEntry =
                        (ErasedLogEntry) reader.logEntry;

                    if (!erasedEntry.isAllZeros()) {
                        doErase = true;
                    }
                } else {
                    final DatabaseId dbId = reader.logEntry.getDbId();
                    DbCache.DbInfo dbInfo = dbCache.getDbInfo(dbId);

                    if (dbInfo.deleted) {
                        /*
                         * All LNs and BINs in deleted DBs can be erased. But
                         * when eraseDeletedDbs is false, if the DB is deleted
                         * then we cannot erase its extinct records either,
                         * because we don't have the DB name and dups status.
                         *
                         * If the DB is being deleted (DbInfo.deleting is
                         * true) then we cannot erase its active INs because
                         * they may not have been counted obsolete yet.
                         * However, it is OK to erase its LNs and obsolete
                         * INs, and we do that further below.
                         */
                        if (eraseDeletedDbs) {
                            doErase = true;
                        }

                    } else if (reader.isLN && eraseExtinctRecords) {
                        /*
                         * All extinct LNs (in non-deleted DBs) can be erased.
                         */
                        final LNLogEntry<?> lnEntry =
                            (LNLogEntry<?>) reader.logEntry;

                        lnEntry.postFetchInit(dbInfo.dups);

                        if (envImpl.getExtinctionState(
                            dbInfo.name, dbInfo.dups, dbInfo.internal,
                            lnEntry.getKey()) == EXTINCT) {

                            doErase = true;
                        }

                    } else if (reader.isBIN && eraseExtinctRecords) {
                        /*
                         * A BIN with an extinct slot can be erased only if
                         * the BIN is obsolete/dead.
                         */
                        final BIN bin = (BIN) reader.logEntry.getMainItem();

                        for (int i = 0; i < bin.getNEntries(); i += 1) {

                            checkContinue();

                            final byte[] key = bin.getKey(i);

                            final ExtinctionFilter.ExtinctionStatus status =
                                envImpl.getExtinctionState(
                                    dbInfo.name, dbInfo.dups, dbInfo.internal,
                                    key);

                            if (status == MAYBE_EXTINCT) {
                                /*
                                 * This is a crude way to restart and give the
                                 * the app a chance to initialize its metadata.
                                 */
                                throw new AbortCurrentFileException(file);
                            }

                            if (status != EXTINCT) {
                                continue;
                            }

                            if (isKnownObsolete) {
                                doErase = true;
                                break;
                            }

                            /*
                             * Defer expensive getDbImpl and Btree lookup
                             * (isBINDead call) until we encounter an extinct
                             * slot in a BIN that is not known-obsolete. We
                             * must do a Btree lookup because the known-
                             * obsolete metadata is not 100% complete.
                             */
                            dbInfo = dbCache.getDbImpl(dbId);

                            /*
                             * If we need to do a Btree lookup and dbImpl is
                             * null, then the DB was deleted since the last
                             * DB lookup and erasure must wait until the next
                             * cycle.
                             *
                             * If the key for this DB is part of an active
                             * extinction, then the BIN cannot be persistently
                             * obsolete and we must wait for the task to
                             * finish before erasing the BIN.
                             *
                             * In addition, as an extra precaution, do not
                             * erase the BIN if isBINDead returns false, which
                             * means the BIN is still active in the in-memory
                             * Btree.
                             */
                            if (dbInfo.dbImpl != null &&
                                !extinctionScanner.isRecordExtinctionIncomplete(
                                    dbInfo.dbImpl, key) &&
                                isBINDead(dbInfo.dbImpl, bin, logLsn)) {

                                /* BIN is dead/obsolete. */
                                doErase = true;
                            }

                            /*
                             * If the BIN is not dead, we cannot erase it.
                             * This is the edge case where a record extinction
                             * has started (a slot is extinct) but not yet
                             * complete (the BIN is not obsolete yet).
                             */
                            break;
                        }
                    }
                }

                if (doErase) {
                    if (raf == null) {
                        firstWriteTime = System.currentTimeMillis();
                        raf = fileManager.openFileReadWrite(fullFileName);
                        touchAndFsync(raf, fileLength);
                    }

                    if (!reader.isErased) {
                        writeErasedType(raf, fileOffset);
                    }

                    entriesToErase = addEraseEntry(
                        entriesToErase,
                        fileOffset + reader.header.getSize(),
                        reader.header.getItemSize());
                }
            }

            /* Call releaseDbImpls before eraseEntries, which throttles. */
            dbCache.releaseDbImpls();

            eraseEntries(raf, fileLength, entriesToErase);
            filesErased.incrementAndGet();
            completed = true;

        } catch (ChecksumException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_CHECKSUM,
                "Exception erasing file 0x" + Long.toHexString(file),
                e);

        } catch (FileNotFoundException e) {
            /* File was deleted by the cleaner. */
            filesAlreadyDeleted.incrementAndGet();

        } catch (IOException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_WRITE,
                "Exception erasing file 0x" + Long.toHexString(file),
                e);

        } finally {

            if (firstWriteTime != 0) {
                LoggerUtils.info(
                    logger, envImpl,
                    "ERASER attempted to erase " + entriesToErase +
                        " entries in file 0x" + Long.toHexString(file) +
                        ", first write at " + formatTime(firstWriteTime) +
                        ", erasure is " +
                        (completed ? "complete" : "incomplete") + ".");
            }

            dbCache.releaseDbImpls();

            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    LoggerUtils.warning(
                        logger, envImpl,
                        "DataEraser.eraseFile exception when closing " +
                            "file 0x" + Long.toHexString(file) + ": " + e);
                }
            }
        }
    }

    /**
     * File reader that materializes only user-database LNs, and BINs and
     * BIN-deltas.
     */
    private class EraserReader extends FileReader {
        LogEntryHeader header;
        LogEntryType entryType;
        LogEntry logEntry;
        boolean isLN;
        boolean isBIN;
        boolean isErased;

        EraserReader(final Long file) {
            super(DataEraser.this.envImpl, cleaner.readBufferSize,
                true /*forward*/,
                DbLsn.makeLsn(file, 0) /*startLsn*/,
                file /*singleFileNumber*/,
                DbLsn.NULL_LSN /*endOfFileLsn*/,
                DbLsn.NULL_LSN /*finishLsn*/);
        }

        @Override
        protected boolean processEntry(final ByteBuffer entryBuffer) {

            final int readOps = getAndResetNReads();
            reads.addAndGet(readOps);
            readBytes.addAndGet(readOps * cleaner.readBufferSize);

            cycleThrottle.throttle(currentEntryHeader.getEntrySize());
            checkContinue();

            header = currentEntryHeader;
            isLN = false;
            isBIN = false;
            isErased = false;
            logEntry = null;
            entryType = LogEntryType.findType(header.getType());
            assert entryType != null;

            if (entryType.isUserLNType()) {
                isLN = true;
            } else if (entryType.equals(LogEntryType.LOG_BIN) ||
                entryType.equals(LogEntryType.LOG_BIN_DELTA)) {
                isBIN = true;
            } else if (entryType.equals(LogEntryType.LOG_ERASED)) {
                isErased = true;
            } else {
                skipEntry(entryBuffer);
                return false;
            }

            logEntry = entryType.getNewLogEntry();
            logEntry.readEntry(envImpl, header, entryBuffer);
            return true;
        }
    }

    /**
     * Returns whether the given BIN or BIN-delta is absent from the Btree.
     * This only checks the in-memory cache, so returning true does not
     * guarantee that the BIN is persistently obsolete. Therefore this method
     * is only useful to provide added safety.
     */
    private boolean isBINDead(final DatabaseImpl db,
                              final BIN binFromLog,
                              final long logLsn) {

        binFromLog.latchNoUpdateLRU(db);

        final SearchResult result = db.getTree().getParentINForChildIN(
            binFromLog, true /*useTargetLevel*/,
            true /*doFetch*/, CacheMode.UNCHANGED);

        if (result.parent == null) {
            /*
             * Btree is completely empty (no root IN). Since we never reduce
             * the number of levels in the Btree, this should only occur if
             * the DB was truncated.
             */
            return true;
        }

        try {
            /*
             * For a parent search with useTargetLevel=true, a non-null parent
             * implies that a match was found.
             */
            assert result.exactParentFound;

            /*
             * Check whether the BIN is active by comparing the active Btree
             * LSN to the LSN of the BIN we read from the log.
             */
            final long treeLsn = result.parent.getLsn(result.index);

            if (binFromLog.isBINDelta(false)) {
                /*
                 * A BIN-delta read from the log is obsolete if its LSN is
                 * not equal to the tree LSN.
                 */
                return logLsn != treeLsn;
            }

            /*
             * A full BIN read from the log is active if its LSN is equal to
             * the tree LSN.
             */
            if (logLsn == treeLsn) {
                return false;
            }

            /*
             * If the tree and log LSNs are unequal, then we must get the full
             * version LSN in case the tree LSN is actually for a BIN-delta.
             * The only way to do that is to fetch the IN in the tree.
             */
            final BIN treeBin = (BIN) result.parent.fetchIN(
                result.index, CacheMode.UNCHANGED);

            return logLsn != treeBin.getLastFullLsn();

        } finally {
            result.parent.releaseLatch();
        }
    }

    /**
     * Adds the offset/size of one entry to be erased.
     */
    private int addEraseEntry(final int n,
                              final long offset,
                              final int size) {

        if (n == eraseOffsets.length) {

            /* Enlarge arrays. */
            final long[] newOffsets = new long[n * 2];
            System.arraycopy(eraseOffsets, 0, newOffsets, 0, n);
            eraseOffsets = newOffsets;

            final int[] newSizes = new int[newOffsets.length];
            System.arraycopy(eraseSizes, 0, newSizes, 0, n);
            eraseSizes = newSizes;
        }

        eraseOffsets[n] = offset;
        eraseSizes[n] = size;
        return n + 1;
    }

    /**
     * Ensures that the file's lastModifiedTime is persistently updated,
     * before performing any modifications to the file. This allows
     * applications to detect erasure by checking for a change to
     * lastModifiedTime across erasure cycles, without the worry that a
     * crash during erasure may have prevented the file system from updating
     * the lastModifiedTime.
     */
    private void touchAndFsync(final RandomAccessFile file,
                               final long fileLength)
        throws IOException {

        checkContinue();

        file.seek(0);
        final byte b = file.readByte();
        file.seek(0);
        file.writeByte(b);
        writes.incrementAndGet();
        writeBytes.incrementAndGet();

        file.getChannel().force(false);
        fSyncs.incrementAndGet();
        cycleThrottle.throttle((fileLength * FSYNC1_WORK_PCT) / 100);

        // TODO update FileManager stats?
    }

    /**
     * Changes the type of the entry on disk to the LOG_ERASED type.
     */
    private void writeErasedType(final RandomAccessFile file,
                                 final long headerOffset)
        throws IOException {

        file.seek(headerOffset + LogEntryHeader.ENTRYTYPE_OFFSET);
        checkContinue();
        file.writeByte(LogEntryType.LOG_ERASED.getTypeNum());

        writes.incrementAndGet();
        writeBytes.incrementAndGet();

        // TODO update FileManager stats?
    }

    /**
     * Writes zeros in the entries in the erase list and fsyncs to persist
     * the changes.
     */
    private void eraseEntries(final RandomAccessFile raf,
                              final long fileLength,
                              final int entries)
        throws IOException {

        if (entries == 0) {
            return;
        }

        final long fsync1Work = (fileLength * FSYNC1_WORK_PCT) / 100;
        final long fsync2Work = (fileLength * FSYNC2_WORK_PCT) / 100;
        final long fsync3Work = (fileLength * FSYNC3_WORK_PCT) / 100;
        final long writeWork = ((fileLength * WRITE_WORK_PCT) / 100) / entries;

        /* Don't neglect work lost due to int truncation. */
        final long extraWork = fileLength -
            ((writeWork * entries) + fsync1Work + fsync2Work + fsync3Work);

        /*
         * Make type changes persistent before writing zeros. This is the only
         * way to be sure that an entry's type is changed before it is zero
         * filled, since write ordering is not guaranteed.
         */
        checkContinue();
        raf.getChannel().force(false);
        fSyncs.incrementAndGet();
        cycleThrottle.throttle(fsync2Work);

        for (int i = 0; i < entries; i += 1) {

            final long offset = eraseOffsets[i];
            final int size = eraseSizes[i];

            if (zeros.length < size) {
                zeros = new byte[size * 2];
            }

            raf.seek(offset);
            checkContinue();
            raf.write(zeros, 0, size);
            writes.incrementAndGet();
            writeBytes.addAndGet(size);
            cycleThrottle.throttle(writeWork);
        }

        /* Make complete erasure of this file persistent. */
        checkContinue();
        raf.getChannel().force(false);
        fSyncs.incrementAndGet();
        cycleThrottle.throttle(fsync3Work + extraWork);

        // TODO update FileManager stats?
    }

    /**
     * Returns whether the log entry at the given LSN has been erased.
     *
     * Is not particularly efficient (it opens and closes the file each time
     * it is called) and is meant for exceptional circumstances such as when a
     * checksum verification fails.
     */
    public boolean isEntryErased(final long lsn) {

        final long file = DbLsn.getFileNumber(lsn);
        final long offset = DbLsn.getFileOffset(lsn);

        /*
         * Can't use FileManager.getFileHandle here because we may be called
         * with the FileHandle already latched and latching is not reentrant.
         */
        RandomAccessFile fileHandle = null;
        try {
            fileHandle = new RandomAccessFile(
                fileManager.getFullFileName(file),
                FileManager.FileMode.READ_MODE.getModeValue());

            fileHandle.seek(offset + LogEntryHeader.ENTRYTYPE_OFFSET);

            return fileHandle.readByte() ==
                LogEntryType.LOG_ERASED.getTypeNum();

        } catch (FileNotFoundException|EOFException e) {
            return false;

        } catch (IOException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_WRITE,
                "Exception checking erasure file 0x" + Long.toHexString(file),
                e);
        } finally {
            if (fileHandle != null) {
                try {
                    fileHandle.close();
                } catch (IOException e) {
                    LoggerUtils.warning(
                        logger, envImpl,
                        "DataEraser.isEntryErased exception when closing " +
                            "file 0x" + Long.toHexString(file) + ": " + e);
                }
            }
        }
    }

    /**
     * Utility to help spread a given amount of totalWork overs a given
     * durationMs.
     */
    private class WorkThrottle {

        private final float msPerUnitOfWork;
        private final long startTime;
        private long workDone;

        WorkThrottle(final long totalWork,
                     final long durationMs) {

            msPerUnitOfWork = ((float) durationMs) / totalWork;
            startTime = System.currentTimeMillis();
            workDone = 0;
        }

        /**
         * Waits until the ratio of elapsed time to durationMs is GTE the
         * ratio of workDone to totalWork. Elapsed time is time since the
         * constructor was called.
         *
         * If the computed wait time is less than MIN_WORK_DELAY_MS, no wait
         * is performed. This is meant to prevent large numbers of waits for
         * relatively small amounts of work done. Note that this method is
         * called frequently to add very very small amounts of work.
         */
        void throttle(final long addWork) {

            workDone += addWork;

            final long workDoneMs = (long) (workDone * msPerUnitOfWork);

            final long delayMs =
                workDoneMs - (System.currentTimeMillis() - startTime);

            if (delayMs >= MIN_WORK_DELAY_MS) {

                final long checkMs = Math.min(delayMs, pollCheckMs);

                PollCondition.await(checkMs, delayMs, pollMutex,
                    () -> {
                        checkContinue();
                        return false;
                    });
            }
        }
    }

    private void checkShutdown() {
        if (shutdownRequested || !envImpl.isValid()) {
            throw new ShutdownRequestedException();
        }
    }

    private void checkContinue() {

        checkShutdown();

        if (!enabled) {
            throw new ErasureDisabledException();
        }

        if (endTime - startTime != cycleMs) {
            throw new PeriodChangedException();
        }

        synchronized (currentFileMutex) {
            if (abortCurrentFile) {
                final long file = currentFile;
                currentFile = null;
                abortCurrentFile = false;
                throw new AbortCurrentFileException(file);
            }
        }
    }

    private boolean checkForCycleEnd() {

        if (System.currentTimeMillis() >= endTime) {
            if (filesRemaining.isEmpty()) {
                logCycleComplete();
            } else {
                logCycleIncomplete();
            }
            return true;
        }

        return false;
    }

    private static class NoUnprotectedFilesException extends RuntimeException {

        final Level logLevel;

        NoUnprotectedFilesException(final String msg,
                                    final Level logLevel) {
            super(msg);
            this.logLevel = logLevel;
        }
    }

    private static class ErasureDisabledException extends RuntimeException {
    }

    private static class PeriodChangedException extends RuntimeException {
    }

    private static class ShutdownRequestedException extends RuntimeException {
    }

    private static class AbortCurrentFileException extends RuntimeException {
        final long file;

        AbortCurrentFileException(final long file) {
            this.file = file;
        }
    }

    /**
     * If enabled has been set to false, wait for it to be set to true again.
     *
     * Throws ShutdownRequestedException but no other internal exceptions.
     * Calling checkContinue is unnecessary because currentFile is null.
     */
    private void waitForEnabled() {
        while (true) {
            if (PollCondition.await(
                pollCheckMs, FOREVER_TIMEOUT_MS, pollMutex,
                () -> {
                    checkShutdown();
                    return enabled;
                })) {
                return;
            }
        }
    }

    /**
     * Wait for the current time to pass the end time.
     */
    private void waitForCycleEnd() {
        completionTime = System.currentTimeMillis();
        logCycleComplete();

        PollCondition.await(
            pollCheckMs, endTime - completionTime, pollMutex,
            () -> {
                checkContinue();
                return false;
            });
    }

    /**
     * Sleep for a time and dream about things that cause files to become
     * unprotected. When we wake up, our dreams may have come true.
     *
     * Throws ShutdownRequestedException but no other internal exceptions.
     * Calling checkContinue is unnecessary because currentFile is null.
     */
    private void waitForUnprotectedFiles(final NoUnprotectedFilesException e) {

        lastProtectedFilesMsg = e.getMessage();
        lastProtectedFilesMsgLevel = e.logLevel;

        PollCondition.await(
            pollCheckMs, NO_UNPROTECTED_FILES_DELAY_MS, pollMutex,
            () -> {
                checkShutdown();
                return false;
            });
    }

    private void storeState() {

        final TupleOutput out = new TupleOutput();

        out.writeLong(startTime);
        out.writeLong(endTime);
        out.writePackedLong(totalCycleWork);
        storeFileSet(out, filesCompleted);
        storeFileSet(out, filesRemaining);

        final DatabaseEntry data = new DatabaseEntry();
        TupleBase.outputToEntry(out, data);

        envImpl.getMetadataStore().put(MetadataStore.KEY_ERASER, data);
    }

    private boolean loadState() {

        final DatabaseEntry data = new DatabaseEntry();

        if (envImpl.getMetadataStore().get(
            MetadataStore.KEY_ERASER, data) == null) {
            return false;
        }

        final TupleInput in = TupleBase.entryToInput(data);

        startTime = in.readLong();
        endTime = in.readLong();
        totalCycleWork = in.readPackedLong();
        filesCompleted = loadFileSet(in);
        filesRemaining = loadFileSet(in);

        return true;
    }

    private void storeFileSet(final TupleOutput out,
                              final NavigableSet<Long> set) {

        out.writePackedInt(set.size());
        long priorFile = 0;

        for (final long file : set) {
            out.writePackedLong(file - priorFile);
            priorFile = file;
        }
    }

    private NavigableSet<Long> loadFileSet(final TupleInput in) {

        final int size = in.readPackedInt();

        final NavigableSet<Long> set =
            Collections.synchronizedNavigableSet(new TreeSet<>());

        long file = 0;

        for (int i = 0; i < size; i += 1) {
            file += in.readPackedLong();
            set.add(file);
        }

        return set;
    }

    /** Cached info about a file to be erased. */
    private static class FileInfo {

        final long creationTime;
        final long length;
        VLSN lastVlsn;

        FileInfo(final long creationTime, final long length) {
            this.creationTime = creationTime;
            this.length = length;
        }
    }

    private void logCycleInit(final int filesToExamine) {

        LoggerUtils.info(logger, envImpl,
            "ERASER initializing new cycle. Total files: " + filesToExamine);

        callTestEventHook(TestEvent.Type.INIT);
    }

    private void logCycleStart() {

        LoggerUtils.info(logger, envImpl,
            "ERASER new cycle started. " + getCycleStatus());

        callTestEventHook(TestEvent.Type.START);
    }

    private void logCycleComplete() {

        LoggerUtils.info(logger, envImpl,
            "ERASER cycle completed. " + getCycleStatus());

        callTestEventHook(TestEvent.Type.COMPLETE);
    }

    private void logCycleIncomplete() {

        final String msg = "ERASER unable to erase files " +
            "within the erasure period. " +
            ((lastProtectedFilesMsg != null) ?
                lastProtectedFilesMsg :
                "File protection did not prevent erasure, so probably " +
                    "just ran out of time.") +
            " " + getFileProtectionMessage() +
            " " + getCycleStatus();

        LoggerUtils.logMsg(
            logger, envImpl,
            (lastProtectedFilesMsgLevel != null) ?
                lastProtectedFilesMsgLevel : Level.INFO,
            msg);

        callTestEventHook(TestEvent.Type.INCOMPLETE);
    }

    private void logCycleResume() {

        LoggerUtils.info(logger, envImpl,
            "ERASER previously incomplete cycle resumed at startup. " +
                getCycleStatus());

        callTestEventHook(TestEvent.Type.RESUME);
    }

    private void logCycleCannotResume() {

        LoggerUtils.warning(logger, envImpl,
            "ERASER previously incomplete cycle not resumed at startup" +
                " because end time has passed. " + getCycleStatus());

        callTestEventHook(TestEvent.Type.CANNOT_RESUME);
    }

    private void logCycleSuspend() {

        LoggerUtils.info(logger, envImpl,
            "ERASER incomplete cycle suspended at shutdown. " +
                getFileProtectionMessage() + " " + getCycleStatus());

        callTestEventHook(TestEvent.Type.SUSPEND);
    }

    private void logPeriodChanged() {

        LoggerUtils.info(logger, envImpl,
            "ERASER period param was changed, current cycle aborted. " +
                getCycleStatus());

        callTestEventHook(TestEvent.Type.PERIOD_CHANGED);
    }

    private String getCycleStatus() {

        return "Cycle start: " + formatTime(startTime) +
            ", end: " + formatTime(endTime) +
            ", filesCompleted: [" + FormatUtil.asHexString(filesCompleted) +
            "], filesRemaining: [" + FormatUtil.asHexString(filesRemaining) +
            "].";
    }

    private static String formatTime(final long time) {

        return DATE_FORMAT.format(new Date(time));
    }

    private void callTestEventHook(final TestEvent.Type type) {

        if (testEventHook == null) {
            return;
        }

        testEventHook.doHook(new TestEvent(type, this));
    }

    static void setTestEventHook(final TestHook<TestEvent> hook) {
        testEventHook = hook;
    }

    static class TestEvent {

        enum Type {
            INIT,
            START,
            COMPLETE,
            INCOMPLETE,
            RESUME,
            CANNOT_RESUME,
            SUSPEND,
            PERIOD_CHANGED,
        }

        final Type type;
        final long startTime;
        final long endTime;
        final long completionTime;
        final NavigableSet<Long> filesCompleted;
        final NavigableSet<Long> filesRemaining;

        TestEvent(final Type type,
                  final DataEraser eraser) {

            this.type = type;
            startTime = eraser.startTime;
            endTime = eraser.endTime;
            completionTime = eraser.completionTime;
            filesCompleted = new TreeSet<>(eraser.filesCompleted);
            filesRemaining = new TreeSet<>(eraser.filesRemaining);
        }

        @Override
        public String toString() {
            return type.toString() +
                " startTime=" + formatTime(startTime) +
                " endTime=" + formatTime(endTime) +
                " completionTime=" + formatTime(completionTime) +
                " filesCompleted=[" + FormatUtil.asHexString(filesCompleted) +
                "] filesRemaining=[" + FormatUtil.asHexString(filesRemaining) +
                "]";
        }
    }
}
