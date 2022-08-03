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

package com.sleepycat.je.log;

import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * Per-stat Metadata for JE FileManager, FSyncManager, LogManager and
 * LogBufferPool statistics.
 */
public class LogStatDefinition {

    /* Group definition for all log statistics. */
    public static final String GROUP_NAME = "I/O";
    public static final String GROUP_DESC =
        "The file I/O component of the append-only storage system includes " +
            "data file access, buffering and group commit.";

    /*
     * Note that the LBF, FILEMGR and FSYNCMGR groups are not user-visible.
     * These internal groups are used only to copy stats to the I/O group.
     */
    public static final String LBF_GROUP_NAME = "LogBufferPool";
    public static final String LBF_GROUP_DESC = "LogBufferPool statistics";
    public static final String FILEMGR_GROUP_NAME = "FileManager";
    public static final String FILEMGR_GROUP_DESC = "FileManager statistics";
    public static final String FSYNCMGR_GROUP_NAME = "FSyncManager";
    public static final String FSYNCMGR_GROUP_DESC = "FSyncManager statistics";

    /* The following stat definitions are used in FileManager. */
    public static final String FILEMGR_RANDOM_READS_NAME =
        "nRandomReads";
    public static final String FILEMGR_RANDOM_READS_DESC =
        "Number of disk reads which required a seek of more than 1MB from " +
            "the previous file position or were read from a different file.";
    public static final StatDefinition FILEMGR_RANDOM_READS =
        new StatDefinition(
            FILEMGR_RANDOM_READS_NAME,
            FILEMGR_RANDOM_READS_DESC);

    public static final String FILEMGR_RANDOM_WRITES_NAME =
        "nRandomWrites";
    public static final String FILEMGR_RANDOM_WRITES_DESC =
        "Number of disk writes which required a seek of more than 1MB from " +
            "the previous file position or were read from a different file.";
    public static final StatDefinition FILEMGR_RANDOM_WRITES =
        new StatDefinition(
            FILEMGR_RANDOM_WRITES_NAME,
            FILEMGR_RANDOM_WRITES_DESC);

    public static final String FILEMGR_SEQUENTIAL_READS_NAME =
        "nSequentialReads";
    public static final String FILEMGR_SEQUENTIAL_READS_DESC =
        "Number of disk reads which did not require a seek of more than 1MB " +
            "from the previous file position and were read from the same " +
            "file.";
    public static final StatDefinition FILEMGR_SEQUENTIAL_READS =
        new StatDefinition(
            FILEMGR_SEQUENTIAL_READS_NAME,
            FILEMGR_SEQUENTIAL_READS_DESC);

    public static final String FILEMGR_SEQUENTIAL_WRITES_NAME =
        "nSequentialWrites";
    public static final String FILEMGR_SEQUENTIAL_WRITES_DESC =
        "Number of disk writes which did not require a seek of more than " +
            "1MB from the previous file position and were read from the " +
            "same file.";
    public static final StatDefinition FILEMGR_SEQUENTIAL_WRITES =
        new StatDefinition(
            FILEMGR_SEQUENTIAL_WRITES_NAME,
            FILEMGR_SEQUENTIAL_WRITES_DESC);

    public static final String FILEMGR_RANDOM_READ_BYTES_NAME =
        "nRandomReadBytes";
    public static final String FILEMGR_RANDOM_READ_BYTES_DESC =
        "Number of bytes read which required a seek of more than 1MB from " +
            "the previous file position or were read from a different file.";
    public static final StatDefinition FILEMGR_RANDOM_READ_BYTES =
        new StatDefinition(
            FILEMGR_RANDOM_READ_BYTES_NAME,
            FILEMGR_RANDOM_READ_BYTES_DESC);

    public static final String FILEMGR_RANDOM_WRITE_BYTES_NAME =
        "nRandomWriteBytes";
    public static final String FILEMGR_RANDOM_WRITE_BYTES_DESC =
        "Number of bytes written which required a seek of more than 1MB " +
            "from the previous file position or were read from a different " +
            "file.";
    public static final StatDefinition FILEMGR_RANDOM_WRITE_BYTES =
        new StatDefinition(
            FILEMGR_RANDOM_WRITE_BYTES_NAME,
            FILEMGR_RANDOM_WRITE_BYTES_DESC);

    public static final String FILEMGR_SEQUENTIAL_READ_BYTES_NAME =
        "nSequentialReadBytes";
    public static final String FILEMGR_SEQUENTIAL_READ_BYTES_DESC =
        "Number of bytes read which did not require a seek of more " +
            "than 1MB from the previous file position and were read from " +
            "the same file.";
    public static final StatDefinition FILEMGR_SEQUENTIAL_READ_BYTES =
        new StatDefinition(
            FILEMGR_SEQUENTIAL_READ_BYTES_NAME,
            FILEMGR_SEQUENTIAL_READ_BYTES_DESC);

    public static final String FILEMGR_SEQUENTIAL_WRITE_BYTES_NAME =
        "nSequentialWriteBytes";
    public static final String FILEMGR_SEQUENTIAL_WRITE_BYTES_DESC =
        "Number of bytes written which did not require a seek of more than " +
            "1MB from the previous file position and were read from the " +
            "same file.";
    public static final StatDefinition FILEMGR_SEQUENTIAL_WRITE_BYTES =
        new StatDefinition(
            FILEMGR_SEQUENTIAL_WRITE_BYTES_NAME,
            FILEMGR_SEQUENTIAL_WRITE_BYTES_DESC);

    public static final String FILEMGR_FILE_OPENS_NAME =
        "nFileOpens";
    public static final String FILEMGR_FILE_OPENS_DESC =
        "Number of times a log file has been opened.";
    public static final StatDefinition FILEMGR_FILE_OPENS =
        new StatDefinition(
            FILEMGR_FILE_OPENS_NAME,
            FILEMGR_FILE_OPENS_DESC);

    public static final String FILEMGR_OPEN_FILES_NAME =
        "nOpenFiles";
    public static final String FILEMGR_OPEN_FILES_DESC =
        "Number of files currently open in the file cache.";
    public static final StatDefinition FILEMGR_OPEN_FILES =
        new StatDefinition(
            FILEMGR_OPEN_FILES_NAME,
            FILEMGR_OPEN_FILES_DESC,
            StatType.CUMULATIVE);

    public static final String FILEMGR_BYTES_READ_FROM_WRITEQUEUE_NAME =
        "nBytesReadFromWriteQueue";
    public static final String FILEMGR_BYTES_READ_FROM_WRITEQUEUE_DESC =
        "Number of bytes read to fulfill file read operations by reading " +
            "out of the pending write queue.";
    public static final StatDefinition FILEMGR_BYTES_READ_FROM_WRITEQUEUE =
        new StatDefinition(
            FILEMGR_BYTES_READ_FROM_WRITEQUEUE_NAME,
            FILEMGR_BYTES_READ_FROM_WRITEQUEUE_DESC);

    public static final String FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE_NAME =
        "nBytesWrittenFromWriteQueue";
    public static final String FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE_DESC =
        "Number of bytes written from the pending write queue.";
    public static final StatDefinition FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE =
        new StatDefinition(
            FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE_NAME,
            FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE_DESC);

    public static final String FILEMGR_READS_FROM_WRITEQUEUE_NAME =
        "nReadsFromWriteQueue";
    public static final String FILEMGR_READS_FROM_WRITEQUEUE_DESC =
        "Number of file read operations which were fulfilled by reading out " +
            "of the pending write queue.";
    public static final StatDefinition FILEMGR_READS_FROM_WRITEQUEUE =
        new StatDefinition(
            FILEMGR_READS_FROM_WRITEQUEUE_NAME,
            FILEMGR_READS_FROM_WRITEQUEUE_DESC);

    public static final String FILEMGR_WRITES_FROM_WRITEQUEUE_NAME =
        "nWritesFromWriteQueue";
    public static final String FILEMGR_WRITES_FROM_WRITEQUEUE_DESC =
        "Number of file write operations executed from the pending write " +
            "queue.";
    public static final StatDefinition FILEMGR_WRITES_FROM_WRITEQUEUE =
        new StatDefinition(
            FILEMGR_WRITES_FROM_WRITEQUEUE_NAME,
            FILEMGR_WRITES_FROM_WRITEQUEUE_DESC);

    public static final String FILEMGR_WRITEQUEUE_OVERFLOW_NAME =
        "nWriteQueueOverflow";
    public static final String FILEMGR_WRITEQUEUE_OVERFLOW_DESC =
        "Number of write operations which would overflow the write queue.";
    public static final StatDefinition FILEMGR_WRITEQUEUE_OVERFLOW =
        new StatDefinition(
            FILEMGR_WRITEQUEUE_OVERFLOW_NAME,
            FILEMGR_WRITEQUEUE_OVERFLOW_DESC);

    public static final String FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES_NAME =
        "nWriteQueueOverflowFailures";
    public static final String FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES_DESC =
        "Number of write operations which would overflow the write queue and " +
            "could not be queued.";
    public static final StatDefinition FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES =
        new StatDefinition(
            FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES_NAME,
            FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES_DESC);

    /* The following stat definitions are used in FSyncManager. */
    public static final String FSYNCMGR_FSYNCS_NAME =
        "nFSyncs";
    public static final String FSYNCMGR_FSYNCS_DESC =
         "Number of group commit fsyncs completed.";
    public static final StatDefinition FSYNCMGR_FSYNCS =
        new StatDefinition(
            FSYNCMGR_FSYNCS_NAME,
            FSYNCMGR_FSYNCS_DESC);

    public static final String FSYNCMGR_TIMEOUTS_NAME =
        "nGrpCommitTimeouts";
    public static final String FSYNCMGR_TIMEOUTS_DESC =
        "Number of group commit waiter threads that timed out.";
    public static final StatDefinition FSYNCMGR_TIMEOUTS =
        new StatDefinition(
            FSYNCMGR_TIMEOUTS_NAME,
            FSYNCMGR_TIMEOUTS_DESC);

    public static final String FILEMGR_LOG_FSYNCS_NAME =
        "nLogFSyncs";
    public static final String FILEMGR_LOG_FSYNCS_DESC =
        "Number of fsyncs of the JE log.";
    public static final StatDefinition FILEMGR_LOG_FSYNCS =
        new StatDefinition(
            FILEMGR_LOG_FSYNCS_NAME,
            FILEMGR_LOG_FSYNCS_DESC);

    /* The following stat definitions are used in GrpCommitManager. */
    public static final String FILEMGR_FSYNC_AVG_MS_NAME = "fSyncAvgMs";
    public static final String FILEMGR_FSYNC_AVG_MS_DESC =
        "Average number of milliseconds used to perform fsyncs.";
    public static final StatDefinition FILEMGR_FSYNC_AVG_MS =
        new StatDefinition(FILEMGR_FSYNC_AVG_MS_NAME,
                           FILEMGR_FSYNC_AVG_MS_DESC);

    public static final String FILEMGR_FSYNC_95_MS_NAME = "fSync95Ms";
    public static final String FILEMGR_FSYNC_95_MS_DESC =
        "95th percentile of milliseconds used to perform fsyncs.";
    public static final StatDefinition FILEMGR_FSYNC_95_MS =
        new StatDefinition(FILEMGR_FSYNC_95_MS_NAME,
                           FILEMGR_FSYNC_95_MS_DESC);

    public static final String FILEMGR_FSYNC_99_MS_NAME = "fSync99Ms";
    public static final String FILEMGR_FSYNC_99_MS_DESC =
        "99th percentile of milliseconds used to perform fsyncs.";
    public static final StatDefinition FILEMGR_FSYNC_99_MS =
        new StatDefinition(FILEMGR_FSYNC_99_MS_NAME,
                           FILEMGR_FSYNC_99_MS_DESC);

    public static final String FILEMGR_FSYNC_MAX_MS_NAME = "fSyncMaxMs";
    public static final String FILEMGR_FSYNC_MAX_MS_DESC =
        "Maximum number of milliseconds used to perform a single fsync.";
    public static final StatDefinition FILEMGR_FSYNC_MAX_MS =
        new StatDefinition(FILEMGR_FSYNC_MAX_MS_NAME,
                           FILEMGR_FSYNC_MAX_MS_DESC);

    public static final String FSYNCMGR_N_GROUP_COMMIT_REQUESTS_NAME =
        "nGroupCommitRequests";
    public static final String FSYNCMGR_N_GROUP_COMMIT_REQUESTS_DESC =
        "Number of group commit requests.";
    public static final StatDefinition FSYNCMGR_N_GROUP_COMMIT_REQUESTS =
        new StatDefinition(
            FSYNCMGR_N_GROUP_COMMIT_REQUESTS_NAME,
            FSYNCMGR_N_GROUP_COMMIT_REQUESTS_DESC);

    public static final String FSYNCMGR_FSYNC_REQUESTS_NAME =
        "nFSyncRequests";
    public static final String FSYNCMGR_FSYNC_REQUESTS_DESC =
        "Number of group commit requests that include an fsync request.";
    public static final StatDefinition FSYNCMGR_FSYNC_REQUESTS =
        new StatDefinition(
            FSYNCMGR_FSYNC_REQUESTS_NAME,
            FSYNCMGR_FSYNC_REQUESTS_DESC);

    /* The following stat definitions are used in LogManager. */
    public static final String LOGMGR_REPEAT_FAULT_READS_NAME =
        "nRepeatFaultReads";
    public static final String LOGMGR_REPEAT_FAULT_READS_DESC =
        "Number of times a log entry size exceeded the log fault read " +
            "size.";
    public static final StatDefinition LOGMGR_REPEAT_FAULT_READS =
        new StatDefinition(
            LOGMGR_REPEAT_FAULT_READS_NAME,
            LOGMGR_REPEAT_FAULT_READS_DESC);

    public static final String LOGMGR_REPEAT_ITERATOR_READS_NAME =
        "nRepeatIteratorReads";
    public static final String LOGMGR_REPEAT_ITERATOR_READS_DESC =
        "Number of times a log entry size exceeded the log iterator max " +
            "size.";
    public static final StatDefinition LOGMGR_REPEAT_ITERATOR_READS =
        new StatDefinition(
            LOGMGR_REPEAT_ITERATOR_READS_NAME,
            LOGMGR_REPEAT_ITERATOR_READS_DESC);

    public static final String LOGMGR_TEMP_BUFFER_WRITES_NAME =
        "nTempBufferWrites";
    public static final String LOGMGR_TEMP_BUFFER_WRITES_DESC =
        "Number of writes for entries larger than the log buffer size, " +
            "forcing a write in the critical section.";
    public static final StatDefinition LOGMGR_TEMP_BUFFER_WRITES =
        new StatDefinition(
            LOGMGR_TEMP_BUFFER_WRITES_NAME,
            LOGMGR_TEMP_BUFFER_WRITES_DESC);

    public static final String LOGMGR_END_OF_LOG_NAME =
        "endOfLog";
    public static final String LOGMGR_END_OF_LOG_DESC =
        "The LSN of the next entry to be written to the log.";
    public static final StatDefinition LOGMGR_END_OF_LOG =
        new StatDefinition(
            LOGMGR_END_OF_LOG_NAME,
            LOGMGR_END_OF_LOG_DESC,
            StatType.CUMULATIVE);

    public static final String LBFP_NO_FREE_BUFFER_NAME =
        "nNoFreeBuffer";
    public static final String LBFP_NO_FREE_BUFFER_DESC =
        "Number of writes that could not obtain a free buffer, " +
            "forcing a write in the critical section.";
    public static final StatDefinition LBFP_NO_FREE_BUFFER =
        new StatDefinition(
            LBFP_NO_FREE_BUFFER_NAME,
            LBFP_NO_FREE_BUFFER_DESC);

    /* The following stat definitions are used in LogBufferPool. */
    public static final String LBFP_NOT_RESIDENT_NAME =
        "nNotResident";
    public static final String LBFP_NOT_RESIDENT_DESC =
        "Number of requests to read log entries by LSN.";
    public static final StatDefinition LBFP_NOT_RESIDENT =
        new StatDefinition(
            LBFP_NOT_RESIDENT_NAME,
            LBFP_NOT_RESIDENT_DESC);

    public static final String LBFP_MISS_NAME =
        "nCacheMiss";
    public static final String LBFP_MISS_DESC =
        "Number of requests to read log entries by LSN that were not " +
            "present in the log buffers.";
    public static final StatDefinition LBFP_MISS =
        new StatDefinition(
            LBFP_MISS_NAME,
            LBFP_MISS_DESC);

    public static final String LBFP_LOG_BUFFERS_NAME =
        "nLogBuffers";
    public static final String LBFP_LOG_BUFFERS_DESC =
        "Number of log buffers.";
    public static final StatDefinition LBFP_LOG_BUFFERS =
        new StatDefinition(
            LBFP_LOG_BUFFERS_NAME,
            LBFP_LOG_BUFFERS_DESC,
            StatType.CUMULATIVE);

    public static final String LBFP_BUFFER_BYTES_NAME =
        "bufferBytes";
    public static final String LBFP_BUFFER_BYTES_DESC =
        "Total memory currently consumed by all log buffers, in bytes.";
    public static final StatDefinition LBFP_BUFFER_BYTES =
        new StatDefinition(
            LBFP_BUFFER_BYTES_NAME,
            LBFP_BUFFER_BYTES_DESC,
            StatType.CUMULATIVE);
}
