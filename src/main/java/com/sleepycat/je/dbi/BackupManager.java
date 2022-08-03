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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.net.URL;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.sleepycat.je.BackupArchiveLocation;
import com.sleepycat.je.BackupFileCopy;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.cleaner.EraserAbortException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.SnapshotManifest.LogFileInfo;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.utilint.CronScheduleParser;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

import org.checkerframework.checker.nullness.qual.Nullable;

/*
 * TODO: Add a JE parameter to control archive verification
 * TODO: Implement archive verification
 * TODO: Write files again if they are old, perhaps instead of verification
 */

/**
 * Manages automatic backups.
 *
 * <p>Automatic backups can scheduled by setting the appropriate environment
 * variables:
 *
 * <ul>
 * <li>{@link EnvironmentConfig#ENV_RUN_BACKUP}
 * <li>{@link EnvironmentConfig#BACKUP_SCHEDULE}
 * <li>{@link EnvironmentConfig#BACKUP_COPY_CLASS}
 * <li>{@link EnvironmentConfig#BACKUP_COPY_CONFIG}
 * <li>{@link EnvironmentConfig#BACKUP_LOCATION_CLASS}
 * <li>{@link EnvironmentConfig#BACKUP_LOCATION_CONFIG}
 * </ul>
 *
 * <p>The backups are performed using the {@link DbBackup} class. Each backup
 * creates a snapshot with hard links to log files from DbBackup.
 *
 * <p>The files are then copied to an archive location. Only new (or newly
 * modified) files are copied for each snapshot. The copy mechanism maintains a
 * manifest for each snapshot that documents its contents and is used to
 * determine which files have already been copied. After a log file is copied,
 * the associated hard link is removed so that the file can be deleted from
 * disk as needed. When a local snapshot is no longer needed, it is deleted.
 *
 * <p>The backup is performed by two separate threads. {@link SnapshotThread}
 * creates JE snapshots on the configured schedule. It runs as its own thread
 * so that backups are performed as close as possible to the scheduled time, to
 * allow backups for different environments to be coordinated. When a snapshot
 * has been created, the snapshot thread wakes up {@link CopyThread}, which
 * copies the snapshot to archive storage. If the copy thread is still running
 * when the snapshot thread creates a new snapshot, the snapshot thread
 * interrupts the copy thread so that it will abandon its work on that snapshot
 * and start work on the new one.
 */
public class BackupManager {

    /**
     * If non-zero, a multiplier to speed the passage of time by the specified
     * factor, for testing only. Some possible settings:
     *
     * <ul>
     * <li> One hour takes a second: 60*60 = 3600
     * <li> One day takes a second: 60*60*24 = 86400
     * <li> One week takes a second: 60*60*24*7 = 604800
     * </ul>
     *
     * <p>Note that the resulting time value needs to be less than
     * Long.MAX_VALUE. With the current time of around 1528719052059 and a max
     * value of 9223372036854775807, that makes the maximum multiplier
     * approximately 6 million. The one week to a second transformation should
     * continue to work as long as we could possibly care about -- until 2453!
     *
     * <p>Waiting for an amount of time that, because of the multiplier, is
     * less than 1 millisecond will result in a 1 millisecond wait. Tests need
     * to account for the fact that seemingly short operations may take a long
     * time in the dilated time scale.
     *
     * <p>Code in this file (and in tests) that needs the current time should
     * call {@link #currentTimeMs}, call {@link #waitMs} to wait, and call
     * {@link #sleepMs} to sleep. These three methods apply the time multiplier
     * if specified.
     */
    public static volatile long timeMultiplier =
        Long.getLong("com.sleepycat.je.test.timeMultiplier", 0);

    /**
     * If non-null, specifies a hook that will be called when creating a new
     * snapshot directory. First the doHook method will be called with the path
     * of the snapshot directory and then the doIOHook method will be called.
     * For testing only.
     */
    static volatile TestHook<Path> createSnapshotHook = null;

    /**
     * If non-null, specifies a hook that will be called when saving a snapshot
     * manifest. First the doHook method will be called with the path of the
     * snapshot directory containing the manifest file, then the doIOHook
     * method will be called. For testing only.
     */
    static volatile TestHook<Path> saveManifestHook = null;

    /**
     * If non-null, specifies a hook that will be called when writing the
     * snapshot info for a snapshot. The doHook method will be called with no
     * arguments.
     */
    static volatile TestHook<Void> writeSnapshotInfoHook = null;

    /**
     * If non-null, specifies a hook that will be called before attempting to
     * copy a log file in copySnapshotFiles. The doHook method will be called
     * for each log file with snapshot's manifest as an argument.
     */
    static volatile TestHook<SnapshotManifest> copySnapshotFileHook = null;

    /**
     * The subdirectory of the environment home directory used to store
     * snapshots. This subdirectory should just contain snapshot directories.
     */
    private static final String SNAPSHOT_SUBDIRECTORY = "snapshots";

    /**
     * The name of the file created in a snapshot directory that records
     * information about the snapshot as represented by an instance of {@link
     * SnapshotInfo}.
     */
    private static final String SNAPSHOT_INFO = "snapInfo.properties";

    /**
     * A regular expression pattern that matches a snapshot directory file
     * name. The format is YYMMDDHH, with each character a decimal digit.
     */
    static final Pattern SNAPSHOT_PATTERN =
        Pattern.compile("\\d\\d[01][0-9][0-3][0-9][0-2]\\d");

    /**
     * The name of the sentinel file created in a snapshot directory when the
     * snapshot is complete.
     */
    private static final String SNAPSHOT_COMPLETE = "snapComplete";

    /**
     * The name of the manifest file created in a snapshot directory to
     * represent the copy status of files in the snapshot.
     */
    public static final String SNAPSHOT_MANIFEST = "manifest.json";

    /** Wait time for soft shutdown of SnapshotThread and CopyThread. */
    private static final int SOFT_SHUTDOWN_WAIT_MS = 3 * 1000;

    /** The initial wait before retrying after an I/O error. */
    private static final long INITIAL_RETRY_WAIT_MS = 1000;

    /** The maximum wait before retrying after an I/O error. */
    private static final long MAX_RETRY_WAIT_MS = 60 * 60 * 1000;

    /**
     * Date formatter to print dates in human readable format.  Synchronize on
     * this object when using it.
     */
    private static final SimpleDateFormat dateFormat =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /**
     * A calendar set to the UTC time zone. Synchronize on this object when
     * using it.
     */
    private static final Calendar utcCalendar =
        Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private final EnvironmentImpl envImpl;
    private final Path envHomeDir;
    private final String nodeName;
    private final Logger logger;

    /* Synchronize when setting all volatile fields */

    private volatile boolean runBackup;
    private volatile String backupSchedule;

    private volatile String backupCopyClass;
    private volatile File backupCopyConfig;

    /** The backup copy implementation or null if not initialized. */
    private volatile BackupFileCopy backupCopy;

    private volatile String backupLocationClass;
    private volatile File backupLocationConfig;

    /** The backup location implementation or null if not initialized. */
    private volatile BackupArchiveLocation backupLocation;

    private volatile boolean shutdownRequested;

    /** Information about scheduled snapshots. */
    private volatile SnapshotTimeInfo snapshotTimeInfo;

    private volatile SnapshotThread snapshotThread;

    private volatile CopyThread copyThread;

    /**
     * Creates an instance of this class for the specified environment.
     *
     * @param envImpl the environment
     * @throws IllegalArgumentException if there is a problem with a
     * configuration property
     */
    public BackupManager(final EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
        envHomeDir = Paths.get(envImpl.getEnvironmentHome().getPath());
        nodeName = getNodeName(envImpl);
        logger = LoggerUtils.getLogger(getClass());
        init();
    }

    /**
     * Initialize from configuration values.
     *
     * @throws IllegalArgumentException if there is a problem with a
     * configuration property
     */
    private synchronized void init() {
        final DbConfigManager configManager = envImpl.getConfigManager();
        runBackup = configManager.getBoolean(EnvironmentParams.ENV_RUN_BACKUP);
        backupSchedule = configManager.get(EnvironmentParams.BACKUP_SCHEDULE);

        backupCopyClass =
            configManager.get(EnvironmentParams.BACKUP_COPY_CLASS);
        backupCopyConfig =
            new File(configManager.get(EnvironmentParams.BACKUP_COPY_CONFIG));

        backupLocationClass =
            configManager.get(EnvironmentParams.BACKUP_LOCATION_CLASS);
        backupLocationConfig =
            new File(
                configManager.get(EnvironmentParams.BACKUP_LOCATION_CONFIG));

        if (timeMultiplier != 0) {
            LoggerUtils.info(logger, envImpl,
                             "Creating snapshots using time multiplier: " +
                             timeMultiplier);
        }
    }

    /**
     * Start associated threads if automatic backups are enabled.
     */
    synchronized void startThreads() {
        if (runBackup && (snapshotThread == null)) {
            snapshotThread = new SnapshotThread(envImpl);
            snapshotThread.start();
        }
    }

    /**
     * Returns the instance to use for copying files, creating and initializing
     * the instance if needed.
     *
     * @throws InterruptedException if the initialize operation is interrupted
     * @throws IllegalArgumentException if the configuration was invalid
     * @throws IOException if some irrecoverable I/O issue was encountered
     * during initialization
     */
    private synchronized BackupFileCopy getBackupCopy()
        throws IOException, InterruptedException {

        if (backupCopy == null) {
            final BackupFileCopy inst = getImplementationInstance(
                BackupFileCopy.class, backupCopyClass);
            inst.initialize(backupCopyConfig);
            backupCopy = inst;
        }
        return backupCopy;
    }

    /**
     * Returns the instance to use for finding archive locations, creating and
     * initializing the instance if needed.
     *
     * @throws InterruptedException if the initialize operation is interrupted
     * @throws IllegalArgumentException if the configuration was invalid
     * @throws IOException if some irrecoverable I/O issue was encountered
     * during initialization
     */
    private synchronized BackupArchiveLocation getBackupLocation()
        throws IOException, InterruptedException {

        if (backupLocation == null) {
            final BackupArchiveLocation inst = getImplementationInstance(
                BackupArchiveLocation.class, backupLocationClass);
            inst.initialize(nodeName, backupLocationConfig);
            backupLocation = inst;
        }
        return backupLocation;
    }

    void initiateSoftShutdown() {
        synchronized (this) {
            if (shutdownRequested) {
                return;
            }
            shutdownRequested = true;
        }
        if (snapshotThread != null) {
            snapshotThread.initiateSoftShutdown();
        }
        if (copyThread != null) {
            copyThread.initiateSoftShutdown();
        }
    }

    void shutdownThreads() {
        synchronized (this) {
            shutdownRequested = true;
        }
        if (snapshotThread != null) {
            snapshotThread.shutdown();
        }
        if (copyThread != null) {
            copyThread.shutdown();
        }
    }

    boolean getShutdownRequested() {
        return shutdownRequested;
    }

    StoppableThread getSnapshotThread() {
        return snapshotThread;
    }

    StoppableThread getCopyThread() {
        return copyThread;
    }

    /*
     * Snapshot Phase
     */

    /**
     * The thread that creates new snapshots. This thread is only created if
     * automatic backups are enabled.
     *
     * @see #run
     */
    private class SnapshotThread extends StoppableThread {
        SnapshotThread(final EnvironmentImpl envImpl) {
            super(envImpl, "JEBackupSnapshot");
        }

        /** Shutdown this thread. */
        void shutdown() {
            if (shutdownDone(logger)) {
                return;
            }
            shutdownThread(logger);
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        protected int initiateSoftShutdown() {
            shutdownRequested = true;
            wakeUp();
            return SOFT_SHUTDOWN_WAIT_MS;
        }

        /**
         * Creates new snapshots.
         *
         * <p>Unless a shutdown is requested, creates the next snapshot if
         * needed, both on start up and at the next scheduled time. Deletes the
         * existing snapshot directory if it is an incomplete snapshot. Retries
         * if I/O errors occur during the process on the assumption that they
         * might be transient, but logs them because those failures are not
         * expected at this level. Retries for I/O errors wait a period of time
         * starting at {@value #INITIAL_RETRY_WAIT_MS} and doubling for each
         * retry up to a maximum value of {@value #MAX_RETRY_WAIT_MS}. Backups
         * are shut down if any other exceptions occur.
         *
         * @see #createSnapshot
         */
        @Override
        public void run() {
            long retryWait = INITIAL_RETRY_WAIT_MS;
            while (!shutdownRequested) {
                try {
                    try {
                        createNextSnapshot();
                        retryWait = INITIAL_RETRY_WAIT_MS;
                        sleepFor(snapshotTimeInfo.next - currentTimeMs());
                    } catch (InterruptedIOException|ClosedByInterruptException
                             e) {
                        /*
                         * Retry immediately in case an interrupt comes while
                         * creating a snapshot
                         */
                    } catch (IOException|EraserAbortException e) {
                        /* Make sure to wake up for the next snapshot */
                        final long wait =
                            Math.min(retryWait,
                                     snapshotTimeInfo.next - currentTimeMs());
                        LoggerUtils.warning(
                            logger, envImpl,
                            "Problem creating snapshot," +
                            " retrying in: " + wait + " ms," +
                            " exception: " + getExceptionStringForLogging(e));
                        sleepFor(wait);
                        retryWait = Math.min(retryWait * 2, MAX_RETRY_WAIT_MS);
                    }
                } catch (InterruptedException e) {
                    /* Retry immediately */
                } catch (RuntimeException e) {
                    if (envImpl.isValid()) {
                        LoggerUtils.severe(
                            logger, envImpl,
                            "Shutting down backups because of unexpected" +
                            " exception when creating snapshot: " +
                            LoggerUtils.getStackTrace(e));
                    }
                    initiateSoftShutdown();
                    break;
                } catch (Error e) {
                    if (envImpl.isValid()) {
                        StoppableThread.handleUncaughtException(
                            logger, envImpl, this, e);
                    }
                    initiateSoftShutdown();
                    break;
                }
            }
        }

        private void sleepFor(final long delay)
            throws InterruptedException {

            if (delay > 0) {
                sleepMs(delay);
            }
        }

        /**
         * Wake up the thread. Call this method when the backup configuration
         * changes.
         */
        private void wakeUp() {

            /*
             * Use interrupt rather than wait because it is simpler to depend
             * on the fact that the thread interrupted status will cause
             * subsequent I/O operations to fail.
             *
             * Note that JE currently does not really support interrupts, and
             * may treat them as cause to invalidate the environment. In the
             * long run, we expect to JE handle interrupts. In the meantime, we
             * only interrupt this thread on shutdown, the chance that it is
             * performing a JE operation at that time is very low because
             * DbBackup operations are typically quick, and the consequences of
             * invalidating the environment on shutdown are minimal, so no
             * change is needed.
             */
            interrupt();
        }
    }

    /**
     * Stores information about snapshot times. Using a class for this to make
     * it easier to coordinate the values with each other.
     */
    static class SnapshotTimeInfo {

        /** The schedule. */
        private final String schedule;

        /** The absolute time in milliseconds of the next snapshot. */
        final long next;

        /** The interval in milliseconds between snapshots. */
        private final long interval;

        /** The absolute time in milliseconds of the previous snapshot. */
        final long previous;

        /**
         * Computes information about snapshot times from the schedule
         * specified in crontab format.
         *
         * @param schedule the schedule in crontab format
         * @throws IllegalArgumentException if the schedule format is invalid
         */
        SnapshotTimeInfo(final String schedule) {
            this.schedule = schedule;
            final CronScheduleParser parser =
                createSnapshotScheduleParser(schedule);
            next = parser.getTime() + parser.getDelayTime();
            interval = parser.getInterval();
            previous = next - interval;
        }

        private SnapshotTimeInfo(final String schedule,
                                 final long next,
                                 final long interval) {
            this.schedule = schedule;
            this.next = next;
            this.interval = interval;
            this.previous = next - interval;
        }

        /**
         * Returns possibly new information for the current time and specified
         * schedule.
         *
         * @param newSchedule the new schedule
         * @return the current snapshot time info
         * @throws IllegalArgumentException if the schedule format is invalid
         */
        SnapshotTimeInfo update(final String newSchedule) {
            if (!newSchedule.equals(schedule)) {
                return new SnapshotTimeInfo(newSchedule);
            }
            final long now = currentTimeMs();
            if (next > now) {
                return this;
            }
            long newNext = next + interval;
            while (newNext < now) {
                newNext += interval;
            }
            return new SnapshotTimeInfo(newSchedule, newNext, interval);
        }

        @Override
        public String toString() {
            return "SnapshotTimeInfo[" +
                "schedule='" + schedule + "'" +
                " next=" + next + "(" + formatTime(next) + ")" +
                " interval=" + interval +
                " previous=" + previous + "(" + formatTime(previous) + ")" +
                "]";
        }
    }

    /**
     * Information recorded about a snapshot. The fields have the same meaning
     * as in the SnapshotManifest class, and are the source of those values.
     */
    private static class SnapshotInfo {
        final long startTimeMs;
        final long endOfLog;
        final boolean isMaster;

        SnapshotInfo(final long startTimeMs,
                     final long endOfLog,
                     final boolean isMaster) {
            this.startTimeMs = startTimeMs;
            this.endOfLog = endOfLog;
            this.isMaster = isMaster;
        }

        /**
         * Serialize an instance into bytes in Java properties format, using
         * UTF-8 encoding.
         *
         * @return the serialized form
         */
        byte[] serialize() {
            final Properties props = new Properties();
            props.setProperty("startTimeMs", String.valueOf(startTimeMs));
            props.setProperty("endOfLog", String.valueOf(endOfLog));
            props.setProperty("isMaster", String.valueOf(isMaster));
            try {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                props.store(new OutputStreamWriter(baos, UTF_8),
                            "SnapshotInfo");
                return baos.toByteArray();
            } catch (IOException e) {
                throw new IllegalStateException(
                    "Unexpected problem serializing snapshot info: " +
                    e.getMessage(),
                    e);
            }
        }

        /**
         * Return an instance by deserializing the specified bytes.
         *
         * @param bytes the serialized bytes
         * @return an instance of this class
         * @throws IOException if the format of the input is invalid
         */
        static SnapshotInfo deserialize(final byte[] bytes)
            throws IOException {

            final Properties props = new Properties();
            props.load(
                new InputStreamReader(new ByteArrayInputStream(bytes), UTF_8));
            try {
                return new SnapshotInfo(
                    Long.parseLong(props.getProperty("startTimeMs", "")),
                    Long.parseLong(props.getProperty("endOfLog", "")),
                    Boolean.parseBoolean(props.getProperty("isMaster")));
            } catch (IllegalArgumentException e) {
                throw new IOException("Problem deserializing snapshot info: " +
                                      e.getMessage(),
                                      e);
            }
        }
    }

    /**
     * Processes information in the specified crontab formatted schedule.
     *
     * @param cronSchedule the schedule
     * @return information about the schedule
     * @throws IllegalArgumentException if the schedule is illegal
     */
    public static CronScheduleParser createSnapshotScheduleParser(
        final String cronSchedule) {
        if (!cronSchedule.startsWith("0")) {
            throw new IllegalArgumentException(
                "Schedule must start with '0': " + cronSchedule);
        }
        synchronized (utcCalendar) {
            utcCalendar.setTimeInMillis(currentTimeMs());
            return new CronScheduleParser(cronSchedule, utcCalendar);
        }
    }

    /**
     * Create the next snapshot if needed, first deleting the snapshot
     * directory if the snapshot is incomplete, and then notify the copy
     * thread.
     *
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     * @see #createSnapshot
     */
    private void createNextSnapshot()
        throws IOException, InterruptedException {

        final Path snapshotsSubdir = envHomeDir.resolve(SNAPSHOT_SUBDIRECTORY);
        if (Files.notExists(snapshotsSubdir)) {
            Files.createDirectory(snapshotsSubdir);
        }
        updateSnapshotTimeInfo();
        final Path snapshotDir = getSnapshotDir(snapshotTimeInfo.previous);
        if (!isSnapshotComplete(snapshotDir)) {
            deleteSnapshot(snapshotDir);
            createSnapshot(snapshotDir);
        }
        wakeUpCopyThread();
    }

    /**
     * Updates snapshotTimeInfo based on the configured schedule.
     *
     * @throws IllegalArgumentException if the schedule format is invalid
     */
    private synchronized void updateSnapshotTimeInfo() {
        snapshotTimeInfo = (snapshotTimeInfo == null) ?
            new SnapshotTimeInfo(backupSchedule) :
            snapshotTimeInfo.update(backupSchedule);
    }

    /**
     * Returns the path of the snapshot directory for the specified time, which
     * has the format
     * <i>EnvHomeDirectory</i>/{@value #SNAPSHOT_SUBDIRECTORY}/yymmddhh</i>:
     *
     * <ul>
     * <li> yy - Year mod 100
     * <li> mm - 1-based month
     * <li> dd - 1-based day of month
     * <li> hh - Hour in 24 hour time
     * <ul>
     *
     * The time is computed in the UTC time zone.
     */
    private Path getSnapshotDir(final long timeMs) {
        return getSnapshotDir(timeMs, envHomeDir);
    }

    /** Static version of getSnapshotDir, for testing. */
    static Path getSnapshotDir(final long timeMs, final Path envHomeDir) {
        final String snap;
        synchronized (utcCalendar) {
            utcCalendar.setTimeInMillis(timeMs);
            snap = String.format("%02d%02d%02d%02d",
                                 utcCalendar.get(Calendar.YEAR) % 100,
                                 utcCalendar.get(Calendar.MONTH) + 1,
                                 utcCalendar.get(Calendar.DAY_OF_MONTH),
                                 utcCalendar.get(Calendar.HOUR_OF_DAY));
        }
        return envHomeDir.resolve(Paths.get(SNAPSHOT_SUBDIRECTORY, snap));
    }

    /**
     * Converts a snapshot name to a time.
     *
     * @param snapshot the name of the snapshot
     * @return the absolute time in milliseconds
     * @throws IllegalArgumentException if the snapshot format is illegal
     */
    static long getSnapshotTimeMs(final String snapshot) {
        if (snapshot.length() != 8) {
            throw new IllegalArgumentException(
                "Wrong length for snapshot: '" + snapshot + "'");
        }
        final int year = Integer.valueOf(snapshot.substring(0, 2));
        final int month = Integer.valueOf(snapshot.substring(2, 4));
        final int day = Integer.valueOf(snapshot.substring(4, 6));
        final int hour = Integer.valueOf(snapshot.substring(6, 8));
        synchronized (utcCalendar) {
            utcCalendar.setTimeInMillis(currentTimeMs());
            final int century = 100 * (utcCalendar.get(Calendar.YEAR) / 100);
            try {
                utcCalendar.set(Calendar.YEAR, year + century);
                utcCalendar.set(Calendar.MONTH, month - 1);
                utcCalendar.set(Calendar.DAY_OF_MONTH, day);
                utcCalendar.set(Calendar.HOUR_OF_DAY, hour);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Bad format for snapshot: '" + snapshot + "': " +
                    e.getMessage(),
                    e);
            }
            return utcCalendar.getTimeInMillis();
        }
    }

    /** Returns whether the snapshot was completed successfully. */
    private boolean isSnapshotComplete(final Path snapshotDir) {
        return Files.exists(snapshotDir.resolve(SNAPSHOT_COMPLETE));
    }

    /**
     * Creates a snapshot with the specified directory path. Snapshots are
     * created in the {@link #SNAPSHOT_SUBDIRECTORY} subdirectory of the
     * environment home directory. Each snapshot is stored in a subdirectory
     * named YYMMHHDD as described in {@link #getSnapshotDir}.
     *
     * <p>The snapshot directory contains the following files:
     *
     * <ul>
     * <li>{@value #SNAPSHOT_INFO} - information about the snapshot, as
     * written by {@link #writeSnapshotInfo}
     * <li>*.jdb - hard links to the log files that represent the data being
     * backed up
     * <li>{@value #SNAPSHOT_COMPLETE} - a sentinel file, whose presence means
     * that the directory represents a complete snapshot
     * </ul>
     *
     * <p>Note that the caller deletes the snapshot directory before calling
     * this method, so an existing directory or file within the directory is
     * unexpected.
     *
     * @param snapshotDir the path of the snapshot directory
     * @throws IllegalStateException if the directory already exists or a file
     * in the directory is found to already exist
     * @throws IOException if an I/O error occurs other than an unexpected
     * existing file
     * @throws InterruptedException if the operation is interrupted
     */
    private void createSnapshot(final Path snapshotDir)
        throws IOException, InterruptedException {

        if (logger.isLoggable(Level.FINE)) {
            LoggerUtils.fine(logger, envImpl,
                             "Creating snapshot: " +
                             snapshotDir.getFileName() +
                             ", now: " + formatTime(currentTimeMs()));
        }

        final long startTimeMs = currentTimeMs();

        assert TestHookExecute.doHookIfSet(createSnapshotHook, snapshotDir);
        assert TestHookExecute.doIOHookIfSet(createSnapshotHook);

        try {
            Files.createDirectory(snapshotDir);
        } catch (FileAlreadyExistsException e) {
            throw new IllegalStateException(
                "Snapshot directory should not already exist: " + snapshotDir,
                e);
        }

        final DbBackup dbBackup = new DbBackup(envImpl);
        dbBackup.startBackup();
        try {

            /*
             * We use the backup start time to detect if log files are erased
             * between the backup and when they are copied. Files in the backup
             * cannot be erased between startBackup and endBackup calls. The
             * last file in the snapshot is modified as part of the startBackup
             * call, so we need to use a backup start time that shows that file
             * as not modified. Since file modification times may only have 1
             * second accuracy, we add 1 second to modification times when
             * comparing them to the start time. To make sure the final file
             * does not appear modified, we need to use a start time 1 second
             * after that file was modified.
             */
            final long backupStartTimeMs = currentTimeMs() + 1000;
            writeSnapshotInfo(backupStartTimeMs, snapshotDir);

            /* Create hard links to all of the log files */
            final String[] logFiles = dbBackup.getLogFilesInSnapshot();
            for (final String logFile : logFiles) {

                /*
                 * Log files are relative to the environment home directory and
                 * will have "dataNNN/" prefixes if multiple data directories
                 * are being used.
                 */
                final Path logFilePath = Paths.get(logFile);
                Files.createLink(
                    snapshotDir.resolve(logFilePath.getFileName()),
                    envHomeDir.resolve(logFilePath));
            }

            /*
             * Force the directory to disk after creating the links to be sure
             * they are durable before creating SNAPSHOT_COMPLETE.
             */
            forceFile(snapshotDir);
            Files.createFile(snapshotDir.resolve(SNAPSHOT_COMPLETE));

            /*
             * Force afterwards to make sure the snapshot completion is durable
             * before being used.
             */
            forceFile(snapshotDir);

            /*
             * Don't release the backup lock until we reach the start time, so
             * we can be sure files are not erased until after that time. To
             * reduce the wait time, we have performed the hard links in the
             * meantime, but we still might need to wait for the rest of the
             * second.
             */
            final long startWaitTime =
                Math.max(backupStartTimeMs - currentTimeMs(), 0);
            if (startWaitTime > 0) {
                sleepMs(startWaitTime);
            }

            final long creationTimeMs = currentTimeMs() - startTimeMs;
            LoggerUtils.info(logger, envImpl,
                             "Created snapshot: " + snapshotDir.getFileName() +
                             ", number of log files: " + logFiles.length +
                             ", creation time: " + creationTimeMs + " ms" +
                             ", start wait time: " + startWaitTime + " ms");
        } catch (FileAlreadyExistsException e) {
            throw new IllegalStateException(
                "Snapshot directory should not already contain file: " +
                e.getFile(),
                e);
        } finally {
            dbBackup.endBackup();
        }
    }

    /**
     * Force any pending changes to disk for the file with the specified
     * pathname.  Use this on the directory path to force directory changes
     * for renames or deletes.
     *
     * @param path the file path
     * @throws IOException if an I/O failure occurs
     *
     * TODO: Move this method to some je utility package where it would be
     * available for more general use.
     */
    public static void forceFile(final Path path)
        throws IOException {

        try (final FileChannel channel = FileChannel.open(path)) {
            channel.force(true);
        }
    }

    /**
     * Wake up the copy thread, or start it if it isn't running yet, unless a
     * shutdown has been requested. We start or wake up the copy thread when
     * there is a next snapshot completed and ready to copy.
     */
    private synchronized void wakeUpCopyThread() {
        if (shutdownRequested) {
            return;
        }
        LoggerUtils.fine(logger, envImpl, "Waking up snapshot copy thread");
        if (copyThread == null) {
            copyThread = new CopyThread(envImpl);
            copyThread.start();
        } else {
            copyThread.wakeUp();
        }
    }

    /**
     * Writes information about the snapshot to a properties file in the
     * snapshot directory. This operation should be performed as soon as the
     * {@link DbBackup} is started so that the information is as accurate as
     * possible.
     *
     * @param backupStartTimeMs the start time of the backup
     * @param snapshotDir the path of the snapshot directory
     * @throws IOException if an I/O error occurs
     */
    private void writeSnapshotInfo(final long backupStartTimeMs,
                                   final Path snapshotDir)
        throws IOException {

        /*
         * TODO: Use the last VLSN for the last log file in the backup, which
         * might be different than the current latest log file.
         */
        final SnapshotInfo info = new SnapshotInfo(backupStartTimeMs,
                                                   envImpl.getEndOfLog(),
                                                   envImpl.getIsMaster());
        assert TestHookExecute.doHookIfSet(writeSnapshotInfoHook);
        final Path snapshotInfo = snapshotDir.resolve(SNAPSHOT_INFO);
        Files.write(snapshotInfo, info.serialize(),
                    StandardOpenOption.CREATE_NEW);
        forceFile(snapshotInfo);
    }

    /**
     * Returns information about the snapshot from a properties file in the
     * snapshot directory.
     *
     * @param snapshotDir the path of the snapshot directory
     * @return information about the snapshot
     * @throws IOException if an I/O error occurs, including if the file is
     * missing or its format is invalid
     */
    private SnapshotInfo readSnapshotInfo(final Path snapshotDir)
        throws IOException {

        return SnapshotInfo.deserialize(
            Files.readAllBytes(snapshotDir.resolve(SNAPSHOT_INFO)));
    }

    /*
     * Incremental Copy Phase
     */

    /**
     * The thread that performs incremental copies of log files. Use a separate
     * thread from the snapshot thread so that the snapshots can be run on time
     * even if the copy thread is busy doing a copy and doesn't respond quickly
     * to an interrupt.
     *
     * @see #run
     */
    private class CopyThread extends StoppableThread {
        CopyThread(final EnvironmentImpl envImpl) {
            super(envImpl, "JEBackupCopy");
        }

        void shutdown() {
            if (shutdownDone(logger)) {
                return;
            }
            shutdownThread(logger);
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        protected int initiateSoftShutdown() {
            shutdownRequested = true;
            wakeUp();
            return SOFT_SHUTDOWN_WAIT_MS;
        }

        /**
         * Copies snapshots to an archive location and deletes snapshots that
         * are no longer needed.
         *
         * <p>Unless a shutdown is requested, copies the latest snapshot if
         * needed, both on start up and when interrupted by the snapshot
         * thread. Retries any I/O errors and considers them to be expected
         * since they likely represent remote communication errors encountered
         * during copying. Retries for I/O errors wait a period of time
         * starting at {@value #INITIAL_RETRY_WAIT_MS} and doubling for each
         * retry up to a maximum value of {@value #MAX_RETRY_WAIT_MS}. Backups
         * are shut down if any other exceptions occur.
         *
         * @see #copySnapshot
         */
        @Override
        public void run() {
            long retryWait = INITIAL_RETRY_WAIT_MS;
            while (!shutdownRequested) {
                boolean waitingToRetry = false;
                try {
                    try {
                        copyLatestSnapshot();
                        retryWait = INITIAL_RETRY_WAIT_MS;
                        /* Wait until the snapshot thread wakes us up */
                        try {
                            sleepFor(Long.MAX_VALUE);
                        } catch (InterruptedException e) {
                            /* Retry immediately */
                        }
                    } catch (ClosedByInterruptException|InterruptedIOException
                             e) {
                        throw e;
                    } catch (IOException e) {
                        LoggerUtils.info(
                            logger, envImpl,
                            "Problem copying snapshot" +
                            ", retrying in: " + retryWait + " ms" +
                            ", exception: " + getExceptionStringForLogging(e));
                        waitingToRetry = true;
                        sleepFor(retryWait);
                        retryWait = Math.min(retryWait * 2, MAX_RETRY_WAIT_MS);
                    }
                } catch (ClosedByInterruptException|
                         InterruptedIOException|
                         InterruptedException e) {
                    /*
                     * Acknowledge that we've handled the interrupt for the
                     * IOException cases.
                     */
                    interrupted();
                    final String msg = "Interrupted while" +
                        (waitingToRetry ? " waiting to retry" : "") +
                        " copying snapshot, exception: " +
                        getExceptionStringForLogging(e);
                    LoggerUtils.fine(logger, envImpl, msg);
                    /*
                     * Retry immediately, and reset the retry wait because this
                     * should be a new snapshot.
                     */
                    retryWait = INITIAL_RETRY_WAIT_MS;
                } catch (RuntimeException e) {
                    if (envImpl.isValid()) {
                        LoggerUtils.severe(
                            logger, envImpl,
                            "Shutting down backups because of unexpected" +
                            " exception when copying snapshot: " +
                            LoggerUtils.getStackTrace(e));
                    }
                    initiateSoftShutdown();
                    break;
                } catch (Error e) {
                    if (envImpl.isValid()) {
                        StoppableThread.handleUncaughtException(
                            logger, envImpl, this, e);
                    }
                    initiateSoftShutdown();
                    break;
                }
            }
        }

        private void sleepFor(final long delay)
            throws InterruptedException {

            if (delay > 0) {
                sleepMs(delay);
            }
        }

        /**
         * Interrupt the thread. Call this method when the backup configuration
         * changes or there is a new snapshot.
         */
        synchronized void wakeUp() {
            interrupt();
        }
    }

    /**
     * Copies the latest snapshot, deletes any older incomplete snapshots or
     * ones without manifests, and deletes all older snapshots after capturing
     * any needed manifest information.
     *
     * @throws IOException if an I/O failure occurs
     * @throws InterruptedException if the operation is interrupted
     */
    private void copyLatestSnapshot()
        throws IOException, InterruptedException {

        final LatestSnapshotInfo info = getLatestSnapshotInfo();
        copySnapshot(info.snapshotDir, info.parentSnapshotDir, info.parent);
    }

    /** Return value of getLatestSnapshotInfo. */
    private static class LatestSnapshotInfo {
        Path snapshotDir;
        Path parentSnapshotDir;
        SnapshotManifest parent;
    }

    /**
     * Gets information about the current snapshot and the previous (parent)
     * snapshot, if any. Deletes any older snapshots, including ones that are
     * incomplete, and ones that lack manifest files since they represent
     * intermediate snapshots that were not copied.
     *
     * @return information about the latest snapshot
     * @throws IOException if an I/O error occurs
     */
    private LatestSnapshotInfo getLatestSnapshotInfo()
        throws IOException {

        final LatestSnapshotInfo info = new LatestSnapshotInfo();
        withSnapshots(
            s -> s.sorted(Collections.reverseOrder())
            .forEach(
                p -> {
                    try {

                        /* The first snapshot is the current one */
                        if (info.snapshotDir == null) {
                            info.snapshotDir = p;
                            return;
                        }

                        /*
                         * Delete incomplete snapshots. Note that this snapshot
                         * cannot be one that is in the process of being
                         * created because that snapshot must be the latest
                         * one, and this one isn't the latest.
                         */
                        if (!isSnapshotComplete(p)) {
                            deleteSnapshot(p);
                            return;
                        }

                        final SnapshotManifest manifest = getManifest(p);
                        if (manifest == null) {

                            /*
                             * Delete snapshots without manifests because
                             * they did not have any copying done.
                             */
                            deleteSnapshot(p);
                        } else if (info.parentSnapshotDir == null) {

                            /*
                             * Use the newest snapshot with a manifest as a
                             * parent.  Since we always create the manifest
                             * first, this manifest contains information about
                             * all earlier files.
                             */
                            info.parentSnapshotDir = p;
                            info.parent = manifest;
                        } else {

                            /*
                             * Delete all other snapshots, since the parent
                             * snapshot contains all the information about
                             * previously copied log files that we need.
                             */
                            deleteSnapshot(p);
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }));
        return info;
    }

    /**
     * Copies the specified snapshot.
     *
     * <p>If the snapshot isn't complete or present, then return and expect to
     * be called again when the snapshot thread notifies the copy thread that
     * the snapshot is ready.
     *
     * <p>Otherwise, if not already present, creates a manifest that represents
     * all of the files in the snapshot. Which files need to be copied depends
     * both on the log files in this snapshot and on the "parent" snapshot: the
     * most recent previous snapshot that contains a manifest file. The parent
     * manifest records information about any files in this snapshot that have
     * already been copied. Once the manifest for the new snapshot has been
     * created and copied, the previous snapshot is no longer needed and is
     * deleted. If there is no parent snapshot, then all of the log files in
     * the current snapshot are marked as not copied.
     *
     * <p>Then, goes through all of the files in the snapshot, performing
     * whatever work is needed. Each log file is copied if it either was not
     * marked as copied in a previous snapshot or has been modified since it
     * was copied. The behavior after copying the file depends on whether the
     * file has been modified by erasure after the snapshot started. If the
     * file was not modified, then the entry in snapshotFiles is updated. If
     * the file was modified, then the entry for the file in snapshotFiles, if
     * any, is left in place, and an entry is added to erasedFiles using the
     * checksum of the data transferred during the copy.
     *
     * <p>Once all files have been processed, unless a file was erased that was
     * not previously copied, the manifest is marked as complete.
     *
     * @param snapshotDir the path of the snapshot directory
     * @param parentSnapshotDir the path of the parent snapshot directory or
     * null
     * @param parent the manifest of the parent snapshot or null
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    private void copySnapshot(@Nullable final Path snapshotDir,
                              @Nullable final Path parentSnapshotDir,
                              @Nullable final SnapshotManifest parent)
        throws IOException, InterruptedException {

        /*
         * Don't start copying the snapshot if it isn't present or isn't
         * complete, say if the copy thread woke up while the snapshot thread
         * was creating the snapshot. Wait until the snapshot thread notifies
         * us that the snapshot is ready.
         */
        if ((snapshotDir == null) || !isSnapshotComplete(snapshotDir)) {
            return;
        }

        SnapshotManifest base = getManifest(snapshotDir);
        if (base == null) {
            base = createNewSnapshotManifest(snapshotDir, parent);
            saveManifest(base, snapshotDir);
            if (parentSnapshotDir != null) {
                deleteSnapshot(parentSnapshotDir);
            }
        } else if (base.getIsComplete()) {
            LoggerUtils.finer(logger, envImpl,
                              "Latest snapshot is already complete: " +
                              snapshotDir.getFileName());

            /*
             * Check to be sure the parent snapshot has been deleted, in case
             * the backup was interrupted after creating an initial manifest
             * that was also complete, but before deleting the parent.
             */
            if (parentSnapshotDir != null) {
                deleteSnapshot(parentSnapshotDir);
            }
            return;
        }
        copySnapshotFiles(base, snapshotDir);
    }

    /**
     * Gets information about a log file.
     *
     * @param logFile the name of the log file
     * @param snapshotDir the path of the containing snapshot
     * @param parentSnapshotFiles information about snapshot files in the
     * parent snapshot or null if no parent
     * @param startTimeMs the start time for this snapshot
     * @return information about the log file
     * @throws IOException if an I/O error occurs
     */
    private LogFileInfo getLogFileInfo(
        final String logFile,
        final Path snapshotDir,
        @Nullable final Map<String, LogFileInfo> parentSnapshotFiles,
        final long startTimeMs)
        throws IOException {

        final LogFileInfo parentInfo = (parentSnapshotFiles != null) ?
            parentSnapshotFiles.get(logFile) :
            null;

        /*
         * If the log file was copied in the parent and was not modified before
         * the start time of this snapshot, then return the parent info. We may
         * find later that the file was modified after the snapshot started,
         * but then we will treat it as an erased file.
         */
        if ((parentInfo != null) && parentInfo.getIsCopied()) {
            final long logFileModTimeMs =
                fileLastModifiedTimeMs(snapshotDir.resolve(logFile));
            if (logFileModTimeMs < startTimeMs) {
                return parentInfo;
            }
            final String msg =
                "Detected erasure since last copy of log file: " + logFile +
                ", log file modify time: " + formatTime(logFileModTimeMs) +
                ", snapshot start time: " + formatTime(startTimeMs);
            LoggerUtils.info(logger, envImpl, msg);
        }

        /* New information for log file that needs to be copied */
        return new LogFileInfo(getSnapshotName(snapshotDir), nodeName);
    }

    /**
     * Returns the last modified time of the specified file in absolute
     * milliseconds, adjusted as needed for {@link #timeMultiplier}. Since file
     * modification times are only accurate to a second in some cases (Linux
     * ext4 at least), add one second to the modification time so that it can
     * be safely compared to absolute times that have millisecond accuracy.
     *
     * @param file the file
     * @return the last modified time in milliseconds
     * @throws IOException if an I/O error occurs
     */
    static long fileLastModifiedTimeMs(final Path file)
        throws IOException {

        final long modTime = Files.getLastModifiedTime(file).toMillis();
        return (timeMultiplier == 0) ?
            modTime + 1000 :

            /* Add the second after scaling so we get a scaled second */
            (modTime * timeMultiplier) + 1000;
    }

    /**
     * Copy the files in the snapshot, including updating and copying the
     * manifest when it is modified.
     *
     * @param base the manifest for updating after the copy
     * @param snapshotDir the path of the snapshot directory
     * @throws IOException if an I/O failure occurs
     * @throws InterruptedException if the operation is interrupted
     */
    private void copySnapshotFiles(SnapshotManifest base,
                                   final Path snapshotDir)
        throws IOException, InterruptedException {

        if (logger.isLoggable(Level.FINE)) {
            LoggerUtils.fine(logger, envImpl,
                             "Copying snapshot: " + snapshotDir.getFileName() +
                             ", now: " + formatTime(currentTimeMs()));
        }
        int copied = 0;
        int erased = 0;
        boolean cannotComplete = false;
        final long startTimeMs = currentTimeMs();
        for (final Entry<String, LogFileInfo> e :
                 base.getSnapshotFiles().entrySet()) {
            final String logFile = e.getKey();
            final LogFileInfo info = e.getValue();
            final Path logFilePath = snapshotDir.resolve(logFile);

            assert TestHookExecute.doHookIfSet(copySnapshotFileHook, base);

            /*
             * If the file was copied and not modified since the copy started,
             * then it doesn't need to be copied again and can be deleted. If
             * the file was copied but is not present, then we are presumably
             * resuming copying this snapshot and the file was deleted on an
             * earlier pass.
             */
            if (info.getIsCopied()) {
                if (Files.notExists(logFilePath)) {
                    continue;
                }
                if (fileLastModifiedTimeMs(logFilePath) <
                    info.getCopyStartTimeMs()) {
                    Files.delete(logFilePath);
                    continue;
                }
            }

            /*
             * If the file was already copied as an erased file, it also
             * doesn't need to be copied again and can be deleted if it hasn't
             * been already.
             */
            final LogFileInfo erasedInfo = base.getErasedFiles().get(logFile);
            if ((erasedInfo != null) && erasedInfo.getIsCopied()) {
                Files.deleteIfExists(logFilePath);
                continue;
            }

            /*
             * Otherwise, copy the file, update the manifest, and delete the
             * file
             */
            base = copyLogFile(logFilePath, base);
            copied++;
            if (base.getErasedFiles().containsKey(logFile)) {
                erased++;
            }
            LogFileInfo snapshotInfo =
                base.getSnapshotFiles().get(logFile);
            if ((snapshotInfo == null) || !snapshotInfo.getIsCopied()) {

                /*
                 * The manifest can't be marked complete if there is a file
                 * that was erased during the snapshot and wasn't copied in
                 * a previous snapshot.
                 */
                cannotComplete = true;
            }
            saveManifest(base, snapshotDir);
            Files.delete(logFilePath);
        }
        if (!cannotComplete) {
            final SnapshotManifest newManifest =
                new SnapshotManifest.Builder(base)
                .setIsComplete(true)
                .build();
            saveManifest(newManifest, snapshotDir);
        }
        final long copyTimeMs = currentTimeMs() - startTimeMs;
        envImpl.noteBackupCopyFilesMs(copyTimeMs);
        envImpl.noteBackupCopyFilesCount(copied);
        final String msg =
            "Done copying snapshot: " + snapshotDir.getFileName() +
            ", complete: " + !cannotComplete +
            ", copied files: " + copied +
            ", erased files: " + erased +
            ", copy time: " + copyTimeMs + " ms";
        LoggerUtils.info(logger, envImpl, msg);
    }

    /**
     * Creates and returns a new manifest for the specified snapshot.
     *
     * @param snapshotDir the path of the snapshot directory
     * @param parent the manifest of the parent snapshot or null
     * @throws IOException if an I/O error occurs
     */
    private SnapshotManifest createNewSnapshotManifest(
        final Path snapshotDir, @Nullable final SnapshotManifest parent)
        throws IOException {

        final SnapshotManifest.Builder newManifest =
            new SnapshotManifest.Builder();
        final SnapshotInfo snapshotInfo = readSnapshotInfo(snapshotDir);
        newManifest
            .setSequence((parent != null) ? parent.getSequence() + 1 : 1)
            .setSnapshot(getSnapshotName(snapshotDir))
            .setStartTimeMs(snapshotInfo.startTimeMs)
            .setNodeName(nodeName)
            .setEndOfLog(snapshotInfo.endOfLog)
            .setIsMaster(snapshotInfo.isMaster);
        final Map<String, LogFileInfo> parentSnapshotFiles =
            (parent != null) ? parent.getSnapshotFiles() : null;
        final SortedMap<String, LogFileInfo> snapshotFiles =
            newManifest.getSnapshotFiles();
        withLogFiles(
            snapshotDir,
            s -> s.forEach(
                p -> {
                    final String logFile = getFileNameString(p);
                    try {
                        snapshotFiles.put(
                            logFile,
                            getLogFileInfo(logFile, snapshotDir,
                                           parentSnapshotFiles,
                                           snapshotInfo.startTimeMs));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }));
        return newManifest.build();
    }

    /**
     * Copy a log file to the archive, if needed, retrying after I/O errors.
     * Creates a new manifest based on the one specified that represents the
     * addition of the newly copied file.
     *
     * @param logFilePath the path of the log file
     * @param base the current manifest
     * @return the new manifest with the newly copied file
     * @throws IOException if an I/O failure occurs
     * @throws InterruptedException if the operation is interrupted
     */
    private SnapshotManifest copyLogFile(final Path logFilePath,
                                         final SnapshotManifest base)
        throws IOException, InterruptedException {

        LoggerUtils.fine(logger, envImpl,
                         "Copying snapshot log file: " +
                         logFilePath.getFileName());

        final SnapshotManifest.Builder newManifest =
            new SnapshotManifest.Builder(base);
        final long copyStartTimeMs = currentTimeMs();
        final String checksum = copyFile(logFilePath, base.getSnapshot());
        newManifest.setLastFileCopiedTimeMs(currentTimeMs());
        final long logFileModifyTimeMs = fileLastModifiedTimeMs(logFilePath);
        final boolean isErased = logFileModifyTimeMs > base.getStartTimeMs();
        final String logFile = getFileNameString(logFilePath);
        final LogFileInfo logFileInfo =
            new LogFileInfo(checksum, copyStartTimeMs, base);
        if (isErased) {

            /*
             * Add an erased entry. If the file was already copied in the
             * parent, then leave that entry in snapshotFiles.
             */
            final String msg =
                "Detected erasure during copy of log file: " + logFile +
                ", log file modify time: " + formatTime(logFileModifyTimeMs) +
                ", snapshot start time: " + formatTime(base.getStartTimeMs());
            LoggerUtils.info(logger, envImpl, msg);
            newManifest.getErasedFiles().put(logFile, logFileInfo);
        } else {

            /*
             * Add snapshot file entry, replacing a previous one if we are
             * copying the file again so that it reflects the current snapshot.
             */
            newManifest.getSnapshotFiles().put(logFile, logFileInfo);
        }
        return newManifest.build();
    }

    /**
     * Copies a local file to the archive, retrying on I/O errors.
     *
     * @param path the local path of the file
     * @param snapshot the name of the snapshot in YYMMDDHH format
     * @return the file checksum in hex
     * @throws IOException if an I/O failure occurs
     * @throws InterruptedException if the operation is interrupted
     */
    private String copyFile(final Path path, final String snapshot)
        throws IOException, InterruptedException {

        return copyFile(path, getFileNameString(path), snapshot);
    }

    /**
     * Copies a local file to the archive using the specified name, retrying on
     * I/O errors.
     *
     * @param path the local path of the file
     * @param archiveFileName the name of the file in the archive
     * @param snapshot the name of the snapshot in YYMMDDHH format
     * @return the file checksum in hex
     * @throws IOException if an I/O failure occurs
     * @throws InterruptedException if the operation is interrupted
     */
    private String copyFile(final Path path,
                            final String archiveFileName,
                            final String snapshot)
        throws IOException, InterruptedException {

        final URL archiveFile = getBackupLocation().getArchiveLocation(
            snapshot + "/" + archiveFileName);
        long retryWait = INITIAL_RETRY_WAIT_MS;
        while (true) {
            try {
                final byte[] checksum =
                    getBackupCopy().copy(path.toFile(), archiveFile);
                return checksumToHex(checksum);
            } catch (ClosedByInterruptException|InterruptedIOException e) {
                /* Don't retry if interrupted */
                throw e;
            } catch (IOException e) {
                LoggerUtils.info(
                    logger, envImpl,
                    "Problem copying snapshot file: " + path +
                    ", retrying in: " + retryWait + " ms" +
                    ", exception: " + getExceptionStringForLogging(e));
                /* Don't retry if the wait is interrupted */
                sleepMs(retryWait);
                retryWait = Math.min(retryWait * 2, MAX_RETRY_WAIT_MS);
            }
        }
    }

    /**
     * Saves a new manifest file for the specified snapshot directory, making
     * sure that the change is atomic and durable in face of failure, and that
     * the manifest is first copied successfully to the archive.
     *
     * <p>Here are the steps used to make sure that the manifest is saved
     * atomically:
     *
     * <ol>
     *
     * <li>If there is an existing manifest.properties.new, it is an incomplete
     * new manifest. Remove it.
     *
     * <li>If there is an existing manifest.properties and also a
     * manifest.properties.old, the manifest.properties represents the current
     * manifest and the old manifest is not needed. Delete
     * manifest.properties.old.
     *
     * <li>Now, if there is an existing manifest.properties, it is the current
     * manifest. Move it to manifest.properties.old and force the directory so
     * we're sure the backup of the current manifest is durable before creating
     * a new manifest.
     *
     * <li>Write the new manifest to manifest.properties.new and force the file
     * to make sure that all changes to the file are saved.
     *
     * <li>Move manifest.properties.new to manifest.properties and force the
     * directory to reduce the chance that other changes will become durable
     * without this move having been made durable.
     *
     * <li>If there is a manifest.properties.old, delete it.
     *
     * </ol>
     *
     * @param manifest the new manifest
     * @param snapshotDir the path of the snapshot directory
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    private void saveManifest(final SnapshotManifest manifest,
                              final Path snapshotDir)
        throws IOException, InterruptedException {

        assert TestHookExecute.doHookIfSet(saveManifestHook, snapshotDir);
        assert TestHookExecute.doIOHookIfSet(saveManifestHook);

        Path path = snapshotDir.resolve(SNAPSHOT_MANIFEST);
        Path newPath = snapshotDir.resolve(SNAPSHOT_MANIFEST + ".new");
        Path oldPath = snapshotDir.resolve(SNAPSHOT_MANIFEST + ".old");
        if (Files.exists(newPath)) {
            LoggerUtils.finer(logger, envImpl,
                              "Removing incomplete snapshot manifest: " +
                              newPath);
            Files.delete(newPath);
        }
        if (Files.exists(path)) {
            if (Files.exists(oldPath)) {
                LoggerUtils.finer(logger, envImpl,
                                  "Removing obsolete snapshot manifest: " +
                                  oldPath);
                Files.delete(oldPath);
            }
            Files.move(path, oldPath, StandardCopyOption.ATOMIC_MOVE);
            forceFile(snapshotDir);
        }
        Files.write(newPath, manifest.serialize(),
                    StandardOpenOption.SYNC, StandardOpenOption.CREATE_NEW);
        forceFile(newPath);
        copyFile(newPath, SNAPSHOT_MANIFEST, manifest.getSnapshot());
        Files.move(newPath, path, StandardCopyOption.ATOMIC_MOVE);
        forceFile(snapshotDir);
        if (Files.exists(oldPath)) {
            Files.delete(oldPath);
        }
    }

    /**
     * Returns the manifest for the specified snapshot, or null if not found.
     *
     * <p>To support atomic modifications to the manifest, this method needs to
     * handle the following cases:
     *
     * <ol>
     *
     * <li>If there is an existing manifest.properties.new, it is an incomplete
     * new manifest. Remove it.
     *
     * <li>If there is an existing manifest.properties and also a
     * manifest.properties.old, the manifest.properties represents the current
     * manifest and the old manifest is not needed. Delete
     * manifest.properties.old.
     *
     * <li>If there is an existing manifest.properties, it is the current
     * manifest. Return it.
     *
     * <li>If there is an existing manifest.properties.old and no
     * manifest.properties, it is the current manifest. Move it to
     * manifest.properties and return it.
     *
     * <li>If there is no manifest.properties or manifest.properties.old, there
     * is no manifest. Return null.
     *
     * </ol>
     *
     * @param snapshotDir the path of the snapshot directory
     * @return the manifest or null
     * @throws IOException if an I/O error occurs
     */
    @Nullable
    private SnapshotManifest getManifest(final Path snapshotDir)
        throws IOException {

        Path path = snapshotDir.resolve(SNAPSHOT_MANIFEST);
        Path oldPath = snapshotDir.resolve(SNAPSHOT_MANIFEST + ".old");
        Path newPath = snapshotDir.resolve(SNAPSHOT_MANIFEST + ".new");
        if (Files.exists(newPath)) {
            LoggerUtils.finer(logger, envImpl,
                              "Removing incomplete snapshot manifest: " +
                              newPath);
            Files.delete(newPath);
        }
        if (Files.exists(path)) {
            if (Files.exists(oldPath)) {
                LoggerUtils.finer(logger, envImpl,
                                  "Removing obsolete snapshot manifest: " +
                                  oldPath);
                Files.delete(oldPath);
            }
        } else if (Files.exists(oldPath)) {
            LoggerUtils.finer(logger, envImpl,
                              "Restoring old snapshot manifest: " + oldPath);
            Files.move(oldPath, path, StandardCopyOption.ATOMIC_MOVE);
        } else {
            return null;
        }
        return SnapshotManifest.deserialize(Files.readAllBytes(path));
    }

    /*
     * Miscellaneous
     */

    /**
     * Deletes the specified snapshot, if present.
     *
     * <p>Note that the snapshot thread will delete the latest snapshot if it
     * is incomplete, but that is the only time that the latest snapshot is
     * deleted. The copy thread will delete older snapshots as needed. No
     * synchronization between the threads for deleting snapshots is needed
     * because they never attempt to delete the same snapshot.
     *
     * @param snapshotDir the path of the snapshot directory
     * @throws IOException if an I/O error occurs
     */
    private void deleteSnapshot(final Path snapshotDir)
        throws IOException {

        /* Check if already deleted */
        if (Files.notExists(snapshotDir)) {
            return;
        }

        LoggerUtils.fine(logger, envImpl,
                         "Deleting snapshot: " + snapshotDir.getFileName());

        /*
         * Delete the complete snapshot marker file first, if present, and
         * force, to make sure it is clear that the directory is no longer
         * marked as a complete snapshot, since we will be deleting the hard
         * links.
         */
        final Path snapshotComplete = snapshotDir.resolve(SNAPSHOT_COMPLETE);
        if (Files.exists(snapshotComplete)) {
            Files.delete(snapshotComplete);
            forceFile(snapshotDir);
        }

        /* Delete the files first, then the directory */
        try (final Stream<Path> filesStream = Files.find(
                 snapshotDir, 1, (p, a) -> !p.equals(snapshotDir))) {
            filesStream.forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            Files.delete(snapshotDir);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * Returns a public no-arguments constructor for a non-abstract class that
     * implements the specified interface.
     *
     * @param <C> the interface type
     * @param type the interface that the class must implement
     * @param className the fully qualified class name
     * @return the constructor
     * @throws IllegalStateException if type is not an interface, the specified
     * class is not found, does not implement the specified interface, is
     * abstract, or does not have a public no-arguments constructor
     */
    public static <C> Constructor<? extends C>
        getImplementationClassConstructor(final Class<? extends C> type,
                                          final String className) {

        if (!Modifier.isInterface(type.getModifiers())) {
            throw new IllegalArgumentException(
                "Type must be an interface: " + type.getName());
        }
        final Class<? extends C> classType;
        try {
            classType = Class.forName(className).asSubclass(type);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class not found: " + className,
                                               e);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(
                "Class " + className + " must implement " + type.getName(),
                e);
        }
        if (Modifier.isAbstract(classType.getModifiers())) {
            throw new IllegalArgumentException("Class must not be abstract: " +
                                               className);
        }
        try {
            return classType.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                "Class " + className + " does not provide a public" +
                " no-arguments constructor",
                e);
        }
    }

    /**
     * Returns an object that implements an interface by creating an instance
     * of the specified non-abstract class that implements the specified
     * interface.
     *
     * @param <C> the interface type
     * @param type the interface that the class must implement
     * @param className the fully qualified class name
     * @return an instance of the class
     * @throws IllegalStateException if type is not an interface, the specified
     * class is not found, does not implement the specified interface, is
     * abstract, does not have a public no-arguments constructor, or the call
     * to the constructor throws an exception
     */
    public static <C> C getImplementationInstance(final Class<C> type,
                                                  final String className) {

        final Constructor<? extends C> constructor =
            getImplementationClassConstructor(type, className);
        final C instance;
        try {
            instance = constructor.newInstance();
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
                "Class " + className + " must be accessible", e);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(
                "Class " + className + " must not be abstract", e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException(
                "Class " + className + " constructor failed: " + e.getMessage(),
                e);
        }
        return instance;
    }

    /**
     * Returns the current time in absolute milliseconds, adjusted as needed
     * for {@link #timeMultiplier}.
     *
     * @return the current adjusted absolute time
     */
    public static long currentTimeMs() {
        final long now = System.currentTimeMillis();
        return (timeMultiplier == 0) ? now : now * timeMultiplier;
    }

    /**
     * Waits on the specified object for the specified number of milliseconds,
     * adjusted as needed for {@link #timeMultiplier}.
     *
     * @param object the object to wait on
     * @param timeMs the number of milliseconds to wait
     * @throws IllegalArgumentException if timeMs is less than 1
     * @throws InterruptedException if the operation is interrupted
     */
    static void waitMs(final Object object, final long timeMs)
        throws InterruptedException {

        if (timeMs < 1) {
            throw new IllegalArgumentException(
                "timeMs is too small: " + timeMs);
        }
        object.wait(computeWaitTimeMs(timeMs));
    }

    /**
     * Converts a wait time in milliseconds as needed for {@link
     * #timeMultiplier}. Returns a minimum of 1 millisecond when using a
     * non-zero timeMultiplier to make sure to wait for 1 millisecond and not
     * forever.
     */
    private static long computeWaitTimeMs(final long waitTimeMs) {
        return (timeMultiplier == 0) ?
            waitTimeMs :
            Math.max(1, waitTimeMs / timeMultiplier);
    }

    /**
     * Sleeps for the specified number of milliseconds, adjusted as needed for
     * {@link #timeMultiplier}.
     *
     * @param timeMs the number of milliseconds to sleep
     * @throws IllegalArgumentException if timeMs is less than 1
     * @throws InterruptedException if the operation is interrupted
     */
    public static void sleepMs(final long timeMs)
        throws InterruptedException {

        if (timeMs < 1) {
            throw new IllegalArgumentException(
                "timeMs is too small: " + timeMs);
        }
        long wait = computeWaitTimeMs(timeMs);
        final long until = currentTimeMs() + wait;
        do {
            Thread.sleep(wait);
            wait = until - currentTimeMs();
        } while (wait > 0);
    }

    /**
     * Converts a checksum to hex format.
     *
     * @param checksum the checksum as a byte array
     * @return the checksum as a string in hex format
     */
    public static String checksumToHex(final byte[] checksum) {
        final String value = new BigInteger(1, checksum).toString(16);
        final int zeros = (2 * checksum.length) - value.length();
        if (zeros == 0) {
            return value;
        }
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < zeros; i++) {
            sb.append('0');
        }
        sb.append(value);
        return sb.toString();
    }

    /**
     * Performs an action on a stream of the paths of all the snapshot
     * directories in the environment home directory. The stream is closed when
     * this method returns, so the action should not retain the stream. Any
     * {@link UncheckedIOException} thrown when accessing the stream is
     * rethrown as the underlying {@link IOException}.
     *
     * @param consumer the action to perform on the stream.
     * @throws IOException if an I/O error occurs
     */
    private void withSnapshots(final Consumer<Stream<Path>> consumer)
        throws IOException {

        final Path snapshotsSubdir = envHomeDir.resolve(SNAPSHOT_SUBDIRECTORY);
        try (final Stream<Path> snapshots = Files.find(
                 snapshotsSubdir, 1, (p, a) -> !snapshotsSubdir.equals(p))) {
            consumer.accept(snapshots);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * Performs an action on a stream of paths of all log files in the
     * specified snapshot directory. The stream is closed when this method
     * returns, so the action should not retain the stream. Any {@link
     * UncheckedIOException} thrown when accessing the stream is rethrown as
     * the underlying {@link IOException}.
     *
     * @param snapshotDir the path of the snapshot directory
     * @param consumer the action to perform on the stream
     * @throws IOException if an I/O error occurs
     */
    private static void withLogFiles(final Path snapshotDir,
                                     final Consumer<Stream<Path>> consumer)
        throws IOException {

        try (final Stream<Path> stream = Files.find(
                 snapshotDir, 1,
                 (p, a) -> getFileNameString(p).endsWith(".jdb"))) {
            consumer.accept(stream);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /** Returns the name of the file denoted by the path as a String. */
    private static String getFileNameString(final Path path) {
        return path.getFileName().toString();
    }

    /**
     * Returns the node name for a replicated environment or the environment
     * name for a standalone one.
     */
    private static String getNodeName(final EnvironmentImpl envImpl) {
        final String nodeName = envImpl.getNodeName();
        return (nodeName != null) ? nodeName : envImpl.getName();
    }

    /**
     * Returns the name of the snapshot associated with a snapshot directory.
     * The filename of the directory should be the snapshot prefix followed by
     * the date in YYMMDDHH format.
     */
    private static String getSnapshotName(final Path snapshotDir) {
        final String fileName = getFileNameString(snapshotDir);
        if (!SNAPSHOT_PATTERN.matcher(fileName).matches()) {
            throw new IllegalArgumentException("Bad snapshot directory: " +
                                               snapshotDir);
        }
        return fileName;
    }

    /** Format the date in human readable format. */
    static String formatTime(final long millis) {
        synchronized (dateFormat) {
            return dateFormat.format(new Date(millis));
        }
    }

    /**
     * Returns the exception string for logging, which includes the full stack
     * trace for level FINE and finer.
     */
    private String getExceptionStringForLogging(final Throwable e) {
        return logger.isLoggable(Level.FINE) ?
            LoggerUtils.getStackTrace(e) :
            e.toString();
    }
}
