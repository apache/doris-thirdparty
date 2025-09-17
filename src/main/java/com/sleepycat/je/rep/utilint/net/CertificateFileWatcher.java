/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package com.sleepycat.je.rep.utilint.net;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.ClosedWatchServiceException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap.KeySetView;

import com.sleepycat.je.rep.net.InstanceLogger;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

/**
 * A file watcher that monitors certificate files for changes and notifies
 * registered listeners when changes occur.
 */
public class CertificateFileWatcher {

    private final WatchService watchService;
    private final ConcurrentHashMap<Path, CertificateReloadListener> listeners;
    private final ConcurrentHashMap<Path, WatchKey> registeredDirectories;
    private final ConcurrentHashMap<Path, AtomicLong> fileLastNotificationTimes;
    private final AtomicBoolean running;
    private final Thread watcherThread;
    private final InstanceLogger logger;
    private final long refreshIntervalSeconds;

    /**
     * File change event types
     */
    public enum FileChangeType {
        CREATED, MODIFIED, DELETED
    }

    /**
     * Interface for listeners that want to be notified when certificate files change.
     */
    public interface CertificateReloadListener {
        /**
         * Called when a certificate file has been modified.
         * @param filePath the path of the file that was modified
         * @param changeType the type of change that occurred
         */
        void onCertificateFileChanged(String filePath, FileChangeType changeType);

        /**
         * Legacy method for backward compatibility
         * @param filePath the path of the file that was modified
         */
        default void onCertificateFileChanged(String filePath) {
            onCertificateFileChanged(filePath, FileChangeType.MODIFIED);
        }
    }

    /**
     * Create a new certificate file watcher.
     *
     * @param logger the logger to use for logging messages
     * @param refreshIntervalSeconds the interval in seconds to check for file changes
     *                              (0 means use blocking wait)
     * @throws IOException if the watch service cannot be created
     */
    public CertificateFileWatcher(InstanceLogger logger, long refreshIntervalSeconds) throws IOException {
        this.watchService = FileSystems.getDefault().newWatchService();
        this.listeners = new ConcurrentHashMap<>();
        this.registeredDirectories = new ConcurrentHashMap<>();
        this.fileLastNotificationTimes = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
        this.logger = logger;
        this.refreshIntervalSeconds = refreshIntervalSeconds;
        this.watcherThread = new Thread(this::watchLoop, "CertificateFileWatcher");
        this.watcherThread.setDaemon(true);
    }

    /**
     * Register a file to be watched and add a listener for changes.
     *
     * @param filePath the path to the file to watch
     * @param listener the listener to notify when the file changes
     * @throws IOException if the file cannot be watched
     */
    public void registerFile(String filePath, CertificateReloadListener listener) throws IOException {
        if (filePath == null || listener == null) {
            throw new IllegalArgumentException("File path and listener cannot be null");
        }

        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("File does not exist: " + filePath);
        }

        // Convert to absolute path and normalize for consistent matching
        Path absoluteFilePath;
        try {
            absoluteFilePath = Paths.get(filePath).toRealPath(); // Resolves symlinks and normalizes case
        } catch (IOException e) {
            // Fallback if toRealPath() fails (e.g., file on different filesystem)
            absoluteFilePath = Paths.get(filePath).toAbsolutePath().normalize();
        }

        Path parentDir = absoluteFilePath.getParent();
        if (parentDir == null) {
            parentDir = Paths.get(".").toAbsolutePath();
        }

        try {
            // Only register directory if not already registered
            WatchKey existingKey = registeredDirectories.get(parentDir);
            if (existingKey == null) {
                WatchKey newKey = parentDir.register(watchService,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE);

                // Use putIfAbsent to handle race conditions
                WatchKey actualKey = registeredDirectories.putIfAbsent(parentDir, newKey);
                if (actualKey != null) {
                    // Another thread already registered this directory, cancel our key
                    newKey.cancel();
                } else {
                    logger.log(INFO, "Registered directory for watching: " + parentDir);
                }
            }

            // Register the file listener
            listeners.put(absoluteFilePath, listener);

            // Initialize debounce time for this file
            fileLastNotificationTimes.put(absoluteFilePath, new AtomicLong(0));

            logger.log(INFO, "Registered file for certificate auto-reload: " + absoluteFilePath);

            // Start watcher thread if not already running
            if (!running.get()) {
                start();
            }
        } catch (IOException e) {
            logger.log(WARNING, "Failed to register file for watching: " + filePath, e);
            throw e;
        }
    }

    /**
     * Unregister a file from being watched.
     *
     * @param filePath the path to the file to stop watching
     */
    public void unregisterFile(String filePath) {
        // Convert to absolute path for consistent lookup
        Path absoluteFilePath;
        try {
            absoluteFilePath = Paths.get(filePath).toRealPath();
        } catch (IOException e) {
            absoluteFilePath = Paths.get(filePath).toAbsolutePath().normalize();
        }

        listeners.remove(absoluteFilePath);
        fileLastNotificationTimes.remove(absoluteFilePath);
        logger.log(INFO, "Unregistered file from certificate auto-reload: " + absoluteFilePath);

        // Note: We don't remove directory registrations here as other files might
        // still be watching the same directory. Directory cleanup happens on shutdown.
    }

    /**
     * Start the file watcher.
     */
    public synchronized void start() {
        // Use simple CAS to prevent multiple starts
        if (!running.compareAndSet(false, true)) {
            return; // Already running
        }

        try {
            watcherThread.start();
            logger.log(INFO, "Certificate file watcher started");
        } catch (IllegalThreadStateException e) {
            // Thread was already started, reset running flag
            running.set(false);
            logger.log(WARNING, "Certificate file watcher thread was already started", e);
            throw new IllegalStateException("Watcher thread was already started", e);
        }
    }

    /**
     * Stop the file watcher.
     */
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        // Interrupt thread first, then close watch service to avoid ClosedWatchServiceException
        watcherThread.interrupt();

        try {
            watchService.close();
        } catch (IOException e) {
            logger.log(WARNING, "Error closing watch service", e);
        }

        // Cancel all registered watch keys to clean up resources
        for (WatchKey key : registeredDirectories.values()) {
            try {
                key.cancel();
            } catch (Exception e) {
                logger.log(WARNING, "Error canceling watch key", e);
            }
        }

        // Clear all state
        registeredDirectories.clear();
        listeners.clear();
        fileLastNotificationTimes.clear();

        logger.log(INFO, "Certificate file watcher stopped");
    }

    /**
     * The main watch loop that runs in a separate thread.
     */
    private void watchLoop() {
        logger.log(INFO, "Certificate file watcher loop started");

        while (running.get()) {
            try {
                WatchKey key;

                if (refreshIntervalSeconds <= 0) {
                    // Use blocking wait if interval is 0 or negative
                    key = watchService.take();
                } else {
                    // Use configured polling interval (no artificial limit)
                    key = watchService.poll(refreshIntervalSeconds, TimeUnit.SECONDS);
                }

                if (key == null) {
                    // Timeout occurred, continue to next iteration
                    continue;
                }

                // Process all events for this key
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        logger.log(WARNING, "Watch service overflow - some file events may have been lost");
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                    Path changedFile = pathEvent.context();

                    if (changedFile == null) {
                        continue;
                    }

                    Path watchedDir = (Path) key.watchable();
                    Path fullPath;
                    try {
                        fullPath = watchedDir.resolve(changedFile).toRealPath();
                    } catch (IOException e) {
                        // Fallback if toRealPath() fails (e.g., file was deleted)
                        fullPath = watchedDir.resolve(changedFile).toAbsolutePath().normalize();
                    }

                    CertificateReloadListener listener = listeners.get(fullPath);
                    if (listener != null) {
                        // Get per-file debounce time
                        AtomicLong fileLastNotificationTime = fileLastNotificationTimes.get(fullPath);
                        if (fileLastNotificationTime == null) {
                            // File was unregistered, skip processing
                            continue;
                        }

                        // Per-file debounce check (1 second)
                        long currentTime = System.currentTimeMillis();
                        long lastTime = fileLastNotificationTime.get();

                        if (currentTime - lastTime > 1000) {
                            if (fileLastNotificationTime.compareAndSet(lastTime, currentTime)) {
                                FileChangeType changeType = getChangeType(kind);

                                try {
                                    if (changeType == FileChangeType.DELETED) {
                                        logger.log(WARNING, "Certificate file was deleted: " + fullPath);
                                        // For delete events, still notify listener but don't check file existence
                                        listener.onCertificateFileChanged(fullPath.toString(), changeType);
                                    } else {
                                        // For create/modify events, check if file still exists
                                        if (fullPath.toFile().exists()) {
                                            logger.log(INFO, "Certificate file " + changeType.toString().toLowerCase() +
                                                      ", triggering reload: " + fullPath);
                                            listener.onCertificateFileChanged(fullPath.toString(), changeType);
                                        } else {
                                            logger.log(WARNING, "Certificate file no longer exists during " +
                                                      changeType.toString().toLowerCase() + " event: " + fullPath);
                                        }
                                    }
                                } catch (Exception e) {
                                    logger.log(WARNING, "Error during certificate reload for " + fullPath, e);
                                }
                            }
                        } else {
                            // Debounced - log at FINE level to reduce noise
                            logger.log(java.util.logging.Level.FINE,
                                      "Certificate file change debounced: " + fullPath +
                                      " (" + getChangeType(kind).toString().toLowerCase() + ")");
                        }
                    }
                }

                // Reset key and handle invalid keys gracefully
                boolean valid = key.reset();
                if (!valid) {
                    // Key is no longer valid, remove it from our tracking
                    Path watchedDir = (Path) key.watchable();
                    WatchKey removedKey = registeredDirectories.remove(watchedDir);
                    if (removedKey != null) {
                        logger.log(WARNING, "Watch key for directory is no longer valid, removed: " + watchedDir);
                    }

                    // Don't break the loop, continue watching other directories
                    // Only break if no more directories are being watched
                    if (registeredDirectories.isEmpty()) {
                        logger.log(INFO, "No more directories to watch, exiting watch loop");
                        break;
                    }
                }

            } catch (ClosedWatchServiceException e) {
                // Normal shutdown condition
                logger.log(INFO, "Watch service closed, exiting watch loop");
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(INFO, "Certificate file watcher interrupted, exiting watch loop");
                break;
            } catch (Exception e) {
                logger.log(WARNING, "Unexpected error in certificate file watcher", e);
                // Continue running unless it's a critical error
                if (e instanceof OutOfMemoryError || e instanceof StackOverflowError) {
                    logger.log(WARNING, "Critical error in file watcher, exiting: " + e.getClass().getSimpleName());
                    break;
                }
            }
        }
        logger.log(INFO, "Certificate file watcher loop exited");
    }

    /**
     * Convert WatchEvent.Kind to FileChangeType
     */
    private FileChangeType getChangeType(WatchEvent.Kind<?> kind) {
        if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
            return FileChangeType.CREATED;
        } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
            return FileChangeType.MODIFIED;
        } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
            return FileChangeType.DELETED;
        } else {
            return FileChangeType.MODIFIED; // Default fallback
        }
    }
}