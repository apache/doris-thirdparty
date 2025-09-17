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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.rep.net.InstanceLogger;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Level.FINE;

/**
 * A file watcher that monitors certificate files for changes and notifies
 * registered listeners when changes occur.
 */
public class CertificateFileWatcher {

    private final WatchService watchService;
    private final ConcurrentHashMap<String, CertificateReloadListener> listeners;
    private final ConcurrentHashMap<String, FileState> fileStates;
    private final AtomicBoolean running;
    private final Thread watcherThread;
    private final InstanceLogger logger;
    private final long refreshIntervalSeconds;

    /**
     * Tracks the state of a monitored file to detect delete-then-recreate scenarios.
     */
    private static class FileState {
        final String filePath;
        volatile boolean exists;
        volatile long lastModified;
        volatile long size;

        FileState(String filePath) {
            this.filePath = filePath;
            updateFromFile();
        }

        void updateFromFile() {
            File file = new File(filePath);
            this.exists = file.exists();
            this.lastModified = exists ? file.lastModified() : 0;
            this.size = exists ? file.length() : 0;
        }

        boolean hasChanged() {
            File file = new File(filePath);
            boolean currentExists = file.exists();
            long currentLastModified = currentExists ? file.lastModified() : 0;
            long currentSize = currentExists ? file.length() : 0;

            boolean changed = (currentExists != exists) ||
                             (currentExists && (currentLastModified != lastModified || currentSize != size));

            if (changed) {
                exists = currentExists;
                lastModified = currentLastModified;
                size = currentSize;
            }

            return changed;
        }

        boolean wasDeletedThenRecreated() {
            File file = new File(filePath);
            boolean currentExists = file.exists();

            // File was deleted and then recreated if:
            // 1. Our tracked state shows it didn't exist before (exists = false)
            // 2. It currently exists
            // 3. This indicates a delete->create sequence
            return !exists && currentExists;
        }
    }

    /**
     * Interface for listeners that want to be notified when certificate files change.
     */
    public interface CertificateReloadListener {
        /**
         * Called when a certificate file has been modified.
         * @param filePath the path of the file that was modified
         */
        void onCertificateFileChanged(String filePath);
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
        this.fileStates = new ConcurrentHashMap<>();
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

        Path parentDir = Paths.get(filePath).getParent();
        if (parentDir == null) {
            parentDir = Paths.get(".");
        }

        try {
            parentDir.register(watchService,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE);

            listeners.put(filePath, listener);

            // Initialize file state tracking
            fileStates.put(filePath, new FileState(filePath));

            logger.log(INFO, "Registered file for certificate auto-reload: " + filePath);

            if (!running.get()) {
                start();
            }
        } catch (IOException e) {
            logger.log(WARNING, "Failed to register file for watching: " + filePath + ", error: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Unregister a file from being watched.
     *
     * @param filePath the path to the file to stop watching
     */
    public void unregisterFile(String filePath) {
        listeners.remove(filePath);
        fileStates.remove(filePath);
        logger.log(INFO, "Unregistered file from certificate auto-reload: " + filePath);
    }

    /**
     * Start the file watcher.
     */
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        watcherThread.start();
        logger.log(INFO, "Certificate file watcher started");
    }

    /**
     * Stop the file watcher.
     */
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        try {
            watchService.close();
        } catch (IOException e) {
            logger.log(WARNING, "Error closing watch service: " + e.getMessage());
        }

        watcherThread.interrupt();
        logger.log(INFO, "Certificate file watcher stopped");
    }

    /**
     * The main watch loop that runs in a separate thread.
     */
    private void watchLoop() {
        while (running.get()) {
            try {
                WatchKey key;

                if (refreshIntervalSeconds <= 0) {
                    // Use blocking wait if interval is 0 or negative
                    key = watchService.take();
                } else {
                    // Use polling with configured interval
                    key = watchService.poll(refreshIntervalSeconds, TimeUnit.SECONDS);
                }

                if (key == null) {
                    // Timeout occurred, check for delete-then-recreate scenarios
                    checkForDeleteThenRecreate();
                    continue;
                }

                // Process file system events
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                    Path changedFile = pathEvent.context();

                    if (changedFile == null) {
                        continue;
                    }

                    Path watchedDir = (Path) key.watchable();
                    Path fullPath = watchedDir.resolve(changedFile);
                    String fullPathStr = fullPath.toString();

                    // Check if this is a file we're monitoring
                    CertificateReloadListener listener = listeners.get(fullPathStr);
                    FileState fileState = fileStates.get(fullPathStr);

                    if (listener != null && fileState != null) {
                        handleFileEvent(kind, fullPathStr, listener, fileState);
                    }
                }

                // Also check for delete-then-recreate scenarios after processing events
                checkForDeleteThenRecreate();

                if (!key.reset()) {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.log(WARNING, "Unexpected error in certificate file watcher: " + e.getMessage());
            }
        }
    }

    /**
     * Handle a specific file system event.
     */
    private void handleFileEvent(WatchEvent.Kind<?> kind, String fullPath,
                                CertificateReloadListener listener, FileState fileState) {
        try {
            if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                logger.log(FINE, "Certificate file deleted: " + fullPath);
                // Mark as deleted but don't update other fields yet
                // This allows wasDeletedThenRecreated() to work correctly
                fileState.exists = false;

            } else if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                // Check if this is a recreation after deletion
                if (fileState.wasDeletedThenRecreated()) {
                    logger.log(INFO, "Certificate file recreated after deletion, triggering reload: " + fullPath);
                    fileState.updateFromFile(); // Update all fields now
                    listener.onCertificateFileChanged(fullPath);
                } else {
                    logger.log(FINE, "Certificate file created: " + fullPath);
                    fileState.updateFromFile();
                }

            } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                // Standard modification - only trigger if file actually changed
                if (fileState.hasChanged()) {
                    logger.log(INFO, "Certificate file modified, triggering reload: " + fullPath);
                    listener.onCertificateFileChanged(fullPath);
                }
            }
        } catch (Exception e) {
            logger.log(WARNING, "Error handling file event for " + fullPath + ": " + e.getMessage());
        }
    }

    /**
     * Check all monitored files for delete-then-recreate scenarios.
     * This handles cases where the file system events might be missed
     * or when polling is used instead of blocking wait.
     */
    private void checkForDeleteThenRecreate() {
        for (String filePath : fileStates.keySet()) {
            FileState fileState = fileStates.get(filePath);
            CertificateReloadListener listener = listeners.get(filePath);

            if (fileState != null && listener != null) {
                try {
                    if (fileState.wasDeletedThenRecreated()) {
                        logger.log(INFO, "Detected delete-then-recreate scenario for: " + filePath);
                        fileState.updateFromFile();
                        listener.onCertificateFileChanged(filePath);
                    } else if (fileState.hasChanged()) {
                        logger.log(FINE, "Detected file changes during polling for: " + filePath);
                        listener.onCertificateFileChanged(filePath);
                    }
                } catch (Exception e) {
                    logger.log(WARNING, "Error checking file state for " + filePath + ": " + e.getMessage());
                }
            }
        }
    }
}