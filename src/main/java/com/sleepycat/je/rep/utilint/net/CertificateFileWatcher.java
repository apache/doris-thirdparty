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

/**
 * A file watcher that monitors certificate files for changes and notifies
 * registered listeners when changes occur.
 */
public class CertificateFileWatcher {

    private final WatchService watchService;
    private final ConcurrentHashMap<String, CertificateReloadListener> listeners;
    private final AtomicBoolean running;
    private final Thread watcherThread;
    private final InstanceLogger logger;
    private final long refreshIntervalSeconds;

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
                StandardWatchEventKinds.ENTRY_CREATE);

            listeners.put(filePath, listener);
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
                    // Timeout occurred, continue to next iteration
                    continue;
                }

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

                    CertificateReloadListener listener = listeners.get(fullPathStr);
                    if (listener != null) {
                        logger.log(INFO, "Certificate file changed, triggering reload: " + fullPathStr);
                        try {
                            listener.onCertificateFileChanged(fullPathStr);
                        } catch (Exception e) {
                            logger.log(WARNING, "Error during certificate reload: " + e.getMessage());
                        }
                    }
                }

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
}