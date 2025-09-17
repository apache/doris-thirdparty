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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.sleepycat.je.rep.net.InstanceLogger;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Level.FINE;

/**
 * Connection pool manager responsible for tracking and managing all active SSL connections,
 * supporting graceful certificate migration.
 *
 * Core features:
 * 1. Connection registration and lifecycle management
 * 2. Batch connection migration scheduling
 * 3. Connection state monitoring and health checks
 * 4. Asynchronous migration task execution
 */
public class ConnectionPoolManager {

    /**
     * Connection state enumeration
     */
    public enum ConnectionState {
        /** Active state, working normally */
        ACTIVE,
        /** Migrating state, preparing to switch SSL context */
        MIGRATING,
        /** Migrated state, using new SSL context */
        MIGRATED,
        /** Retrying state, preparing for migration retry after failure */
        RETRYING,
        /** Failed state, migration failed permanently after all retries */
        FAILED,
        /** Closed state */
        CLOSED
    }

    /**
     * Connection information wrapper class
     */
    public static class ConnectionInfo {
        private final String connectionId;
        private final MigratableSSLDataChannel channel;
        private final AtomicReference<ConnectionState> state;
        private volatile long lastActivityTime;
        private final AtomicInteger migrationAttempts;
        private volatile String errorMessage;
        private final long creationTime;

        public ConnectionInfo(String connectionId, MigratableSSLDataChannel channel) {
            this.connectionId = connectionId;
            this.channel = channel;
            this.state = new AtomicReference<>(ConnectionState.ACTIVE);
            this.lastActivityTime = System.currentTimeMillis();
            this.migrationAttempts = new AtomicInteger(0);
            this.creationTime = System.currentTimeMillis();
        }

        // Getters and setters
        public String getConnectionId() { return connectionId; }
        public MigratableSSLDataChannel getChannel() { return channel; }
        public ConnectionState getState() { return state.get(); }
        public void setState(ConnectionState newState) { this.state.set(newState); }

        /**
         * Compare and set state atomically - prevents duplicate migrations
         */
        public boolean compareAndSetState(ConnectionState expectedState, ConnectionState newState) {
            return this.state.compareAndSet(expectedState, newState);
        }

        public long getLastActivityTime() { return lastActivityTime; }
        public void updateActivity() { this.lastActivityTime = System.currentTimeMillis(); }
        public int getMigrationAttempts() { return migrationAttempts.get(); }
        public void incrementMigrationAttempts() { migrationAttempts.incrementAndGet(); }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        public long getCreationTime() { return creationTime; }
    }

    /**
     * Migration callback interface
     */
    public interface MigrationCallback {
        /**
         * Called when migration starts
         */
        void onMigrationStart(String connectionId);

        /**
         * Called when migration succeeds
         */
        void onMigrationSuccess(String connectionId);

        /**
         * Called when migration fails
         */
        void onMigrationFailure(String connectionId, Exception cause);

        /**
         * Called when migration completes (regardless of success or failure)
         */
        void onMigrationComplete(String connectionId, boolean success);
    }

    // Connection pool storage
    private final ConcurrentHashMap<String, ConnectionInfo> connections = new ConcurrentHashMap<>();

    // Migration related
    private final CopyOnWriteArrayList<MigrationCallback> migrationCallbacks = new CopyOnWriteArrayList<>();
    private final ExecutorService migrationExecutor;
    private final ScheduledExecutorService scheduler;
    private final ReentrantReadWriteLock migrationLock = new ReentrantReadWriteLock();

    // Statistics
    private final AtomicLong totalConnections = new AtomicLong(0);
    private final AtomicLong successfulMigrations = new AtomicLong(0);
    private final AtomicLong failedMigrations = new AtomicLong(0);

    // Configuration parameters
    private final int maxMigrationRetries;
    private final long migrationTimeoutMs;
    private final long connectionTimeoutMs;

    // Logger
    private final InstanceLogger logger;

    // Health check
    private volatile boolean healthCheckEnabled = true;

    // Thread naming counters
    private static final AtomicInteger migrationThreadCounter = new AtomicInteger();

    /**
     * Constructor with default parameters
     */
    public ConnectionPoolManager(InstanceLogger logger) {
        this(logger, 3, 30000, 300000); // Default: 3 retries, 30s migration timeout, 5min connection timeout
    }

    /**
     * Constructor with custom parameters
     */
    public ConnectionPoolManager(InstanceLogger logger,
                               int maxMigrationRetries,
                               long migrationTimeoutMs,
                               long connectionTimeoutMs) {
        this.logger = logger;
        this.maxMigrationRetries = maxMigrationRetries;
        this.migrationTimeoutMs = migrationTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;

        // Create bounded migration executor to prevent thread explosion
        this.migrationExecutor = new ThreadPoolExecutor(
            4,                              // Core pool size
            16,                             // Maximum pool size
            60, TimeUnit.SECONDS,           // Keep alive time
            new LinkedBlockingQueue<>(1000), // Bounded queue to prevent memory issues
            r -> {
                Thread t = new Thread(r, "SSL-Migration-" + migrationThreadCounter.incrementAndGet());
                t.setDaemon(true);
                return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy()  // Rejection policy: run in caller thread
        );

        // Create scheduler for health checks and retry scheduling
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Connection-Scheduler");
            t.setDaemon(true);
            return t;
        });

        // Start health check
        startHealthCheck();

        logger.log(INFO, "ConnectionPoolManager initialized with " +
                  "maxRetries=" + maxMigrationRetries +
                  ", migrationTimeout=" + migrationTimeoutMs + "ms" +
                  ", connectionTimeout=" + connectionTimeoutMs + "ms");
    }

    /**
     * Register a connection to the pool
     */
    public void registerConnection(String connectionId, MigratableSSLDataChannel channel) {
        if (connectionId == null || channel == null) {
            throw new IllegalArgumentException("Connection ID and channel cannot be null");
        }

        ConnectionInfo info = new ConnectionInfo(connectionId, channel);
        ConnectionInfo existing = connections.put(connectionId, info);

        if (existing == null) {
            totalConnections.incrementAndGet();
            logger.log(FINE, "Registered new connection: " + connectionId);
        } else {
            logger.log(WARNING, "Replaced existing connection: " + connectionId);
        }
    }

    /**
     * Unregister a connection from the pool and close its channel with CAS protection
     */
    public void unregisterConnection(String connectionId) {
        ConnectionInfo info = connections.remove(connectionId);
        if (info != null) {
            // Decrement total connections counter
            totalConnections.decrementAndGet();

            // Try to atomically set state to CLOSED
            // This prevents races with ongoing migrations
            ConnectionState oldState = info.getState();
            if (info.compareAndSetState(oldState, ConnectionState.CLOSED)) {
                // Close the channel to prevent resource leaks
                try {
                    info.getChannel().closeForcefully();
                } catch (IOException e) {
                    logger.log(WARNING, "Error closing channel during unregistration for " +
                              connectionId, e);
                } catch (Exception e) {
                    logger.log(WARNING, "Unexpected error closing channel for " +
                              connectionId, e);
                }

                logger.log(FINE, "Unregistered and closed connection: " + connectionId +
                          " (was in " + oldState + " state)");
            } else {
                // State was changed concurrently, but we still removed it from pool
                logger.log(FINE, "Unregistered connection: " + connectionId +
                          " (state changed during removal)");
            }
        }
    }

    /**
     * Get connection information
     */
    public ConnectionInfo getConnectionInfo(String connectionId) {
        return connections.get(connectionId);
    }

    /**
     * Get the count of active connections (improved accuracy)
     */
    public int getActiveConnectionCount() {
        return (int) connections.values().stream()
                .filter(info -> info.getState() == ConnectionState.ACTIVE && info.getChannel().isOpen())
                .count();
    }

    /**
     * Get the count of actually usable connections (active and migrated)
     */
    public int getUsableConnectionCount() {
        return (int) connections.values().stream()
                .filter(info -> (info.getState() == ConnectionState.ACTIVE || info.getState() == ConnectionState.MIGRATED)
                               && info.getChannel().isOpen())
                .count();
    }

    /**
     * Get the count of migrating connections
     */
    public int getMigratingConnectionCount() {
        return (int) connections.values().stream()
                .filter(info -> info.getState() == ConnectionState.MIGRATING)
                .count();
    }

    /**
     * Get the count of retrying connections
     */
    public int getRetryingConnectionCount() {
        return (int) connections.values().stream()
                .filter(info -> info.getState() == ConnectionState.RETRYING)
                .count();
    }

    /**
     * Get the count of failed connections
     */
    public int getFailedConnectionCount() {
        return (int) connections.values().stream()
                .filter(info -> info.getState() == ConnectionState.FAILED)
                .count();
    }

    /**
     * Trigger batch connection migration with optimized lock-free snapshot
     */
    public void triggerBatchMigration(SSLContextManager contextManager) {
        // Create snapshot without any locks - ConcurrentHashMap.values() provides weakly consistent view
        List<ConnectionInfo> snapshot = connections.values().stream()
                .filter(info -> info.getState() == ConnectionState.ACTIVE)
                .collect(Collectors.toList());

        if (snapshot.isEmpty()) {
            logger.log(INFO, "No active connections to migrate");
            return;
        }

        logger.log(INFO, "Starting batch migration for " + snapshot.size() + " connections");

        // Submit migration tasks without holding any locks
        for (ConnectionInfo info : snapshot) {
            migrationExecutor.submit(() -> migrateConnection(info, contextManager));
        }
    }

    /**
     * Migrate a single connection with CAS state protection against duplicate migrations
     */
    private void migrateConnection(ConnectionInfo info, SSLContextManager contextManager) {
        String connectionId = info.getConnectionId();

        // Use CAS to atomically transition from ACTIVE or RETRYING to MIGRATING
        // This prevents multiple threads from migrating the same connection
        ConnectionState currentState = info.getState();
        if (currentState == ConnectionState.ACTIVE) {
            if (!info.compareAndSetState(ConnectionState.ACTIVE, ConnectionState.MIGRATING)) {
                logger.log(FINE, "Connection " + connectionId + " state changed, skipping migration");
                return;
            }
            // Increment attempt counter for new migrations
            info.incrementMigrationAttempts();
        } else if (currentState == ConnectionState.RETRYING) {
            if (!info.compareAndSetState(ConnectionState.RETRYING, ConnectionState.MIGRATING)) {
                logger.log(FINE, "Connection " + connectionId + " no longer in RETRYING state, skipping retry");
                return;
            }
            // Don't increment counter for retries - already counted
        } else {
            logger.log(FINE, "Connection " + connectionId + " is in " + currentState + " state, skipping migration");
            return;
        }

        // Notify migration start (non-blocking)
        notifyMigrationCallbacks(callback -> callback.onMigrationStart(connectionId));

        boolean success = false;
        Exception lastException = null;

        try {
            logger.log(FINE, "Starting migration for connection: " + connectionId +
                      " (attempt " + info.getMigrationAttempts() + ")");

            // Execute actual migration logic outside of lock
            success = performConnectionMigration(info, contextManager);

            if (success) {
                info.setState(ConnectionState.MIGRATED);
                successfulMigrations.incrementAndGet();
                logger.log(INFO, "Successfully migrated connection: " + connectionId);

                // Notify migration success
                notifyMigrationCallbacks(callback -> callback.onMigrationSuccess(connectionId));

                // Exit successfully, skip catch block
                notifyMigrationCallbacks(callback -> callback.onMigrationComplete(connectionId, success));
                return;
            } else {
                // Handle migration failure without throwing exception to avoid thread pool stress
                logger.log(WARNING, "Migration failed for connection: " + connectionId + " (returned false)");
                info.setErrorMessage("Migration operation returned false");
            }

        } catch (Exception e) {
            lastException = e;
            info.setErrorMessage(e.getMessage());
        }

        // Handle migration failure (either from exception or false return)
        if (!success) {
            // Check if retry is needed
            if (info.getMigrationAttempts() < maxMigrationRetries) {
                info.setState(ConnectionState.RETRYING); // Mark as retrying instead of active
                logger.log(WARNING, "Migration failed for connection " + connectionId +
                          ", will retry. Error: " + (lastException != null ? lastException.getMessage() : "returned false"));

                // Schedule retry with backoff using scheduler
                scheduleRetry(info, contextManager);
            } else {
                info.setState(ConnectionState.FAILED);
                failedMigrations.incrementAndGet();
                logger.log(WARNING, "Migration permanently failed for connection " + connectionId +
                          " after " + info.getMigrationAttempts() + " attempts. Error: " +
                          (lastException != null ? lastException.getMessage() : "returned false"));

                // Notify migration failure
                if (lastException != null) {
                    notifyMigrationCallbacks(callback -> callback.onMigrationFailure(connectionId, lastException));
                } else {
                    notifyMigrationCallbacks(callback -> callback.onMigrationFailure(connectionId,
                        new RuntimeException("Migration operation returned false")));
                }
            }

            // Notify migration complete
            notifyMigrationCallbacks(callback -> callback.onMigrationComplete(connectionId, success));
        }
    }

    /**
     * Perform the core connection migration logic with protection against concurrent interruption
     */
    private boolean performConnectionMigration(ConnectionInfo info, SSLContextManager contextManager) {
        try {
            MigratableSSLDataChannel channel = info.getChannel();

            // Check if channel is still open - protection against concurrent closure
            if (!channel.isOpen()) {
                logger.log(WARNING, "Channel for connection " + info.getConnectionId() +
                          " was closed during migration attempt");
                return false;
            }

            // Check if connection is still registered (not removed by unregisterConnection)
            if (!connections.containsKey(info.getConnectionId())) {
                logger.log(WARNING, "Connection " + info.getConnectionId() +
                          " was unregistered during migration attempt");
                return false;
            }

            // Get new SSL context
            int newVersion = contextManager.getCurrentVersion();

            // Final check before migration - ensure channel is still healthy
            if (!channel.isHealthy()) {
                logger.log(WARNING, "Channel for connection " + info.getConnectionId() +
                          " is not healthy, skipping migration");
                return false;
            }

            // Execute migration
            boolean migrated = channel.migrateToNewContext(contextManager, newVersion, migrationTimeoutMs);

            if (migrated) {
                // Double-check channel is still open after migration
                if (channel.isOpen()) {
                    info.updateActivity();
                    return true;
                } else {
                    logger.log(WARNING, "Channel for connection " + info.getConnectionId() +
                              " became closed after migration");
                    return false;
                }
            }

            return false;

        } catch (Exception e) {
            logger.log(WARNING, "Exception during connection migration for " +
                      info.getConnectionId(), e);
            throw e;
        }
    }

    /**
     * Schedule retry migration using proper scheduler with connection existence validation
     */
    private void scheduleRetry(ConnectionInfo info, SSLContextManager contextManager) {
        // Calculate exponential backoff delay
        long delay = Math.min(1000L * (1L << (info.getMigrationAttempts() - 1)), 30000L);

        logger.log(FINE, "Scheduling retry for connection " + info.getConnectionId() +
                  " in " + delay + "ms (attempt " + info.getMigrationAttempts() + ")");

        // Use scheduler to schedule retry without occupying worker threads
        scheduler.schedule(() -> {
            // Check if connection still exists before retrying
            String connectionId = info.getConnectionId();
            if (!connections.containsKey(connectionId)) {
                logger.log(FINE, "Connection " + connectionId +
                          " no longer exists in pool, skipping retry");
                return;
            }

            // Verify the connection is still in RETRYING state
            if (info.getState() != ConnectionState.RETRYING) {
                logger.log(FINE, "Connection " + connectionId +
                          " is no longer in RETRYING state, skipping retry");
                return;
            }

            // Submit actual migration task to migration executor only if connection exists
            migrationExecutor.submit(() -> {
                // Double-check connection existence just before migration
                if (connections.containsKey(connectionId)) {
                    migrateConnection(info, contextManager);
                } else {
                    logger.log(FINE, "Connection " + connectionId +
                              " removed from pool before retry execution");
                }
            });
        }, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Add migration callback
     */
    public void addMigrationCallback(MigrationCallback callback) {
        if (callback != null) {
            migrationCallbacks.add(callback);
        }
    }

    /**
     * Remove migration callback
     */
    public void removeMigrationCallback(MigrationCallback callback) {
        migrationCallbacks.remove(callback);
    }

    /**
     * Notify all migration callbacks with improved error handling
     */
    private void notifyMigrationCallbacks(Consumer<MigrationCallback> action) {
        for (MigrationCallback callback : migrationCallbacks) {
            try {
                action.accept(callback);
            } catch (Exception e) {
                logger.log(WARNING, "Error in migration callback", e);
            }
        }
    }

    /**
     * Start health check using the scheduler
     */
    private void startHealthCheck() {
        scheduler.scheduleAtFixedRate(this::performHealthCheck, 30, 30, TimeUnit.SECONDS);
    }

    /**
     * Perform health check with proper connection cleanup
     */
    private void performHealthCheck() {
        if (!healthCheckEnabled) {
            return;
        }

        try {
            long currentTime = System.currentTimeMillis();
            int checkedCount = 0;
            int removedCount = 0;

            // Use iterator to safely remove entries during iteration
            for (Iterator<Map.Entry<String, ConnectionInfo>> it = connections.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, ConnectionInfo> entry = it.next();
                ConnectionInfo info = entry.getValue();
                checkedCount++;

                // Skip connections that are actively migrating or retrying to avoid race conditions
                ConnectionState state = info.getState();
                if (state == ConnectionState.MIGRATING || state == ConnectionState.RETRYING) {
                    logger.log(FINE, "Skipping health check for connection in " + state + " state: " + entry.getKey());
                    continue;
                }

                boolean shouldRemove = false;

                // Check connection timeout
                if (currentTime - info.getLastActivityTime() > connectionTimeoutMs) {
                    shouldRemove = true;
                    logger.log(FINE, "Connection timed out: " + entry.getKey());
                }

                // Check if channel is still open
                if (!info.getChannel().isOpen()) {
                    shouldRemove = true;
                    logger.log(FINE, "Connection channel is closed: " + entry.getKey());
                }

                if (shouldRemove) {
                    // Use CAS to atomically transition to CLOSED state
                    // This prevents races with migration attempts
                    if (info.compareAndSetState(state, ConnectionState.CLOSED)) {
                        it.remove(); // Safe removal during iteration

                        // Decrement total connections counter
                        totalConnections.decrementAndGet();

                        // Close the channel to prevent resource leaks
                        try {
                            info.getChannel().closeForcefully();
                        } catch (IOException e) {
                            logger.log(WARNING, "Error closing channel in health check for " +
                                      entry.getKey(), e);
                        } catch (Exception e) {
                            logger.log(WARNING, "Unexpected error closing channel in health check for " +
                                      entry.getKey(), e);
                        }

                        removedCount++;
                        logger.log(FINE, "Removed and closed inactive connection: " + entry.getKey());
                    } else {
                        // State changed - likely migration started, skip removal
                        logger.log(FINE, "Connection state changed during health check, skipping removal: " + entry.getKey());
                    }
                }
            }

            if (removedCount > 0) {
                logger.log(INFO, "Health check completed: checked " + checkedCount +
                          " connections, removed " + removedCount + " inactive/closed connections");
            }

        } catch (Exception e) {
            logger.log(WARNING, "Error during health check", e);
        }
    }

    /**
     * Get comprehensive statistics information
     */
    public String getStatistics() {
        long currentTime = System.currentTimeMillis();

        // Calculate detailed connection counts
        int activeCount = 0;
        int migratingCount = 0;
        int migratedCount = 0;
        int failedCount = 0;
        int closedCount = 0;
        int openChannelCount = 0;

        for (ConnectionInfo info : connections.values()) {
            switch (info.getState()) {
                case ACTIVE:
                    activeCount++;
                    break;
                case MIGRATING:
                    migratingCount++;
                    break;
                case MIGRATED:
                    migratedCount++;
                    break;
                case FAILED:
                    failedCount++;
                    break;
                case CLOSED:
                    closedCount++;
                    break;
            }

            if (info.getChannel().isOpen()) {
                openChannelCount++;
            }
        }

        return String.format(
            "ConnectionPoolManager Statistics:\n" +
            "  Total Connections Ever: %d\n" +
            "  Current Registered: %d\n" +
            "  Connection States:\n" +
            "    Active: %d\n" +
            "    Migrating: %d\n" +
            "    Migrated: %d\n" +
            "    Failed: %d\n" +
            "    Closed: %d\n" +
            "  Open Channels: %d\n" +
            "  Migration Stats:\n" +
            "    Successful: %d\n" +
            "    Failed: %d\n" +
            "  Configuration:\n" +
            "    Max Retries: %d\n" +
            "    Migration Timeout: %dms\n" +
            "    Connection Timeout: %dms\n" +
            "  Health Check: %s",
            totalConnections.get(),
            connections.size(),
            activeCount,
            migratingCount,
            migratedCount,
            failedCount,
            closedCount,
            openChannelCount,
            successfulMigrations.get(),
            failedMigrations.get(),
            maxMigrationRetries,
            migrationTimeoutMs,
            connectionTimeoutMs,
            healthCheckEnabled ? "Enabled" : "Disabled"
        );
    }

    /**
     * Get connection by ID (for debugging/monitoring)
     */
    public ConnectionInfo getConnection(String connectionId) {
        return connections.get(connectionId);
    }

    /**
     * Get all connection IDs (for monitoring/debugging)
     */
    public java.util.Set<String> getAllConnectionIds() {
        return new java.util.HashSet<>(connections.keySet());
    }

    /**
     * Force abort migration for a specific connection
     */
    public boolean abortConnectionMigration(String connectionId) {
        ConnectionInfo info = connections.get(connectionId);
        if (info != null && info.getState() == ConnectionState.MIGRATING) {
            info.getChannel().abortMigration();
            logger.log(INFO, "Aborted migration for connection: " + connectionId);
            return true;
        }
        return false;
    }

    /**
     * Check if migration system is healthy
     */
    public boolean isHealthy() {
        return healthCheckEnabled &&
               !migrationExecutor.isShutdown() &&
               !scheduler.isShutdown();
    }

    /**
     * Get current configuration as a string
     */
    public String getConfiguration() {
        return String.format(
            "ConnectionPoolManager Configuration:\n" +
            "  Max Migration Retries: %d\n" +
            "  Migration Timeout: %d ms\n" +
            "  Connection Timeout: %d ms\n" +
            "  Health Check: %s\n" +
            "  Migration Executor: %s\n" +
            "  Scheduler: %s",
            maxMigrationRetries,
            migrationTimeoutMs,
            connectionTimeoutMs,
            healthCheckEnabled ? "Enabled" : "Disabled",
            migrationExecutor.isShutdown() ? "Shutdown" : "Running",
            scheduler.isShutdown() ? "Shutdown" : "Running"
        );
    }

    /**
     * Shutdown connection pool manager with proper resource cleanup
     */
    public void shutdown() {
        healthCheckEnabled = false;

        logger.log(INFO, "Shutting down ConnectionPoolManager...");

        // Shutdown executor services
        scheduler.shutdown();
        migrationExecutor.shutdown();

        try {
            // Wait for graceful shutdown
            if (!migrationExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                migrationExecutor.shutdownNow();
            }
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            migrationExecutor.shutdownNow();
            scheduler.shutdownNow();
        }

        // Close and cleanup all connections to prevent resource leaks
        for (Map.Entry<String, ConnectionInfo> entry : connections.entrySet()) {
            ConnectionInfo info = entry.getValue();
            try {
                info.getChannel().closeForcefully();
            } catch (IOException e) {
                logger.log(WARNING, "Error closing channel during shutdown for " +
                          info.getConnectionId(), e);
            } catch (Exception e) {
                logger.log(WARNING, "Unexpected error closing channel during shutdown for " +
                          info.getConnectionId(), e);
            }
            info.setState(ConnectionState.CLOSED);
        }
        connections.clear();

        // Reset total connections counter
        totalConnections.set(0);

        logger.log(INFO, "ConnectionPoolManager shutdown completed");
    }
}