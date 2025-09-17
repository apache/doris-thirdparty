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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.time.Instant;
import java.time.Duration;
import java.util.OptionalInt;

import javax.net.ssl.SSLContext;

import com.sleepycat.je.rep.net.InstanceLogger;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Level.FINE;

/**
 * SSL context manager with versioning support for graceful certificate migration.
 *
 * This manager maintains multiple versions of SSL contexts to enable seamless
 * transitions during certificate reloads. Old contexts are kept alive for a
 * configurable period to allow existing connections to complete their operations.
 *
 * Features:
 * - Version-based SSL context management
 * - Graceful context transitions
 * - Backward compatibility for existing connections
 * - Automatic cleanup of expired contexts
 * - Thread-safe operations
 */
public class SSLContextManager {

    /**
     * SSL context version information with thread-safe timestamp updates
     */
    public static class ContextVersion {
        private final int version;
        private final SSLContext serverContext;
        private final SSLContext clientContext;
        private final Instant creationTime;
        private final AtomicReference<Instant> lastUsedTime;
        private final AtomicBoolean deprecated;

        public ContextVersion(int version, SSLContext serverContext, SSLContext clientContext) {
            this.version = version;
            this.serverContext = serverContext;
            this.clientContext = clientContext;
            this.creationTime = Instant.now();
            this.lastUsedTime = new AtomicReference<>(this.creationTime);
            this.deprecated = new AtomicBoolean(false);
        }

        public int getVersion() { return version; }
        public SSLContext getServerContext() { return serverContext; }
        public SSLContext getClientContext() { return clientContext; }
        public Instant getCreationTime() { return creationTime; }
        public Instant getLastUsedTime() { return lastUsedTime.get(); }
        public boolean isDeprecated() { return deprecated.get(); }

        /**
         * Thread-safe update of last used time with throttling to reduce contention
         */
        public void updateLastUsed() {
            Instant now = Instant.now();
            Instant lastUsed = this.lastUsedTime.get();

            // Only update if more than 1 second has passed to reduce contention
            if (Duration.between(lastUsed, now).toMillis() > 1000) {
                this.lastUsedTime.compareAndSet(lastUsed, now);
            }
        }

        public void markDeprecated() {
            this.deprecated.set(true);
        }

        public Duration getAge() {
            return Duration.between(creationTime, Instant.now());
        }

        public Duration getTimeSinceLastUse() {
            return Duration.between(lastUsedTime.get(), Instant.now());
        }
    }

    // Version management
    private final AtomicInteger currentVersion = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, ContextVersion> contexts = new ConcurrentHashMap<>();

    // Synchronization
    private final ReentrantReadWriteLock contextLock = new ReentrantReadWriteLock();

    // Configuration
    private final Duration maxContextAge;
    private final Duration deprecatedContextGracePeriod;
    private final int maxContextVersions;

    // Logger
    private final InstanceLogger logger;

    // Active context tracking
    private volatile ContextVersion activeContext;

    // Async cleanup executor
    private final ScheduledExecutorService cleanupExecutor;

    /**
     * Constructor with default settings
     */
    public SSLContextManager(InstanceLogger logger) {
        this(logger, Duration.ofHours(24), Duration.ofMinutes(30), 10);
    }

    /**
     * Constructor with custom settings
     */
    public SSLContextManager(InstanceLogger logger,
                           Duration maxContextAge,
                           Duration deprecatedContextGracePeriod,
                           int maxContextVersions) {
        this.logger = logger;
        this.maxContextAge = maxContextAge;
        this.deprecatedContextGracePeriod = deprecatedContextGracePeriod;
        this.maxContextVersions = maxContextVersions;

        // Initialize async cleanup executor
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "SSLContextCleanup");
            t.setDaemon(true);
            return t;
        });

        logger.log(INFO, "SSLContextManager initialized with " +
                  "maxAge=" + maxContextAge +
                  ", gracePeriod=" + deprecatedContextGracePeriod +
                  ", maxVersions=" + maxContextVersions);
    }

    /**
     * Register a new SSL context version
     */
    public int registerNewContext(SSLContext serverContext, SSLContext clientContext) {
        if (serverContext == null || clientContext == null) {
            throw new IllegalArgumentException("SSL contexts cannot be null");
        }

        contextLock.writeLock().lock();
        try {
            int newVersion = currentVersion.incrementAndGet();
            ContextVersion contextVersion = new ContextVersion(newVersion, serverContext, clientContext);

            // Mark previous active context as deprecated
            if (activeContext != null) {
                activeContext.markDeprecated();
                logger.log(INFO, "Marked SSL context version " + activeContext.getVersion() + " as deprecated");
            }

            // Register new context
            contexts.put(newVersion, contextVersion);
            activeContext = contextVersion;

            logger.log(INFO, "Registered new SSL context version: " + newVersion);

            // Schedule async cleanup to avoid blocking registration
            scheduleAsyncCleanup();

            return newVersion;

        } finally {
            contextLock.writeLock().unlock();
        }
    }

    /**
     * Get the current active context version (improved semantics)
     */
    public OptionalInt getCurrentVersionOpt() {
        ContextVersion current = activeContext;
        return current != null ? OptionalInt.of(current.getVersion()) : OptionalInt.empty();
    }

    /**
     * Get the current active context version (legacy method - returns -1 if none)
     */
    public int getCurrentVersion() {
        ContextVersion current = activeContext;
        return current != null ? current.getVersion() : -1;
    }

    /**
     * Get SSL context by version for server mode
     */
    public SSLContext getServerContext(int version) {
        ContextVersion contextVersion = getContextVersion(version);
        if (contextVersion != null) {
            contextVersion.updateLastUsed();
            return contextVersion.getServerContext();
        }
        return null;
    }

    /**
     * Get SSL context by version for client mode
     */
    public SSLContext getClientContext(int version) {
        ContextVersion contextVersion = getContextVersion(version);
        if (contextVersion != null) {
            contextVersion.updateLastUsed();
            return contextVersion.getClientContext();
        }
        return null;
    }

    /**
     * Get current server context with consistent locking
     */
    public SSLContext getCurrentServerContext() {
        contextLock.readLock().lock();
        try {
            ContextVersion current = activeContext;
            if (current != null) {
                current.updateLastUsed();
                return current.getServerContext();
            }
            return null;
        } finally {
            contextLock.readLock().unlock();
        }
    }

    /**
     * Get current client context with consistent locking
     */
    public SSLContext getCurrentClientContext() {
        contextLock.readLock().lock();
        try {
            ContextVersion current = activeContext;
            if (current != null) {
                current.updateLastUsed();
                return current.getClientContext();
            }
            return null;
        } finally {
            contextLock.readLock().unlock();
        }
    }

    /**
     * Get context version information
     */
    public ContextVersion getContextVersion(int version) {
        contextLock.readLock().lock();
        try {
            return contexts.get(version);
        } finally {
            contextLock.readLock().unlock();
        }
    }

    /**
     * Check if a context version exists and is usable
     */
    public boolean isContextVersionAvailable(int version) {
        ContextVersion contextVersion = getContextVersion(version);
        if (contextVersion == null) {
            return false;
        }

        // Check if context is too old
        if (contextVersion.getAge().compareTo(maxContextAge) > 0) {
            return false;
        }

        // Check if deprecated context has exceeded grace period
        if (contextVersion.isDeprecated() &&
            contextVersion.getTimeSinceLastUse().compareTo(deprecatedContextGracePeriod) > 0) {
            return false;
        }

        return true;
    }

    /**
     * Get all available context versions
     */
    public Map<Integer, ContextVersion> getAllContextVersions() {
        contextLock.readLock().lock();
        try {
            return Collections.unmodifiableMap(new HashMap<>(contexts));
        } finally {
            contextLock.readLock().unlock();
        }
    }

    /**
     * Force cleanup of a specific context version
     */
    public boolean removeContextVersion(int version) {
        if (version == getCurrentVersion()) {
            logger.log(WARNING, "Cannot remove current active context version: " + version);
            return false;
        }

        contextLock.writeLock().lock();
        try {
            ContextVersion removed = contexts.remove(version);
            if (removed != null) {
                logger.log(INFO, "Manually removed SSL context version: " + version);
                return true;
            }
            return false;
        } finally {
            contextLock.writeLock().unlock();
        }
    }

    /**
     * Schedule async cleanup to avoid blocking registration
     */
    private void scheduleAsyncCleanup() {
        cleanupExecutor.schedule(this::cleanupExpiredContexts, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Cleanup expired contexts
     */
    public void cleanupExpiredContexts() {
        contextLock.writeLock().lock();
        try {
            Instant now = Instant.now();
            AtomicInteger removedCount = new AtomicInteger(0);

            // Remove contexts that are too old or exceeded grace period
            contexts.entrySet().removeIf(entry -> {
                ContextVersion contextVersion = entry.getValue();
                int version = entry.getKey();

                // Never remove current active context
                if (version == getCurrentVersion()) {
                    return false;
                }

                // Remove if too old
                if (contextVersion.getAge().compareTo(maxContextAge) > 0) {
                    logger.log(FINE, "Removing aged SSL context version: " + version);
                    removedCount.incrementAndGet();
                    return true;
                }

                // Remove deprecated contexts that exceeded grace period
                if (contextVersion.isDeprecated() &&
                    contextVersion.getTimeSinceLastUse().compareTo(deprecatedContextGracePeriod) > 0) {
                    logger.log(FINE, "Removing deprecated SSL context version: " + version);
                    removedCount.incrementAndGet();
                    return true;
                }

                return false;
            });

            // Limit number of contexts to prevent memory issues
            if (contexts.size() > maxContextVersions) {
                // Remove oldest deprecated contexts first
                int limitRemoveCount = contexts.entrySet().stream()
                    .filter(entry -> entry.getKey() != getCurrentVersion())
                    .filter(entry -> entry.getValue().isDeprecated())
                    .sorted((e1, e2) -> e1.getValue().getCreationTime().compareTo(e2.getValue().getCreationTime()))
                    .limit(contexts.size() - maxContextVersions)
                    .mapToInt(entry -> {
                        contexts.remove(entry.getKey());
                        logger.log(FINE, "Removed SSL context version due to limit: " + entry.getKey());
                        return 1;
                    })
                    .sum();

                removedCount.addAndGet(limitRemoveCount);
            }

            if (removedCount.get() > 0) {
                logger.log(INFO, "Cleaned up " + removedCount.get() + " expired SSL contexts");
            }

        } finally {
            contextLock.writeLock().unlock();
        }
    }

    /**
     * Get statistics about context usage
     */
    public String getStatistics() {
        contextLock.readLock().lock();
        try {
            StringBuilder stats = new StringBuilder();
            stats.append("SSLContextManager Statistics:\n");
            stats.append("  Current Version: ").append(getCurrentVersion()).append("\n");
            stats.append("  Total Contexts: ").append(contexts.size()).append("\n");

            int activeCount = 0;
            int deprecatedCount = 0;

            for (ContextVersion contextVersion : contexts.values()) {
                if (contextVersion.isDeprecated()) {
                    deprecatedCount++;
                } else {
                    activeCount++;
                }
            }

            stats.append("  Active Contexts: ").append(activeCount).append("\n");
            stats.append("  Deprecated Contexts: ").append(deprecatedCount).append("\n");
            stats.append("  Max Context Age: ").append(maxContextAge).append("\n");
            stats.append("  Grace Period: ").append(deprecatedContextGracePeriod).append("\n");

            // Detail per context
            stats.append("  Context Details:\n");
            for (ContextVersion contextVersion : contexts.values()) {
                stats.append("    Version ").append(contextVersion.getVersion())
                     .append(": age=").append(contextVersion.getAge())
                     .append(", lastUsed=").append(contextVersion.getTimeSinceLastUse())
                     .append(", deprecated=").append(contextVersion.isDeprecated())
                     .append("\n");
            }

            return stats.toString();

        } finally {
            contextLock.readLock().unlock();
        }
    }

    /**
     * Get context usage summary for monitoring
     */
    public String getContextUsageSummary() {
        contextLock.readLock().lock();
        try {
            StringBuilder summary = new StringBuilder();
            summary.append("SSL Context Usage Summary:\n");
            summary.append(String.format("  Total Contexts: %d\n", contexts.size()));
            summary.append("  Context Details:\n");

            for (Map.Entry<Integer, ContextVersion> entry : contexts.entrySet()) {
                ContextVersion ctx = entry.getValue();
                summary.append(String.format(
                    "    Version %d: age=%s, lastUsed=%s, deprecated=%s%s\n",
                    entry.getKey(),
                    formatDuration(ctx.getAge()),
                    formatDuration(ctx.getTimeSinceLastUse()),
                    ctx.isDeprecated(),
                    entry.getKey() == getCurrentVersion() ? " (CURRENT)" : ""
                ));
            }

            return summary.toString();
        } finally {
            contextLock.readLock().unlock();
        }
    }

    /**
     * Force cleanup of deprecated contexts (for admin use)
     */
    public int forceCleanupDeprecatedContexts() {
        contextLock.writeLock().lock();
        try {
            int removedCount = 0;
            int currentVersion = getCurrentVersion();

            // Remove all deprecated contexts except current
            removedCount = (int) contexts.entrySet().removeIf(entry -> {
                if (entry.getKey() != currentVersion && entry.getValue().isDeprecated()) {
                    logger.log(INFO, "Force removed deprecated SSL context version: " + entry.getKey());
                    return true;
                }
                return false;
            });

            logger.log(INFO, "Force cleanup removed " + removedCount + " deprecated contexts");
            return removedCount;
        } finally {
            contextLock.writeLock().unlock();
        }
    }

    /**
     * Check if system is ready for new certificate deployment
     */
    public boolean isReadyForNewCertificate() {
        contextLock.readLock().lock();
        try {
            // System is ready if there are no ongoing migrations
            // (indicated by having only one active context or all old contexts are deprecated)
            if (contexts.size() <= 1) {
                return true;
            }

            int currentVersion = getCurrentVersion();
            return contexts.entrySet().stream()
                    .filter(entry -> entry.getKey() != currentVersion)
                    .allMatch(entry -> entry.getValue().isDeprecated());
        } finally {
            contextLock.readLock().unlock();
        }
    }

    /**
     * Format duration with comprehensive time range support (simplified)
     */
    private String formatDuration(Duration duration) {
        // Handle edge cases
        if (duration.isNegative()) {
            return "negative";
        }
        if (duration.isZero()) {
            return "0s";
        }

        long totalSeconds = duration.getSeconds();
        int nanos = duration.getNano();

        // Handle sub-second durations first
        if (totalSeconds == 0) {
            if (nanos >= 1_000_000) {
                return (nanos / 1_000_000) + "ms";
            } else if (nanos >= 1_000) {
                return (nanos / 1_000) + "μs";
            } else {
                return nanos + "ns";
            }
        }

        // Handle durations >= 1 second
        StringBuilder result = new StringBuilder();

        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;

        // Build duration string progressively
        boolean hasContent = false;

        if (days > 0) {
            result.append(days).append("d");
            hasContent = true;
            if (hours > 0 && days < 7) { // Show hours for less than a week
                result.append(hours).append("h");
            }
        } else if (hours > 0) {
            result.append(hours).append("h");
            hasContent = true;
            if (minutes > 0) {
                result.append(minutes).append("m");
            }
        } else if (minutes > 0) {
            result.append(minutes).append("m");
            hasContent = true;
            if (seconds > 0) {
                result.append(seconds).append("s");
            }
        } else {
            result.append(seconds).append("s");
            hasContent = true;
        }

        // Add milliseconds for short durations (< 10 seconds)
        if (hasContent && totalSeconds < 10 && nanos >= 1_000_000) {
            result.append((nanos / 1_000_000)).append("ms");
        }

        return result.toString();
    }

    /**
     * Shutdown the context manager
     */
    public void shutdown() {
        contextLock.writeLock().lock();
        try {
            logger.log(INFO, "Shutting down SSLContextManager with " + contexts.size() + " contexts");

            // Shutdown cleanup executor first
            cleanupExecutor.shutdown();
            try {
                if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cleanupExecutor.shutdownNow();
            }

            contexts.clear();
            activeContext = null;
        } finally {
            contextLock.writeLock().unlock();
        }
    }

    /**
     * Check if there are any available contexts
     */
    public boolean hasAvailableContexts() {
        return !contexts.isEmpty() && activeContext != null;
    }

    /**
     * Get the version of the oldest available context (optimized to avoid nested locking)
     */
    public int getOldestAvailableVersion() {
        contextLock.readLock().lock();
        try {
            int oldestVersion = -1;

            for (Map.Entry<Integer, ContextVersion> entry : contexts.entrySet()) {
                int version = entry.getKey();
                ContextVersion contextVersion = entry.getValue();

                // Check availability inline to avoid nested locking
                if (contextVersion != null) {
                    // Check if context is too old
                    if (contextVersion.getAge().compareTo(maxContextAge) > 0) {
                        continue;
                    }

                    // Check if deprecated context has exceeded grace period
                    if (contextVersion.isDeprecated() &&
                        contextVersion.getTimeSinceLastUse().compareTo(deprecatedContextGracePeriod) > 0) {
                        continue;
                    }

                    // This version is available, check if it's the oldest so far
                    if (oldestVersion == -1 || version < oldestVersion) {
                        oldestVersion = version;
                    }
                }
            }

            return oldestVersion;
        } finally {
            contextLock.readLock().unlock();
        }
    }

    /**
     * Get the number of deprecated contexts still available
     */
    public int getDeprecatedContextCount() {
        contextLock.readLock().lock();
        try {
            return (int) contexts.values().stream()
                    .filter(ContextVersion::isDeprecated)
                    .count();
        } finally {
            contextLock.readLock().unlock();
        }
    }
}