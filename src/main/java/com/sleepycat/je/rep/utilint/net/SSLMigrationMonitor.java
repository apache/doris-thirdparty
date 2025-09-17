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

import java.time.Instant;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

import com.sleepycat.je.rep.net.InstanceLogger;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Level.FINE;

/**
 * Comprehensive SSL migration monitoring and management facade.
 *
 * This class provides a unified interface for monitoring and managing
 * the SSL certificate migration system, combining information from
 * ConnectionPoolManager and SSLContextManager.
 */
public class SSLMigrationMonitor {

    public static class MonitorConfig {
        private final int migrationWarningThreshold;
        private final double migrationWarningPercent;
        private final int deprecatedContextWarningThreshold;
        private final boolean enableDetailedLogging;

        public MonitorConfig() {
            this(10, 0.5, 5, false);
        }

        public MonitorConfig(int migrationWarningThreshold, double migrationWarningPercent,
                        int deprecatedContextWarningThreshold, boolean enableDetailedLogging) {
            this.migrationWarningThreshold = migrationWarningThreshold;
            this.migrationWarningPercent = migrationWarningPercent;
            this.deprecatedContextWarningThreshold = deprecatedContextWarningThreshold;
            this.enableDetailedLogging = enableDetailedLogging;
        }

        public int getMigrationWarningThreshold() { return migrationWarningThreshold; }
        public double getMigrationWarningPercent() { return migrationWarningPercent; }
        public int getDeprecatedContextWarningThreshold() { return deprecatedContextWarningThreshold; }
        public boolean isDetailedLoggingEnabled() { return enableDetailedLogging; }
    }

    public interface HealthChecker {
        HealthStatus check(Map<String, Object> details);
        String getDescription();
    }

    private final ConnectionPoolManager connectionPoolManager;
    private final SSLContextManager contextManager;
    private final InstanceLogger logger;
    private final MonitorConfig config;
    private final List<HealthChecker> healthCheckers;

    /**
     * System health status
     */
    public enum HealthStatus {
        HEALTHY,
        WARNING,
        CRITICAL,
        UNKNOWN
    }

    /**
     * Comprehensive system status
     */
    public static class SystemStatus {
        private final HealthStatus overallHealth;
        private final String summary;
        private final Map<String, Object> details;
        private final Instant timestamp;

        public SystemStatus(HealthStatus health, String summary, Map<String, Object> details) {
            this.overallHealth = health;
            this.summary = summary;
            this.details = details;
            this.timestamp = Instant.now();
        }

        public HealthStatus getOverallHealth() { return overallHealth; }
        public String getSummary() { return summary; }
        public Map<String, Object> getDetails() { return details; }
        public Instant getTimestamp() { return timestamp; }

        @Override
        public String toString() {
            return String.format("SystemStatus{health=%s, summary='%s', timestamp=%s}",
                               overallHealth, summary, timestamp);
        }
    }

    /**
     * Constructor with default configuration
     */
    public SSLMigrationMonitor(ConnectionPoolManager connectionPoolManager,
                             SSLContextManager contextManager,
                             InstanceLogger logger) {
        this(connectionPoolManager, contextManager, logger, new MonitorConfig());
    }

    /**
     * Constructor with custom configuration
     */
    public SSLMigrationMonitor(ConnectionPoolManager connectionPoolManager,
                             SSLContextManager contextManager,
                             InstanceLogger logger,
                             MonitorConfig config) {
        this.connectionPoolManager = connectionPoolManager;
        this.contextManager = contextManager;
        this.logger = logger;
        this.config = config;
        this.healthCheckers = new ArrayList<>();

        // Initialize default health checkers
        initializeDefaultHealthCheckers();
    }

    /**
     * Initialize default health checkers
     */
    private void initializeDefaultHealthCheckers() {
        // Pool health checker
        healthCheckers.add(new HealthChecker() {
            @Override
            public HealthStatus check(Map<String, Object> details) {
                Object poolHealthy = details.get("poolHealthy");
                if (Boolean.FALSE.equals(poolHealthy)) {
                    return HealthStatus.CRITICAL;
                }
                return HealthStatus.HEALTHY;
            }

            @Override
            public String getDescription() {
                return "Connection pool health check";
            }
        });

        // Migration percentage checker
        healthCheckers.add(new HealthChecker() {
            @Override
            public HealthStatus check(Map<String, Object> details) {
                int migratingConnections = getIntValue(details, "migratingConnections", 0);
                int totalConnections = getIntValue(details, "totalConnections", 0);

                if (totalConnections < config.getMigrationWarningThreshold()) {
                    return HealthStatus.HEALTHY;
                }
                if (migratingConnections <= totalConnections * config.getMigrationWarningPercent()) {
                    return HealthStatus.HEALTHY;
                }
                return HealthStatus.WARNING;
            }

            @Override
            public String getDescription() {
                return "Migration load check";
            }
        });

        // Deprecated contexts checker
        healthCheckers.add(new HealthChecker() {
            @Override
            public HealthStatus check(Map<String, Object> details) {
                int deprecatedContexts = getIntValue(details, "deprecatedContexts", 0);
                if (deprecatedContexts <= config.getDeprecatedContextWarningThreshold()) {
                    return HealthStatus.HEALTHY;
                }
                return HealthStatus.WARNING;
            }

            @Override
            public String getDescription() {
                return "SSL context cleanup check";
            }
        });
    }

    /**
     * Safe integer extraction with type conversion handling
     */
    private int getIntValue(Map<String, Object> details, String key, int defaultValue) {
        Object value = details.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            logger.log(FINE, "Failed to parse value for key " + key + ": " + value);
            return defaultValue;
        }
    }

    /**
     * Safe boolean extraction with null handling
     */
    private boolean getBooleanValue(Map<String, Object> details, String key, boolean defaultValue) {
        Object value = details.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        try {
            return Boolean.parseBoolean(value.toString());
        } catch (Exception e) {
            logger.log(FINE, "Failed to parse boolean value for key " + key + ": " + value);
            return defaultValue;
        }
    }

    /**
     * Get comprehensive system status
     */
    public SystemStatus getSystemStatus() {
        Map<String, Object> details = new java.util.HashMap<>();

        try {
            // Collect connection pool stats with safety checks
            if (connectionPoolManager != null && connectionPoolManager.isHealthy()) {
                try {
                    details.put("activeConnections", connectionPoolManager.getActiveConnectionCount());
                    details.put("migratingConnections", connectionPoolManager.getMigratingConnectionCount());
                    details.put("usableConnections", connectionPoolManager.getUsableConnectionCount());
                    details.put("retryingConnections", connectionPoolManager.getRetryingConnectionCount());
                    details.put("failedConnections", connectionPoolManager.getFailedConnectionCount());
                    details.put("totalConnections", connectionPoolManager.getAllConnectionIds().size());
                    details.put("poolHealthy", true);
                } catch (Exception e) {
                    logger.log(WARNING, "Error collecting connection pool stats", e);
                    details.put("poolHealthy", false);
                    details.put("poolError", e.getMessage());
                }
            } else {
                details.put("poolHealthy", false);
                details.put("poolError", "Connection pool manager unavailable or unhealthy");
            }

            // Collect context manager stats with concurrent consistency
            if (contextManager != null && contextManager.hasAvailableContexts()) {
                try {
                    // Get snapshot of context state to ensure consistency
                    java.util.OptionalInt currentVersionOpt = contextManager.getCurrentVersionOpt();
                    details.put("currentContextVersion", currentVersionOpt.orElse(-1));
                    details.put("hasActiveContext", currentVersionOpt.isPresent());

                    Map<Integer, SSLContextManager.ContextVersion> allContexts = contextManager.getAllContextVersions();
                    details.put("totalContexts", allContexts.size());
                    details.put("deprecatedContexts", contextManager.getDeprecatedContextCount());
                    details.put("oldestAvailableVersion", contextManager.getOldestAvailableVersion());
                    details.put("readyForNewCert", contextManager.isReadyForNewCertificate());
                    details.put("contextManagerHealthy", true);
                } catch (Exception e) {
                    logger.log(WARNING, "Error collecting context manager stats", e);
                    details.put("contextManagerHealthy", false);
                    details.put("contextError", e.getMessage());
                }
            } else {
                details.put("contextManagerHealthy", false);
                details.put("contextError", "Context manager unavailable or no contexts");
                details.put("currentContextVersion", -1);
                details.put("hasActiveContext", false);
            }

            // Determine overall health using extensible checker system
            HealthStatus health = determineOverallHealth(details);
            String summary = generateSummary(health, details);

            return new SystemStatus(health, summary, details);

        } catch (OutOfMemoryError | StackOverflowError critical) {
            // Critical JVM errors - propagate immediately
            logger.log(WARNING, "Critical JVM error during status collection: " + critical.getClass().getSimpleName());
            throw critical;
        } catch (Exception e) {
            // All other exceptions - return UNKNOWN status
            logger.log(WARNING, "Unexpected error getting system status", e);
            Map<String, Object> errorDetails = new java.util.HashMap<>();
            errorDetails.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
            errorDetails.put("timestamp", Instant.now().toString());
            return new SystemStatus(HealthStatus.UNKNOWN, "Error retrieving status: " + e.getMessage(), errorDetails);
        }
    }

    /**
     * Determine overall system health using extensible health checkers
     */
    private HealthStatus determineOverallHealth(Map<String, Object> details) {
        HealthStatus worstStatus = HealthStatus.HEALTHY;

        for (HealthChecker checker : healthCheckers) {
            try {
                HealthStatus status = checker.check(details);
                if (config.isDetailedLoggingEnabled()) {
                    logger.log(FINE, checker.getDescription() + ": " + status);
                }

                // Determine worst status (CRITICAL > WARNING > HEALTHY > UNKNOWN)
                if (status == HealthStatus.CRITICAL) {
                    worstStatus = HealthStatus.CRITICAL;
                    break; // Critical is worst, no need to check further
                } else if (status == HealthStatus.WARNING && worstStatus != HealthStatus.CRITICAL) {
                    worstStatus = HealthStatus.WARNING;
                } else if (status == HealthStatus.UNKNOWN && worstStatus == HealthStatus.HEALTHY) {
                    worstStatus = HealthStatus.UNKNOWN;
                }
            } catch (Exception e) {
                logger.log(WARNING, "Error in health checker: " + checker.getDescription(), e);
                if (worstStatus == HealthStatus.HEALTHY) {
                    worstStatus = HealthStatus.UNKNOWN;
                }
            }
        }

        return worstStatus;
    }

    /**
     * Generate human-readable summary with safe type conversion
     */
    private String generateSummary(HealthStatus health, Map<String, Object> details) {
        int activeConnections = getIntValue(details, "activeConnections", 0);
        int migratingConnections = getIntValue(details, "migratingConnections", 0);
        int currentVersion = getIntValue(details, "currentContextVersion", -1);
        boolean hasActiveContext = getBooleanValue(details, "hasActiveContext", false);

        switch (health) {
            case HEALTHY:
                if (hasActiveContext) {
                    return String.format("System healthy: %d active connections, SSL context v%d",
                                       activeConnections, currentVersion);
                } else {
                    return String.format("System healthy: %d active connections, no SSL context",
                                       activeConnections);
                }
            case WARNING:
                return String.format("System warning: %d active, %d migrating connections, SSL context v%d",
                                   activeConnections, migratingConnections, currentVersion);
            case CRITICAL:
                String poolError = (String) details.get("poolError");
                String contextError = (String) details.get("contextError");
                if (poolError != null) {
                    return "System critical: " + poolError;
                } else if (contextError != null) {
                    return "System critical: " + contextError;
                } else {
                    return "System critical: Unknown critical error";
                }
            default:
                String error = (String) details.get("error");
                return error != null ? "System status unknown: " + error : "System status unknown";
        }
    }

    /**
     * Get detailed migration report
     */
    public String getDetailedReport() {
        StringBuilder report = new StringBuilder();
        report.append("=== SSL Migration System Detailed Report ===\n");
        report.append("Generated at: ").append(Instant.now()).append("\n\n");

        SystemStatus status = getSystemStatus();
        report.append("Overall Health: ").append(status.getOverallHealth()).append("\n");
        report.append("Summary: ").append(status.getSummary()).append("\n\n");

        if (connectionPoolManager != null) {
            report.append("=== Connection Pool Manager ===\n");
            report.append(connectionPoolManager.getStatistics()).append("\n\n");
            report.append(connectionPoolManager.getConfiguration()).append("\n\n");
        }

        if (contextManager != null) {
            report.append("=== SSL Context Manager ===\n");
            report.append(contextManager.getStatistics()).append("\n\n");
            report.append(contextManager.getContextUsageSummary()).append("\n\n");
        }

        return report.toString();
    }

    /**
     * Perform system health check and return recommendations with safe type handling
     */
    public String performHealthCheck() {
        SystemStatus status = getSystemStatus();
        StringBuilder recommendations = new StringBuilder();

        recommendations.append("Health Check Results:\n");
        recommendations.append("Status: ").append(status.getOverallHealth()).append("\n");
        recommendations.append("Summary: ").append(status.getSummary()).append("\n");
        recommendations.append("Timestamp: ").append(status.getTimestamp()).append("\n\n");

        Map<String, Object> details = status.getDetails();

        // Generate recommendations based on status
        if (status.getOverallHealth() == HealthStatus.CRITICAL) {
            recommendations.append("CRITICAL ISSUES:\n");

            String poolError = (String) details.get("poolError");
            String contextError = (String) details.get("contextError");

            if (poolError != null) {
                recommendations.append("- Connection pool error: ").append(poolError).append("\n");
            }
            if (contextError != null) {
                recommendations.append("- SSL context error: ").append(contextError).append("\n");
            }
            recommendations.append("- Immediate attention required\n");
            recommendations.append("- Consider restarting SSL migration system\n\n");
        }

        if (status.getOverallHealth() == HealthStatus.WARNING) {
            recommendations.append("WARNINGS:\n");

            int migratingCount = getIntValue(details, "migratingConnections", 0);
            int totalCount = getIntValue(details, "totalConnections", 0);
            int retryingCount = getIntValue(details, "retryingConnections", 0);
            int failedCount = getIntValue(details, "failedConnections", 0);

            if (migratingCount > 0) {
                double migrationPercent = totalCount > 0 ? (double) migratingCount / totalCount * 100 : 0;
                recommendations.append("- ").append(migratingCount).append(" connections migrating")
                              .append(String.format(" (%.1f%% of total)", migrationPercent)).append("\n");
                recommendations.append("- Monitor migration progress closely\n");
            }

            if (retryingCount > 0) {
                recommendations.append("- ").append(retryingCount).append(" connections retrying migration\n");
            }

            if (failedCount > 0) {
                recommendations.append("- ").append(failedCount).append(" connections failed migration\n");
                recommendations.append("- Investigate migration failures\n");
            }

            int deprecatedCount = getIntValue(details, "deprecatedContexts", 0);
            if (deprecatedCount > config.getDeprecatedContextWarningThreshold()) {
                recommendations.append("- Too many deprecated SSL contexts (").append(deprecatedCount).append(")\n");
                recommendations.append("- Consider forcing cleanup: contextManager.forceCleanupDeprecatedContexts()\n");
            }
            recommendations.append("\n");
        }

        if (status.getOverallHealth() == HealthStatus.HEALTHY) {
            recommendations.append("SYSTEM HEALTHY:\n");
            recommendations.append("- All systems operating normally\n");

            boolean readyForNewCert = getBooleanValue(details, "readyForNewCert", false);
            if (readyForNewCert) {
                recommendations.append("- System ready for new certificate deployment\n");
            } else {
                recommendations.append("- System not ready for new certificate (migrations in progress)\n");
            }

            // Provide optimization suggestions
            int activeCount = getIntValue(details, "activeConnections", 0);
            int usableCount = getIntValue(details, "usableConnections", 0);
            if (activeCount > 0 && usableCount > activeCount) {
                recommendations.append("- ").append(usableCount - activeCount)
                              .append(" additional connections available after migration\n");
            }
        }

        if (status.getOverallHealth() == HealthStatus.UNKNOWN) {
            recommendations.append("STATUS UNKNOWN:\n");
            String error = (String) details.get("error");
            if (error != null) {
                recommendations.append("- Error: ").append(error).append("\n");
            }
            recommendations.append("- Unable to determine system health\n");
            recommendations.append("- Check system logs for more details\n");
        }

        return recommendations.toString();
    }

    /**
     * Get active migration summary
     */
    public String getActiveMigrationSummary() {
        if (connectionPoolManager == null) {
            return "Migration monitoring not available";
        }

        Set<String> connectionIds = connectionPoolManager.getAllConnectionIds();
        StringBuilder summary = new StringBuilder();
        summary.append("Active Migration Summary:\n");

        int migratingCount = 0;
        int failedCount = 0;
        int migratedCount = 0;

        for (String id : connectionIds) {
            ConnectionPoolManager.ConnectionInfo info = connectionPoolManager.getConnection(id);
            if (info != null) {
                switch (info.getState()) {
                    case MIGRATING:
                        migratingCount++;
                        summary.append("  ").append(id).append(": MIGRATING (attempt ")
                               .append(info.getMigrationAttempts()).append(")\n");
                        break;
                    case FAILED:
                        failedCount++;
                        summary.append("  ").append(id).append(": FAILED - ")
                               .append(info.getErrorMessage()).append("\n");
                        break;
                    case MIGRATED:
                        migratedCount++;
                        break;
                }
            }
        }

        summary.append("Total: ").append(migratingCount).append(" migrating, ")
               .append(migratedCount).append(" migrated, ")
               .append(failedCount).append(" failed\n");

        return summary.toString();
    }

    /**
     * Add a custom health checker
     */
    public void addHealthChecker(HealthChecker checker) {
        if (checker != null) {
            healthCheckers.add(checker);
            logger.log(INFO, "Added health checker: " + checker.getDescription());
        }
    }

    /**
     * Remove a health checker
     */
    public boolean removeHealthChecker(HealthChecker checker) {
        boolean removed = healthCheckers.remove(checker);
        if (removed) {
            logger.log(INFO, "Removed health checker: " + checker.getDescription());
        }
        return removed;
    }

    /**
     * Get current configuration
     */
    public MonitorConfig getConfig() {
        return config;
    }

    /**
     * Get health checker descriptions
     */
    public List<String> getHealthCheckerDescriptions() {
        List<String> descriptions = new ArrayList<>();
        for (HealthChecker checker : healthCheckers) {
            descriptions.add(checker.getDescription());
        }
        return descriptions;
    }
}