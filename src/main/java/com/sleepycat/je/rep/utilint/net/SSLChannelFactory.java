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

package com.sleepycat.je.rep.utilint.net;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Level.FINE;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.InstanceContext;
import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.rep.net.InstanceParams;
import com.sleepycat.je.rep.net.PasswordSource;
import com.sleepycat.je.rep.net.SSLAuthenticator;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder.CtorArgSpec;

/**
 * A factory class for generating SSLDataChannel instances based on
 * SocketChannel instances.
 */
public class SSLChannelFactory implements DataChannelFactory {

    /*
     * Protocol to use in call to SSLContext.getInstance.  This isn't a
     * protocol per-se.  Actual protocol selection is chosen at the time
     * a connection is established based on enabled protocol settings for
     * both client and server.
     */
    private static final String SSL_CONTEXT_PROTOCOL = "TLSv1.2";

    /**
     * A system property to allow users to specify the correct X509 certificate
     * algorithm name based on the JVMs they are using.
     */
    private static final String X509_ALGO_NAME_PROPERTY =
        "je.ssl.x509AlgoName";

    /**
     * The algorithm name of X509 certificate. It depends on the vendor of JVM.
     */
    private static final String X509_ALGO_NAME = getX509AlgoName();

    /**
     * An SSLContext that will hold all the interesting connection parameter
     * information for session creation in server mode.
     */
    private volatile SSLContext serverSSLContext;

    /**
     * An SSLContext that will hold all the interesting connection parameter
     * information for session creation in client mode.
     */
    private volatile SSLContext clientSSLContext;

    /**
     * Backup SSL contexts for graceful certificate transitions.
     * These contexts hold the previous certificates during smooth certificate migration.
     */
    private volatile SSLContext backupServerSSLContext;
    private volatile SSLContext backupClientSSLContext;

    /**
     * Certificate transition management.
     * Controls smooth transition from old to new certificates.
     * We use a queue-based approach to handle multiple certificate changes.
     */
    private volatile boolean inCertificateTransition = false;
    private volatile long transitionStartTime = 0;
    private final ConcurrentHashMap<String, Long> pendingReloads = new ConcurrentHashMap<>();

    /**
     * The base SSLParameters for use in channel creation.
     */
    private final SSLParameters baseSSLParameters;

    /**
     * An authenticator object for validating SSL session peers when acting
     * in server mode
     */
    private final SSLAuthenticator sslAuthenticator;

    /**
     * A host verifier object for validating SSL session peers when acting
     * in client mode
     */
    private final HostnameVerifier sslHostVerifier;

    private final InstanceLogger logger;

    /**
     * Instance parameters used for SSL context reconstruction during certificate reload
     */
    private final InstanceParams instanceParams;

    /**
     * Certificate file watcher for monitoring certificate file changes
     */
    private CertificateFileWatcher certificateWatcher;

    /**
     * Constructor for use during creating based on access configuration
     */
    public SSLChannelFactory(InstanceParams params) {
        this.instanceParams = params;
        serverSSLContext = constructSSLContext(params, false);
        clientSSLContext = constructSSLContext(params, true);
        baseSSLParameters =
            filterSSLParameters(constructSSLParameters(params),
                                serverSSLContext);
        sslAuthenticator = constructSSLAuthenticator(params);
        sslHostVerifier = constructSSLHostVerifier(params);
        logger = params.getContext().getLoggerFactory().getLogger(getClass());

        // Initialize certificate file monitoring
        initializeCertificateWatcher();
    }

    /**
     * Constructor for use when SSL configuration objects have already
     * been constructed.
     */
    public SSLChannelFactory(SSLContext serverSSLContext,
                             SSLContext clientSSLContext,
                             SSLParameters baseSSLParameters,
                             SSLAuthenticator sslAuthenticator,
                             HostnameVerifier sslHostVerifier,
                             InstanceLogger logger) {

        this.instanceParams = null; // No automatic reloading for pre-configured contexts
        this.serverSSLContext = serverSSLContext;
        this.clientSSLContext = clientSSLContext;
        this.baseSSLParameters =
            filterSSLParameters(baseSSLParameters, serverSSLContext);
        this.sslAuthenticator = sslAuthenticator;
        this.sslHostVerifier = sslHostVerifier;
        this.logger = logger;
    }

    /**
     * Construct a DataChannel wrapping the newly accepted SocketChannel
     */
    @Override
    public DataChannel acceptChannel(SocketChannel socketChannel) {

        final SocketAddress socketAddress =
            socketChannel.socket().getRemoteSocketAddress();
        String host = null;
        if (socketAddress == null) {
            throw new IllegalArgumentException(
                "socketChannel is not connected");
        }

        if (socketAddress instanceof InetSocketAddress) {
            host = ((InetSocketAddress)socketAddress).getAddress().toString();
        }

        final SSLEngine engine =
            serverSSLContext.createSSLEngine(host,
                                             socketChannel.socket().getPort());
        engine.setSSLParameters(baseSSLParameters);
        engine.setUseClientMode(false);
        if (sslAuthenticator != null) {
            engine.setWantClientAuth(true);
        }

        return new SSLDataChannel(socketChannel, engine, null, null,
                                  sslAuthenticator, logger);
    }

    /**
     * Construct a DataChannel wrapping a new connection to the specified
     * address using the associated connection options.
     */
    @Override
    public DataChannel connect(InetSocketAddress addr,
                               InetSocketAddress localAddr,
                               ConnectOptions connectOptions)
        throws IOException {

        final SocketChannel socketChannel =
            RepUtils.openSocketChannel(addr, localAddr, connectOptions);

        /*
         * Figure out a good host to specify.  This is used for session caching
         * so it's not critical what answer we come up with, so long as it
         * is relatively repeatable.
         */
        String host = addr.getHostName();
        if (host == null) {
            host = addr.getAddress().toString();
        }

        final SSLEngine engine =
            clientSSLContext.createSSLEngine(host, addr.getPort());
        engine.setSSLParameters(baseSSLParameters);
        engine.setUseClientMode(true);

        return new SSLDataChannel(
            socketChannel, engine, host, sslHostVerifier, null, logger);
    }

    /**
     * Reads the KeyStore configured in the ReplicationNetworkConfig into
     * memory.
     */
    public static KeyStore readKeyStore(InstanceContext context) {

        KeyStoreInfo ksInfo = readKeyStoreInfo(context);
        try {
            return ksInfo.ks;
        } finally {
            ksInfo.clearPassword();
        }
    }

    /**
     * Checks whether the auth string is a valid authenticator specification
     */
    public static boolean isValidAuthenticator(String authSpec) {
        authSpec = authSpec.trim();

        if (authSpec.equals("") || authSpec.equals("mirror")) {
            return true;
        }

        if (authSpec.startsWith("dnmatch(") && authSpec.endsWith(")")) {
            try {
                SSLDNAuthenticator.validate(authSpec);
                return true;
            } catch(IllegalArgumentException iae) {
                return false;
            }
        }

        return false;
    }

    /**
     * Checks whether input string is a valid host verifier specification
     */
    public static boolean isValidHostVerifier(String hvSpec) {

        hvSpec = hvSpec.trim();

        if (hvSpec.equals("") || hvSpec.equals("mirror") ||
            hvSpec.equals("hostname")) {
            return true;
        }

        if (hvSpec.startsWith("dnmatch(") && hvSpec.endsWith(")")) {
            try {
                SSLDNHostVerifier.validate(hvSpec);
            } catch (IllegalArgumentException iae) {
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Initialize certificate file watcher for automatic reloading.
     */
    private void initializeCertificateWatcher() {
        if (instanceParams == null) {
            return;
        }

        try {
            final ReplicationSSLConfig config =
                (ReplicationSSLConfig) instanceParams.getContext().getRepNetConfig();

            // Get the refresh interval from configuration
            long refreshInterval = config.getSSLCertRefreshIntervalSeconds();

            // If refresh interval is 0, disable certificate monitoring
            if (refreshInterval == 0) {
                logger.log(INFO, "Certificate file monitoring disabled (refresh interval = 0)");
                return;
            }

            certificateWatcher = new CertificateFileWatcher(logger, refreshInterval);

            // Watch keystore file if configured
            String keystorePath = config.getSSLKeyStore();
            if (keystorePath == null || keystorePath.isEmpty()) {
                keystorePath = System.getProperty("javax.net.ssl.keyStore");
            }
            if (keystorePath != null && !keystorePath.isEmpty()) {
                certificateWatcher.registerFile(keystorePath, this::reloadCertificates);
            }

            // Watch truststore file if configured
            String truststorePath = config.getSSLTrustStore();
            if (truststorePath == null || truststorePath.isEmpty()) {
                truststorePath = System.getProperty("javax.net.ssl.trustStore");
            }
            if (truststorePath != null && !truststorePath.isEmpty()) {
                certificateWatcher.registerFile(truststorePath, this::reloadCertificates);
            }

            logger.log(INFO, "Certificate file monitoring initialized with " + refreshInterval + " second interval");

        } catch (Exception e) {
            logger.log(WARNING, "Failed to initialize certificate file watcher: " + e.getMessage());
        }
    }

    /**
     * Reload certificates when file changes are detected.
     * This method implements smart certificate transition that handles multiple file changes efficiently.
     *
     * @param changedFilePath the path of the file that changed
     */
    private void reloadCertificates(String changedFilePath) {
        if (instanceParams == null) {
            logger.log(WARNING, "Cannot reload certificates: instance parameters not available");
            return;
        }

        logger.log(INFO, "Starting smart certificate reload due to file change: " + changedFilePath);

        // Record this file change with timestamp
        pendingReloads.put(changedFilePath, System.currentTimeMillis());

        try {
            // Start or continue the certificate transition process
            smartCertificateTransition();

        } catch (Exception e) {
            logger.log(WARNING, "Failed to reload SSL certificates: " + e.getMessage());
            // Remove from pending if failed
            pendingReloads.remove(changedFilePath);
        }
    }

    /**
     * Smart certificate transition that handles multiple file changes efficiently.
     * This approach:
     * 1. Collects multiple certificate changes in a short time window
     * 2. Performs a single reload operation for all changes
     * 3. Avoids redundant SSL context reconstruction
     * 4. Supports independent processing of different certificate files
     */
    private void smartCertificateTransition() {
        synchronized (this) {
            if (inCertificateTransition) {
                // Already in progress, the pending changes will be picked up
                logger.log(INFO, "Certificate transition in progress, queuing additional changes");
                return;
            }

            if (pendingReloads.isEmpty()) {
                return;
            }

            inCertificateTransition = true;
            transitionStartTime = System.currentTimeMillis();
        }

        // Use a background thread to handle the actual transition
        Thread transitionThread = new Thread(this::performCertificateTransition, "SSLCertTransition");
        transitionThread.setDaemon(true);
        transitionThread.start();
    }

    /**
     * Perform the actual certificate transition in a background thread.
     */
    private void performCertificateTransition() {
        try {
            // Wait a short time to collect multiple file changes
            Thread.sleep(100); // 100ms debounce window

            ConcurrentHashMap<String, Long> currentPendingReloads;
            synchronized (this) {
                if (pendingReloads.isEmpty()) {
                    inCertificateTransition = false;
                    return;
                }

                // Take a snapshot of pending reloads
                currentPendingReloads = new ConcurrentHashMap<>(pendingReloads);
                pendingReloads.clear();
            }

            logger.log(INFO, "Processing certificate changes for " + currentPendingReloads.size() + " files: " +
                      String.join(", ", currentPendingReloads.keySet()));

            logger.log(FINE, "Pre-loading new SSL certificates in background");

            // Step 1: Pre-construct new SSL contexts (lightweight validation)
            SSLContext candidateServerContext = constructSSLContext(instanceParams, false);
            SSLContext candidateClientContext = constructSSLContext(instanceParams, true);

            // Step 2: Validate new contexts by creating a test SSL engine
            validateSSLContext(candidateServerContext, "server");
            validateSSLContext(candidateClientContext, "client");

            logger.log(FINE, "New SSL certificates validated successfully");

            // Step 3: Store current contexts as backup for graceful fallback
            backupServerSSLContext = serverSSLContext;
            backupClientSSLContext = clientSSLContext;

            // Step 4: Atomically switch to new contexts (this is the actual lightweight transition)
            this.serverSSLContext = candidateServerContext;
            this.clientSSLContext = candidateClientContext;

            // Step 5: Update mirror matcher principals for new certificates
            reloadMirrorMatcherPrincipals();

            logger.log(INFO, "Smart SSL certificate transition completed successfully");

            // Step 6: Schedule backup cleanup after transition timeout
            scheduleBackupCleanup();

            // Step 7: Check if there are new pending changes while we were processing
            synchronized (this) {
                if (!pendingReloads.isEmpty()) {
                    logger.log(INFO, "Additional certificate changes detected during transition, scheduling another round");
                    // Reset transition state and trigger another round
                    inCertificateTransition = false;
                    smartCertificateTransition();
                    return;
                }
                inCertificateTransition = false;
            }

        } catch (Exception e) {
            logger.log(WARNING, "Smart certificate transition failed, maintaining current certificates: " + e.getMessage());
            rollbackCertificateTransition();
        }
    }

    /**
     * Lightweight SSL context validation to ensure new certificates are valid
     * before committing to the transition.
     */
    private void validateSSLContext(SSLContext context, String mode) throws Exception {
        if (context == null) {
            throw new IllegalStateException("SSL context is null for mode: " + mode);
        }

        // Create a test SSL engine to validate the context
        javax.net.ssl.SSLEngine testEngine = context.createSSLEngine();
        if (testEngine == null) {
            throw new IllegalStateException("Failed to create SSL engine for mode: " + mode);
        }

        // Validate that we can retrieve supported cipher suites
        String[] cipherSuites = testEngine.getSupportedCipherSuites();
        if (cipherSuites == null || cipherSuites.length == 0) {
            throw new IllegalStateException("No cipher suites available for mode: " + mode);
        }

        logger.log(FINE, "SSL context validation passed for " + mode + " mode with " +
                   cipherSuites.length + " cipher suites");
    }

    /**
     * Rollback to backup SSL contexts if transition fails.
     */
    private void rollbackCertificateTransition() {
        synchronized (this) {
            if (backupServerSSLContext != null && backupClientSSLContext != null) {
                logger.log(INFO, "Rolling back to previous SSL certificates");
                this.serverSSLContext = backupServerSSLContext;
                this.clientSSLContext = backupClientSSLContext;
            }

            // Clean up backup contexts and pending changes
            backupServerSSLContext = null;
            backupClientSSLContext = null;
            pendingReloads.clear();
            inCertificateTransition = false;
        }
    }

    /**
     * Schedule cleanup of backup contexts after transition timeout.
     * This prevents memory leaks from holding old SSL contexts indefinitely.
     */
    private void scheduleBackupCleanup() {
        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) instanceParams.getContext().getRepNetConfig();
        final long timeoutMs = config.getSSLCertTransitionTimeoutSeconds() * 1000;

        // Use a simple approach with a daemon thread for cleanup
        Thread cleanupThread = new Thread(() -> {
            try {
                Thread.sleep(timeoutMs);
                synchronized (this) {
                    if (inCertificateTransition &&
                        (System.currentTimeMillis() - transitionStartTime) >= timeoutMs) {
                        logger.log(FINE, "Cleaning up backup SSL contexts after transition timeout");
                        backupServerSSLContext = null;
                        backupClientSSLContext = null;
                        inCertificateTransition = false;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "SSLCertificateCleanup");

        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    /**
     * Reload principals for mirror authenticators and verifiers.
     * Note: DN-based authenticators/verifiers don't need explicit reloading
     * as they re-evaluate peer certificates on each SSL handshake.
     */
    private void reloadMirrorMatcherPrincipals() {
        try {
            // Reload authenticator principal if it's a mirror authenticator
            if (sslAuthenticator instanceof SSLMirrorAuthenticator) {
                ((SSLMirrorAuthenticator) sslAuthenticator).reloadPrincipal();
            }

            // Reload host verifier principal if it's a mirror host verifier
            if (sslHostVerifier instanceof SSLMirrorHostVerifier) {
                ((SSLMirrorHostVerifier) sslHostVerifier).reloadPrincipal();
            }

            // DN-based authenticators and verifiers automatically handle
            // certificate changes during SSL handshake validation, no action needed
        } catch (Exception e) {
            logger.log(WARNING, "Failed to reload mirror matcher principals: " + e.getMessage());
        }
    }

    /**
     * Stop the certificate file watcher and clean up resources.
     */
    public void shutdown() {
        if (certificateWatcher != null) {
            certificateWatcher.stop();
            certificateWatcher = null;
        }

        // Clean up any ongoing certificate transition
        synchronized (this) {
            if (inCertificateTransition) {
                logger.log(FINE, "Cleaning up certificate transition during shutdown");
                backupServerSSLContext = null;
                backupClientSSLContext = null;
                pendingReloads.clear();
                inCertificateTransition = false;
            }
        }
    }

    /**
     * Manually trigger smart certificate transition.
     * This can be called programmatically to reload certificates without
     * waiting for file system events.
     *
     * @return true if transition was initiated successfully, false otherwise
     */
    public boolean triggerCertificateTransition() {
        if (instanceParams == null) {
            logger.log(WARNING, "Cannot trigger certificate transition: instance parameters not available");
            return false;
        }

        try {
            logger.log(INFO, "Manually triggered certificate transition");
            // Add a manual trigger entry
            pendingReloads.put("MANUAL_TRIGGER", System.currentTimeMillis());
            smartCertificateTransition();
            return true;
        } catch (Exception e) {
            logger.log(WARNING, "Failed to trigger certificate transition: " + e.getMessage());
            return false;
        }
    }

    /**
     * Check if a certificate transition is currently in progress.
     *
     * @return true if transition is in progress, false otherwise
     */
    public boolean isInCertificateTransition() {
        return inCertificateTransition;
    }

    /**
     * Diagnose current SSL certificate configuration and status.
     * This method provides detailed information about certificate files,
     * monitoring status, and current certificate fingerprints.
     */
    public void diagnoseCertificateConfiguration() {
        if (instanceParams == null) {
            logger.log(WARNING, "Cannot diagnose: instance parameters not available");
            return;
        }

        try {
            final ReplicationSSLConfig config =
                (ReplicationSSLConfig) instanceParams.getContext().getRepNetConfig();

            SSLCertificateDiagnostics diagnostics = new SSLCertificateDiagnostics(logger, config);
            diagnostics.diagnose();

            // Additional status information
            logger.log(INFO, "Certificate watcher status: " +
                      (certificateWatcher != null ? "ACTIVE" : "INACTIVE"));
            logger.log(INFO, "In certificate transition: " + inCertificateTransition);

            if (inCertificateTransition) {
                long elapsed = System.currentTimeMillis() - transitionStartTime;
                logger.log(INFO, "Transition elapsed time: " + elapsed + " ms");
            }

        } catch (Exception e) {
            logger.log(WARNING, "Failed to run certificate diagnostics: " + e.getMessage());
        }
    }

    /**
     * Force certificate reload immediately, bypassing file monitoring.
     * This method is useful for troubleshooting or when automatic monitoring
     * is not working properly.
     *
     * @return true if reload was successful, false otherwise
     */
    public boolean forceReloadCertificates() {
        logger.log(INFO, "Force reloading SSL certificates (bypassing file monitoring)");
        return triggerCertificateTransition();
    }

    /**
     * Builds an SSLContext object for the specified access mode.
     * @param params general instantiation information
     * @param clientMode set to true if the SSLContext is being created for
     * the client side of an SSL connection and false otherwise
     */
    private static SSLContext constructSSLContext(
        InstanceParams params, boolean clientMode) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) params.getContext().getRepNetConfig();

        KeyManager[] kmList = null;
        final KeyStoreInfo ksInfo = readKeyStoreInfo(params.getContext());

        if (ksInfo != null) {
            try {

                /*
                 * Determine whether a specific key is supposed to be used
                 */
                String ksAliasProp = clientMode ?
                    config.getSSLClientKeyAlias() :
                    config.getSSLServerKeyAlias();
                if (ksAliasProp != null && ksAliasProp.isEmpty()) {
                    ksAliasProp = null;
                }

                kmList = buildKeyManagerList(ksInfo, ksAliasProp, clientMode);
            } finally {
                ksInfo.clearPassword();
            }
        }

        TrustManager[] tmList = null;
        final KeyStoreInfo tsInfo = readTrustStoreInfo(params.getContext());
        if (tsInfo != null) {
            try {
                tmList = buildTrustManagerList(tsInfo);
            } finally {
                tsInfo.clearPassword();
            }
        }

        /*
         * Get an SSLContext object
         */
        SSLContext newContext = null;
        try {
            newContext = SSLContext.getInstance(SSL_CONTEXT_PROTOCOL);
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalStateException(
                "Unable to find a suitable SSLContext", nsae);
        }

        /*
         * Put it all together into the SSLContext object
         */
        try {
            newContext.init(kmList, tmList, null);
        } catch (KeyManagementException kme) {
            throw new IllegalStateException(
                "Error establishing SSLContext", kme);
        }

        return newContext;
    }

    /**
     * Builds a list of KeyManagers for incorporation into an SSLContext.
     *
     * @param ksInfo a KeyStoreInfo referencing the Keystore for which the
     * the key manager list is to be built.
     * @param ksAlias an optional KeyStore alias.  If set, the key manager
     * for X509 certs will always select the certificate with the specified
     * alias.
     * @param clientMode set to true if this is for the client side of
     * an SSL connection and fals otherwise.
     */
    private static KeyManager[] buildKeyManagerList(KeyStoreInfo ksInfo,
                                                    String ksAlias,
                                                    boolean clientMode) {

        /*
         * Get a KeyManagerFactory
         */
        final KeyManagerFactory kmf;
        try {
            kmf = KeyManagerFactory.getInstance(X509_ALGO_NAME);
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalStateException(
                "Unable to find a suitable KeyManagerFactory", nsae);
        }

        /*
         * Initialize the key manager factory
         */
        try {
            kmf.init(ksInfo.ks, ksInfo.ksPwd);
        } catch (KeyStoreException kse) {
            throw new IllegalStateException(
                "Error processing keystore file " + ksInfo.ksFile,
                kse);
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalStateException(
                "Unable to find appropriate algorithm for " +
                "keystore file " + ksInfo.ksFile, nsae);
        } catch (UnrecoverableKeyException uke) {
            throw new IllegalStateException(
                "Unable to recover key from keystore file " +
                ksInfo.ksFile, uke);
        }

        /*
         * Get the list of key managers used
         */
        KeyManager[] kmList = kmf.getKeyManagers();

        /*
         * If an alias was specified, we need to construct an
         * AliasKeyManager, which will delegate to the correct
         * underlying KeyManager, which we need to locate.
         */
        if (ksAlias != null) {

            /*
             * Locate the first appropriate keymanager in the list
             */
            X509ExtendedKeyManager x509KeyManager = null;
            for (KeyManager km : kmList) {
                if (km instanceof X509ExtendedKeyManager) {
                    x509KeyManager = (X509ExtendedKeyManager) km;
                    break;
                }
            }

            if (x509KeyManager == null) {
                throw new IllegalStateException(
                    "Unable to locate an X509ExtendedKeyManager " +
                    "corresponding to keyStore " + ksInfo.ksFile);
            }

            kmList = new KeyManager[] {
                (clientMode ?
                 new AliasKeyManager(x509KeyManager, null, ksAlias) :
                 new AliasKeyManager(x509KeyManager, ksAlias, null)) };
        }

        return kmList;
    }

    /**
     * Reads a KeyStore into memory based on the config.
     */
    private static KeyStoreInfo readKeyStoreInfo(InstanceContext context) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) context.getRepNetConfig();

        /*
         * Determine what KeyStore file to access
         */
        String ksProp = config.getSSLKeyStore();
        if (ksProp == null || ksProp.isEmpty()) {
            ksProp = System.getProperty("javax.net.ssl.keyStore");
        }

        if (ksProp == null) {
            return null;
        }

        /*
         * Determine what type of keystore to assume.  If not specified
         * loadStore determines the default
         */
        final String ksTypeProp = config.getSSLKeyStoreType();

        final char[] ksPw = getKeyStorePassword(context);
        try {
            if (ksPw == null) {
                throw new IllegalArgumentException(
                    "Unable to open keystore without a password");
            }

            /*
             * Get a KeyStore instance
             */
            final KeyStore ks = loadStore(ksProp, ksPw, "keystore", ksTypeProp);

            return new KeyStoreInfo(ksProp, ks, ksPw);
        } finally {
            if (ksPw != null) {
                Arrays.fill(ksPw, ' ');
            }
        }
    }

    /**
     * Finds the keystore password based on the input config.
     */
    private static char[] getKeyStorePassword(InstanceContext context) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) context.getRepNetConfig();

        char[] ksPw = null;

        /*
         * Determine the password for the keystore file
         * Try first using a password source, either explicit or
         * constructed.
         */
        PasswordSource ksPwSource = config.getSSLKeyStorePasswordSource();
        if (ksPwSource == null) {
            ksPwSource =
                constructKSPasswordSource(new InstanceParams(context, null));
        }

        if (ksPwSource != null) {
            ksPw = ksPwSource.getPassword();
        } else {
            /* Next look for an explicit password setting */
            String ksPwProp = config.getSSLKeyStorePassword();
            if (ksPwProp == null || ksPwProp.isEmpty()) {

                /*
                 * Finally, consider the standard Java Keystore
                 * password system property
                 */
                ksPwProp =
                    System.getProperty("javax.net.ssl.keyStorePassword");
            }
            if (ksPwProp != null) {
                ksPw = ksPwProp.toCharArray();
            }
        }

        return ksPw;
    }

    /**
     * Builds a TrustManager list for the input Truststore for use in creating
     * an SSLContext.
     */
    private static TrustManager[] buildTrustManagerList(KeyStoreInfo tsInfo) {

        final TrustManagerFactory tmf;
        try {
            tmf = TrustManagerFactory.getInstance(X509_ALGO_NAME);
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalStateException(
                "Unable to find a suitable TrustManagerFactory", nsae);
        }

        try {
            tmf.init(tsInfo.ks);
        } catch (KeyStoreException kse) {
            throw new IllegalStateException(
                "Error initializing truststore " + tsInfo.ksFile, kse);
        }

        return tmf.getTrustManagers();
    }

    /**
     * Finds the truststore password based on the input config.
     */
    private static char[] getTrustStorePassword(InstanceContext context) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) context.getRepNetConfig();

        char[] ksPw = null;

        String ksPwProp = config.getSSLTrustStorePassword();
        if (ksPwProp == null || ksPwProp.isEmpty()) {
            /*
             * Finally, consider the standard Java Keystore
             * password system property
             */
            ksPwProp =
                System.getProperty("javax.net.ssl.trustStorePassword");
        }
        if (ksPwProp != null) {
            ksPw = ksPwProp.toCharArray();
        }

        return ksPw;
    }

    /**
     * Based on the input config, read the configured TrustStore into memory.
     */
    private static KeyStoreInfo readTrustStoreInfo(InstanceContext context) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) context.getRepNetConfig();

        /*
         * Determine what truststore file, if any, to use
         */
        String tsProp = config.getSSLTrustStore();
        if (tsProp == null || tsProp.isEmpty()) {
            tsProp = System.getProperty("javax.net.ssl.trustStore");
        }

        /*
         * Determine what type of truststore to assume
         */
        String tsTypeProp = config.getSSLTrustStoreType();
        if (tsTypeProp == null || tsTypeProp.isEmpty()) {
            tsTypeProp = KeyStore.getDefaultType();
        }

        /*
         * Build a TrustStore, if specified
         */
        final char[] tsPw = getTrustStorePassword(context);

        if (tsProp != null) {
            final KeyStore ts = loadStore(tsProp, tsPw, "truststore", tsTypeProp);

            return new KeyStoreInfo(tsProp, ts, tsPw);
        }

        return null;
    }

    /**
     * Create an SSLParameters base on the input configuration.
     */
    private static SSLParameters constructSSLParameters(
        InstanceParams params) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) params.getContext().getRepNetConfig();

        /*
         * Determine cipher suites configuration
         */
        String cipherSuitesProp = config.getSSLCipherSuites();
        String[] cipherSuites = null;
        if (cipherSuitesProp != null && !cipherSuitesProp.isEmpty()) {
            cipherSuites = cipherSuitesProp.split(",");
        }

        /*
         * Determine protocols configuration
         */
        String protocolsProp = config.getSSLProtocols();
        String[] protocols = null;
        if (protocolsProp != null && !protocolsProp.isEmpty()) {
            protocols = protocolsProp.split(",");
        }

        return new SSLParameters(cipherSuites, protocols);
    }

    /**
     * Filter SSLParameter configuration to respect the supported
     * configuration capabilities of the context.
     */
    private static SSLParameters filterSSLParameters(
        SSLParameters configParams, SSLContext filterContext)
        throws IllegalArgumentException {

        SSLParameters suppParams = filterContext.getSupportedSSLParameters();

        /* Filter the cipher suite selection */
        String[] configCipherSuites = configParams.getCipherSuites();
        if (configCipherSuites != null) {
            final String[] suppCipherSuites = suppParams.getCipherSuites();
            configCipherSuites =
                filterConfig(configCipherSuites, suppCipherSuites);
            if (configCipherSuites.length == 0) {
                throw new IllegalArgumentException(
                    "None of the configured SSL cipher suites are supported " +
                    "by the environment.");
            }
        }

        /* Filter the protocol selection */
        String[] configProtocols =
            configParams.getProtocols();
        if (configProtocols != null) {
            final String[] suppProtocols = suppParams.getProtocols();
            configProtocols = filterConfig(configProtocols, suppProtocols);
            if (configProtocols.length == 0) {
                throw new IllegalArgumentException(
                    "None of the configured SSL protocols are supported " +
                    "by the environment.");
            }
        }

        final SSLParameters newParams =
            new SSLParameters(configCipherSuites, configProtocols);
        newParams.setWantClientAuth(configParams.getWantClientAuth());
        newParams.setNeedClientAuth(configParams.getNeedClientAuth());
        return newParams;
    }

    /**
     * Return the intersection of configChoices and supported
     */
    private static String[] filterConfig(String[] configChoices,
                                         String[] supported) {

        ArrayList<String> keep = new ArrayList<>();
        for (String choice : configChoices) {
            for (String supp : supported) {
                if (choice.equals(supp)) {
                    keep.add(choice);
                    break;
                }
            }
        }
        return keep.toArray(new String[keep.size()]);
    }

    /**
     * Build an SSLAuthenticator or HostnameVerifier based on property
     * configuration. This method looks up a class of the specified name,
     * then finds a constructor that has a single argument of type
     * InstanceParams and constructs an instance with that constructor, then
     * validates that the instance extends or implements the mustImplement
     * class specified.
     *
     * @param params the parameters for constructing this factory.
     * @param checkerClassName the name of the class to instantiate
     * @param checkerClassParams the value of the configured String params
     *                           argument
     * @param mustImplement a class denoting a required base class or
     *   required implemented interface of the class whose name is
     *   specified by checkerClassName.
     * @param miDesc a descriptive term for the class to be instantiated
     * @return an instance of the specified class
     */
    private static Object constructSSLChecker(
        InstanceParams params,
        String checkerClassName,
        String checkerClassParams,
        Class<?> mustImplement,
        String miDesc) {

        InstanceParams objParams =
            new InstanceParams(params.getContext(), checkerClassParams);

        return DataChannelFactoryBuilder.constructObject(
            checkerClassName, mustImplement, miDesc,
            /* class(InstanceParams) */
            new CtorArgSpec(
                new Class<?>[] { InstanceParams.class },
                new Object[]   { objParams }));
    }

    /**
     * Builds an SSLAuthenticator based on the input configuration referenced
     * by params.
     */
    private static SSLAuthenticator constructSSLAuthenticator(
        InstanceParams params)
        throws IllegalArgumentException {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) params.getContext().getRepNetConfig();

        final String authSpec = config.getSSLAuthenticator();
        final String authClassName = config.getSSLAuthenticatorClass();

        /* check for conflicts */
        if (authSpec != null && !authSpec.equals("") &&
            authClassName != null && !authClassName.equals("")) {

            throw new IllegalArgumentException(
                "Cannot specify both authenticator and authenticatorClass");
        }

        if (authSpec != null && !authSpec.equals("")) {
            /* construct an authenticator of a known type */
            return constructStdAuthenticator(params, authSpec);
        }

        if (authClassName == null || authClassName.equals("")) {
            return null;
        }

        /* construct an authenticator using the specified class */
        final String authParams = config.getSSLAuthenticatorParams();

        return (SSLAuthenticator) constructSSLChecker(
            params, authClassName, authParams, SSLAuthenticator.class,
            "authenticator");
    }

    /**
     * Builds an SSLAuthenticator of a known type.
     */
    private static SSLAuthenticator constructStdAuthenticator(
        InstanceParams params, String authSpec)
        throws IllegalArgumentException {

        authSpec = authSpec.trim();
        if (authSpec.startsWith("dnmatch(") && authSpec.endsWith(")")) {
            /* a DN matching authenticator */
            final String match =
                authSpec.substring("dnmatch(".length(),
                                        authSpec.length()-1);
            return new SSLDNAuthenticator(
                new InstanceParams(params.getContext(), match));
        } else if (authSpec.equals("mirror")) {
            /* a mirroring  authenticator */
            return new SSLMirrorAuthenticator(
                new InstanceParams(params.getContext(), null));
        }

        throw new IllegalArgumentException(
            authSpec  + " is not a valid authenticator specification.");
    }

    /**
     * Builds an HostnameVerifier based on the configuration referenced in
     * params.
     */
    private static HostnameVerifier constructSSLHostVerifier(
        InstanceParams params)
        throws IllegalArgumentException {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) params.getContext().getRepNetConfig();
        final String hvSpec = config.getSSLHostVerifier();
        final String hvClassName = config.getSSLHostVerifierClass();

        /* Check for conflicts */
        if (hvSpec != null && !hvSpec.equals("") &&
            hvClassName != null && !hvClassName.equals("")) {

            throw new IllegalArgumentException(
                "Cannot specify both hostVerifier and hostVerifierClass");
        }

        if (hvSpec != null && !hvSpec.equals("")) {
            /* construct a host verifier of a known type */
            return constructStdHostVerifier(params, hvSpec);
        }

        if (hvClassName == null || hvClassName.equals("")) {
            return null;
        }

        /* construct a host verifier using the specified class */
        final String hvParams = config.getSSLHostVerifierParams();

        return (HostnameVerifier) constructSSLChecker(
            params, hvClassName, hvParams, HostnameVerifier.class,
            "hostname verifier");
    }

    /**
     * Builds a HostnameVerifier of a known type.
     */
    private static HostnameVerifier constructStdHostVerifier(
        InstanceParams params, String hvSpec)
        throws IllegalArgumentException {

        hvSpec = hvSpec.trim();
        if (hvSpec.startsWith("dnmatch(") && hvSpec.endsWith(")")) {
            /* a DN matching host verifier */
            final String match = hvSpec.substring("dnmatch(".length(),
                                                       hvSpec.length()-1);
            return new SSLDNHostVerifier(
                new InstanceParams(params.getContext(), match));

        } else if (hvSpec.equals("mirror")) {
            /* a mirroring  host verifier */
            return new SSLMirrorHostVerifier(
                new InstanceParams(params.getContext(), null));

        } else if (hvSpec.equals("hostname")) {
            /* a standard  hostname verifier */
            return new SSLStdHostVerifier(
                new InstanceParams(params.getContext(), null));
        }

        throw new IllegalArgumentException(
            hvSpec  + " is not a valid hostVerifier specification.");
    }

    /**
     * Builds a PasswordSource instance via generic instantiation.
     *
     * @param params the parameters driving the instantiation
     * @param pwdSrcClassName the name of the class to instantiate
     * @param pwSrcParams a possibly null String that has been configured as
     * an argument to the class's constructor.
     * @return the new instance
     */
    private static PasswordSource constructPasswordSource(
        InstanceParams params, String pwdSrcClassName, String pwSrcParams) {

        final InstanceParams objParams =
            new InstanceParams(params.getContext(), pwSrcParams);

        return (PasswordSource)
            DataChannelFactoryBuilder.constructObject(
                pwdSrcClassName, PasswordSource.class, "password source",
                /* class(InstanceParams) */
                new CtorArgSpec(
                    new Class<?>[] { InstanceParams.class },
                    new Object[]   { objParams }));
    }

    /**
     * Build a PasswordSource for the keystore based on the configuration
     * referenced by params.
     */
    private static PasswordSource constructKSPasswordSource(
        InstanceParams params) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) params.getContext().getRepNetConfig();

        final String pwSrcClassName =
            config.getSSLKeyStorePasswordClass();

        if (pwSrcClassName == null || pwSrcClassName.equals("")) {
            return null;
        }

        final String pwSrcParams =
            config.getSSLKeyStorePasswordParams();

        return constructPasswordSource(params, pwSrcClassName, pwSrcParams);
    }

    /**
     * Load a keystore/truststore file into memory
     * @param storeName the name of the store file
     * @param storeFlavor a descriptive name of store type
     * @param storeType JKS, etc
     * @throws IllegalArgumentException if the specified parameters
     * do now allow a store to be successfully loaded
     */
    private static KeyStore loadStore(String storeName,
                                      char[] storePassword,
                                      String storeFlavor,
                                      String storeType)
        throws IllegalArgumentException {

        if (storeType == null || storeType.isEmpty()) {
            storeType = KeyStore.getDefaultType();
        }

        final KeyStore ks;
        try {
            ks = KeyStore.getInstance(storeType);
        } catch (KeyStoreException kse) {
            throw new IllegalArgumentException(
                "Unable to find a " + storeFlavor + " instance of type " +
                storeType,
                kse);
        }

        final FileInputStream fis;
        try {
            fis = new FileInputStream(storeName);
        } catch (FileNotFoundException fnfe) {
            throw new IllegalArgumentException(
                "Unable to locate specified " + storeFlavor +
                " " + storeName, fnfe);
        }

        try {
            ks.load(fis, storePassword);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Error reading from " + storeFlavor + " file " + storeName,
                ioe);
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalArgumentException(
                "Unable to check " + storeFlavor + " integrity: " + storeName,
                nsae);
        } catch (CertificateException ce) {
            throw new IllegalArgumentException(
                "Not all certificates could be loaded: " + storeName,
                ce);
        } finally {
            try {
                fis.close();
            } catch (IOException ioe) {
                /* ignored */
            }
        }

        return ks;
    }

    /**
     * Gets a proper algorithm name for the X.509 certificate key manager. If
     * users already specify it via setting the system property of
     * "je.ssl.x509AlgoName", use it directly. Otherwise, for IBM J9 VM, the
     * name is "IbmX509". For Hotspot and other JVMs, the name of "SunX509"
     * will be used.
     *
     * @return algorithm name for X509 certificate manager
     */
    private static String getX509AlgoName() {
        final String x509Name = System.getProperty(X509_ALGO_NAME_PROPERTY);
        if (x509Name != null && !x509Name.isEmpty()) {
            return x509Name;
        }
        final String jvmVendor = System.getProperty("java.vendor");
        if (jvmVendor.startsWith("IBM")) {
            return "IbmX509";
        }
        return "SunX509";
    }

    /**
     * Internal class for communicating a pair of KeyStore and password
     */
    private static class KeyStoreInfo {
        private final String ksFile;
        private final KeyStore ks;
        private final char[] ksPwd;

        private KeyStoreInfo(String ksFile, KeyStore ks, char[] ksPwd) {
            this.ksFile = ksFile;
            this.ks = ks;
            this.ksPwd =
                (ksPwd == null) ? null : Arrays.copyOf(ksPwd, ksPwd.length);
        }

        private void clearPassword() {
            if (ksPwd != null) {
                Arrays.fill(ksPwd, ' ');
            }
        }
    }
}
