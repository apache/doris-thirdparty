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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.KeyFactory;
import java.security.AlgorithmParameters;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Level.FINE;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.SecretKey;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;
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
     * Scheduled executor service for periodic certificate checking
     */
    private ScheduledExecutorService certificateCheckExecutor;

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

        // Initialize certificate monitoring
        initializeCertificateMonitoring();
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
     * Initialize certificate monitoring using scheduled executor for periodic checks.
     */
    private void initializeCertificateMonitoring() {
        if (instanceParams == null) {
            return;
        }

        try {
            final ReplicationSSLConfig config =
                (ReplicationSSLConfig) instanceParams.getContext().getRepNetConfig();

            // Get the refresh interval from configuration
            long refreshInterval = config.getSSLCertRefreshIntervalSeconds();

            // If refresh interval is 0, disable certificate monitoring
            if (refreshInterval <= 0) {
                logger.log(INFO, "Certificate monitoring disabled (refresh interval = 0)");
                return;
            }

            // Only monitor PEM configuration
            final String pemCert = config.getSSLPemCertFile();
            final String pemKey = config.getSSLPemKeyFile();
            final String pemCa = config.getSSLPemCaCertFile();

            // Create scheduled executor for periodic certificate checking
            certificateCheckExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "BDB-SSL-Certificate-Monitor");
                t.setDaemon(true);
                return t;
            });

            // Schedule periodic certificate checks
            certificateCheckExecutor.scheduleAtFixedRate(
                this::checkAndReloadCertificates,
                refreshInterval,
                refreshInterval,
                TimeUnit.SECONDS
            );

            // Initialize file modification times
            initializeFileModificationTimes(config);

            logger.log(INFO, "Certificate monitoring initialized with " + refreshInterval + " second interval");

        } catch (Exception e) {
            logger.log(WARNING, "Failed to initialize certificate monitoring: " + e.getMessage());
        }
    }

    /**
     * Initialize file modification times on startup.
     */
    private void initializeFileModificationTimes(ReplicationSSLConfig config) {
        try {
            caLastModified = Files.getLastModifiedTime(Paths.get(config.getSSLPemCaCertFile())).toMillis();
            keyLastModified = Files.getLastModifiedTime(Paths.get(config.getSSLPemKeyFile())).toMillis();
            certLastModified = Files.getLastModifiedTime(Paths.get(config.getSSLPemCertFile())).toMillis();
            logger.log(INFO, "Certificate timestamps initialized");
        } catch (Exception e) {
            logger.log(WARNING, "Failed to initialize file modification times: " + e.getMessage());
        }
    }

    /**
     * Check and reload certificates if any PEM files have changed.
     * Uses bit operations to determine reload necessity:
     * - If CA changes: check_num |= 1 << 2 (bit 2)
     * - If key changes: check_num |= 1 << 1 (bit 1)
     * - If cert changes: check_num |= 1 (bit 0)
     *
     * Valid reload scenarios:
     * - CA + cert + key changed: check_num = 7 (111)
     * - CA + cert changed: check_num = 5 (101)
     * - Key + cert changed: check_num = 3 (011)
     * - Only cert changed: check_num = 1 (001)
     *
     * Rule: Reload when check_num is odd (cert file changed)
     * Only update modification times after successful reload.
     */
    private void checkAndReloadCertificates() {
        if (instanceParams == null) {
            return;
        }

        try {
            final ReplicationSSLConfig config =
                (ReplicationSSLConfig) instanceParams.getContext().getRepNetConfig();

            // Use AtomicReference to hold temporary modification times
            AtomicReference<Long> caModified = new AtomicReference<>(caLastModified);
            AtomicReference<Long> keyModified = new AtomicReference<>(keyLastModified);
            AtomicReference<Long> certModified = new AtomicReference<>(certLastModified);

            int check_num = 0;

            // Check CA certificate file
            if (checkCertificateFile(config.getSSLPemCaCertFile(), caModified.get(),
                    caModified::set, "CA certificate")) {
                check_num |= 1 << 2;
            }

            // Check private key file
            if (checkCertificateFile(config.getSSLPemKeyFile(), keyModified.get(),
                    keyModified::set, "private key")) {
                check_num |= 1 << 1;
            }

            // Check certificate file
            if (checkCertificateFile(config.getSSLPemCertFile(), certModified.get(),
                    certModified::set, "certificate")) {
                check_num |= 1;
            }

            // Reload certificates when check_num is odd (certificate file changed)
            if ((check_num & 1) == 1) {
                // Try to reload certificates
                if (reloadSSLContexts()) {
                    // Only update modification times after successful reload
                    caLastModified = caModified.get();
                    keyLastModified = keyModified.get();
                    certLastModified = certModified.get();
                    logger.log(INFO, "Certificate modification times updated after successful reload");
                } else {
                    logger.log(WARNING, "Certificate reload failed, keeping previous modification times for retry");
                }
            }

        } catch (Exception e) {
            logger.log(WARNING, "Error checking certificate files: " + e.getMessage());
        }
    }

    /**
     * File modification time tracking for change detection.
     * Initialize to -1 to distinguish from 0 (which could be a valid timestamp)
     */
    private volatile long caLastModified = -1;
    private volatile long keyLastModified = -1;
    private volatile long certLastModified = -1;

    /**
     * Check if a certificate file has been modified and update the stored modification time.
     */
    private boolean checkCertificateFile(String filePath, long lastModified,
                                       java.util.function.Consumer<Long> timeUpdater,
                                       String fileType) {
        if (filePath == null || filePath.isEmpty()) {
            return false;
        }

        try {
            java.io.File file = new java.io.File(filePath);

            // Check if file was deleted
            if (!file.exists()) {
                logger.log(WARNING, fileType + " file was deleted: " + filePath +
                          ", waiting for recreation...");
                if (waitForFileRecreation(file, fileType, filePath, timeUpdater)) {
                    return true; // File was recreated, trigger reload
                } else {
                    return false; // File not recreated, skip reload
                }
            }

            long currentModTime = Files.getLastModifiedTime(Paths.get(filePath)).toMillis();

            if (lastModified == -1 || currentModTime != lastModified) {
                timeUpdater.accept(currentModTime);
                if (lastModified != -1) { // Don't trigger reload on first check
                    logger.log(INFO, fileType + " file changed: " + filePath +
                              " (old: " + lastModified + ", new: " + currentModTime + ")");
                    return true;
                } else {
                    logger.log(INFO, fileType + " file initialized: " + filePath +
                              " (timestamp: " + currentModTime + ")");
                }
            }
            return false;
        } catch (Exception e) {
            logger.log(WARNING, "Failed to check " + fileType + " file modification time for " +
                      filePath + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Wait for file recreation when a certificate file is deleted.
     * This handles the case where certificates are updated by deleting the old file
     * and creating a new one.
     */
    private boolean waitForFileRecreation(java.io.File file, String fileType, String filePath,
                                        java.util.function.Consumer<Long> updateTime) {
        int waitSeconds = 30;
        int checkInterval = 1;

        for (int i = 0; i < waitSeconds; i += checkInterval) {
            try {
                Thread.sleep(checkInterval * 1000);
                if (file.exists()) {
                    long newModified = Files.getLastModifiedTime(file.toPath()).toMillis();
                    updateTime.accept(newModified);
                    logger.log(INFO, fileType + " file recreated and detected: " + filePath);
                    return true;
                }
            } catch (Exception e) {
                logger.log(WARNING, "Error while waiting for " + fileType + " file recreation: " +
                          filePath + ", " + e.getMessage());
            }
        }

        logger.log(WARNING, fileType + " file was deleted and not recreated within " + waitSeconds +
                  " seconds, certificate reload cancelled: " + filePath);
        return false;
    }

    /**
     * Reload SSL contexts using only PEM configuration.
     * @return true if reload was successful, false otherwise
     */
    private boolean reloadSSLContexts() {
        try {
            logger.log(INFO, "Reloading SSL contexts with new PEM certificates");

            // Construct new SSL contexts
            SSLContext newServerContext = constructSSLContext(instanceParams, false);
            SSLContext newClientContext = constructSSLContext(instanceParams, true);

            // Validate new contexts
            if (newServerContext != null && newClientContext != null) {
                // Atomically switch to new contexts
                this.serverSSLContext = newServerContext;
                this.clientSSLContext = newClientContext;

                // Update mirror matcher principals for new certificates
                reloadMirrorMatcherPrincipals();

                logger.log(INFO, "SSL certificate reload completed successfully");
                return true;
            } else {
                logger.log(WARNING, "Failed to create new SSL contexts during reload");
                return false;
            }

        } catch (Exception e) {
            logger.log(WARNING, "Failed to reload SSL certificates: " + e.getMessage());
            return false;
        }
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
     * Stop the certificate monitoring executor and clean up resources.
     */
    public void shutdown() {
        if (certificateCheckExecutor != null) {
            certificateCheckExecutor.shutdown();
            try {
                if (!certificateCheckExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    certificateCheckExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                certificateCheckExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            certificateCheckExecutor = null;
        }
    }

    /**
     * Manually trigger certificate reload.
     * This can be called programmatically to reload certificates without
     * waiting for periodic checks.
     *
     * @return true if reload was initiated successfully, false otherwise
     */
    public boolean triggerCertificateReload() {
        if (instanceParams == null) {
            logger.log(WARNING, "Cannot trigger certificate reload: instance parameters not available");
            return false;
        }

        try {
            logger.log(INFO, "Manually triggered certificate reload");
            reloadSSLContexts();
            return true;
        } catch (Exception e) {
            logger.log(WARNING, "Failed to trigger certificate reload: " + e.getMessage());
            return false;
        }
    }

    /**
     * Builds an SSLContext object using only PEM configuration.
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
     * Reads a KeyStore into memory based on PEM configuration only.
     */
    private static KeyStoreInfo readKeyStoreInfo(InstanceContext context) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) context.getRepNetConfig();

        /*
         * Determine what KeyStore file to access. P12 configuration takes
         * precedence over PEM.
         */
        String ksProp = config.getSSLKeyStore();
        if (ksProp == null || ksProp.isEmpty()) {
            ksProp = System.getProperty("javax.net.ssl.keyStore");
        }

        if (ksProp != null && !ksProp.isEmpty()) {

            /*
             * Determine what type of keystore to assume. If not specified
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
                final KeyStore ks =
                    loadStore(ksProp, ksPw, "keystore", ksTypeProp);

                return new KeyStoreInfo(ksProp, ks, ksPw);
            } finally {
                if (ksPw != null) {
                    Arrays.fill(ksPw, ' ');
                }
            }
        }

        /*
         * No keystore file configured, fall back to PEM material if provided.
         */
        final String pemCert = config.getSSLPemCertFile();
        final String pemKey = config.getSSLPemKeyFile();
        if (pemCert == null || pemCert.isEmpty() ||
            pemKey == null || pemKey.isEmpty()) {
            return null;
        }

        final char[] pemPassword = pemPasswordToChars(config.getSSLPemKeyPassword());
        try {
            final KeyStore ks = loadKeyStore(pemCert, pemKey, pemPassword);
            return new KeyStoreInfo(pemCert, ks, pemPassword);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Error loading PEM key material from " + pemCert, e);
        } finally {
            if (pemPassword != null) {
                Arrays.fill(pemPassword, ' ');
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
     * Based on PEM configuration, read the configured TrustStore into memory.
     */
    private static KeyStoreInfo readTrustStoreInfo(InstanceContext context) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) context.getRepNetConfig();

        /*
         * Only use PEM CA configuration for truststore.
         */
        final String pemCa = config.getSSLPemCaCertFile();
        if (pemCa == null || pemCa.isEmpty()) {
            return null;
        }

        try {
            final KeyStore ts = loadTrustStore(pemCa);
            return new KeyStoreInfo(pemCa, ts, null);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Error loading PEM CA certificate from " + pemCa, e);
        }
    }

    private static char[] pemPasswordToChars(String password) {

        if (password == null || password.isEmpty()) {
            return new char[0];
        }
        return password.toCharArray();
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

    public static KeyStore loadKeyStore(String certPath,
                                        String keyPath,
                                        char[] password)
        throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate cert;
        try (InputStream in = new FileInputStream(certPath)) {
            cert = cf.generateCertificate(in);
        }

        PrivateKey privateKey = loadPrivateKey(keyPath, password);

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("cert", privateKey, password, new Certificate[]{cert});
        return ks;
    }

    public static KeyStore loadTrustStore(String caCertPath) throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate ca;
        try (InputStream in = new FileInputStream(caCertPath)) {
            ca = cf.generateCertificate(in);
        }
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("ca", ca);
        return ts;
    }


    public static PrivateKey loadPrivateKey(String keyPath, char[] password)
        throws Exception {
        if (password == null) {
            password = new char[0];
        }
        String pem = new String(Files.readAllBytes(Paths.get(keyPath)));
        pem = pem.replaceAll("-----BEGIN (.*)-----", "")
                 .replaceAll("-----END (.*)-----", "")
                 .replaceAll("\\s", "");
        byte[] decoded = Base64.getDecoder().decode(pem);

        try {
            EncryptedPrivateKeyInfo encryptedInfo =
                new EncryptedPrivateKeyInfo(decoded);
            Cipher cipher = buildDecryptCipher(encryptedInfo, password);
            PKCS8EncodedKeySpec keySpec = encryptedInfo.getKeySpec(cipher);
            return generatePrivateKey(keySpec);
        } catch (IOException e) {
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decoded);
            return generatePrivateKey(keySpec);
        }
    }

    private static PrivateKey generatePrivateKey(PKCS8EncodedKeySpec keySpec)
        throws Exception {
        String[] keyAlgorithms = {"RSA", "EC", "DSA"};
        Exception lastException = null;

        for (String keyAlg : keyAlgorithms) {
            try {
                return KeyFactory.getInstance(keyAlg).generatePrivate(keySpec);
            } catch (Exception e) {
                lastException = e;
            }
        }

        if (lastException != null) {
            throw lastException;
        }

        throw new Exception(
            "Unable to parse private key with any supported algorithm");
    }

    private static Cipher buildDecryptCipher(EncryptedPrivateKeyInfo info,
                                             char[] password)
        throws Exception {
        final String algName = info.getAlgName();
        if (algName != null && algName.toUpperCase().contains("PBES2")) {
            AlgorithmParameters params = info.getAlgParameters();
            if (params == null) {
                throw new IllegalArgumentException(
                    "Missing PBES2 algorithm parameters");
            }

            PBEParameterSpec pbeSpec =
                params.getParameterSpec(PBEParameterSpec.class);
            if (pbeSpec == null) {
                throw new IllegalArgumentException(
                    "Unsupported PBES2 parameters");
            }

            final String transformation = params.toString();
            if (transformation == null || transformation.isEmpty()) {
                throw new IllegalArgumentException(
                    "Missing PBES2 transformation");
            }

            SecretKeyFactory skf = SecretKeyFactory.getInstance(transformation);
            SecretKey secret = skf.generateSecret(new PBEKeySpec(password,
                                                                pbeSpec.getSalt(),
                                                                pbeSpec.getIterationCount()));

            Cipher cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.DECRYPT_MODE, secret, pbeSpec);
            return cipher;
        }

        Cipher cipher = Cipher.getInstance(algName);
        PBEKeySpec pbeKeySpec = new PBEKeySpec(password);
        SecretKeyFactory skf = SecretKeyFactory.getInstance(algName);
        Key key = skf.generateSecret(pbeKeySpec);
        cipher.init(Cipher.DECRYPT_MODE, key, info.getAlgParameters());
        return cipher;
    }

}
