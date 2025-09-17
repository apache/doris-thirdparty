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
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.MessageDigest;
import java.util.Enumeration;

import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.net.InstanceLogger;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

/**
 * Diagnostic utilities for SSL certificate management and troubleshooting.
 */
public class SSLCertificateDiagnostics {

    private final InstanceLogger logger;
    private final ReplicationSSLConfig config;

    public SSLCertificateDiagnostics(InstanceLogger logger, ReplicationSSLConfig config) {
        this.logger = logger;
        this.config = config;
    }

    /**
     * Diagnose certificate configuration and print detailed information.
     */
    public void diagnose() {
        logger.log(INFO, "=== SSL Certificate Configuration Diagnosis ===");

        // Check refresh interval
        long refreshInterval = config.getSSLCertRefreshIntervalSeconds();
        logger.log(INFO, "Certificate refresh interval: " + refreshInterval + " seconds");
        if (refreshInterval == 0) {
            logger.log(WARNING, "Certificate auto-reload is DISABLED (refresh interval = 0)");
        }

        // Check transition timeout
        long transitionTimeout = config.getSSLCertTransitionTimeoutSeconds();
        logger.log(INFO, "Certificate transition timeout: " + transitionTimeout + " seconds");

        // Check keystore configuration
        diagnoseKeystore();

        // Check truststore configuration
        diagnoseTruststore();
    }

    /**
     * Print keystore certificate information and fingerprints.
     */
    private void diagnoseKeystore() {
        logger.log(INFO, "--- Keystore Diagnosis ---");

        String keystorePath = config.getSSLKeyStore();
        if (keystorePath == null || keystorePath.isEmpty()) {
            keystorePath = System.getProperty("javax.net.ssl.keyStore");
        }

        if (keystorePath == null || keystorePath.isEmpty()) {
            logger.log(WARNING, "No keystore path configured");
            return;
        }

        logger.log(INFO, "Keystore path: " + keystorePath);

        File keystoreFile = new File(keystorePath);
        if (!keystoreFile.exists()) {
            logger.log(WARNING, "Keystore file does not exist: " + keystorePath);
            return;
        }

        logger.log(INFO, "Keystore file exists, last modified: " +
                   new java.util.Date(keystoreFile.lastModified()));

        // Try to load and analyze the keystore
        try {
            String keystoreType = config.getSSLKeyStoreType();
            if (keystoreType == null || keystoreType.isEmpty()) {
                keystoreType = KeyStore.getDefaultType();
            }

            KeyStore keystore = KeyStore.getInstance(keystoreType);

            // For diagnostic purposes, we'll try to load with null password first
            // In a real deployment, proper password handling would be needed
            try (FileInputStream fis = new FileInputStream(keystoreFile)) {
                keystore.load(fis, null); // This will fail if password is required
                analyzeKeyStore(keystore, "Keystore");
            } catch (Exception e) {
                logger.log(WARNING, "Could not load keystore for analysis (password required or invalid format): " + e.getMessage());
            }

        } catch (Exception e) {
            logger.log(WARNING, "Failed to analyze keystore: " + e.getMessage());
        }
    }

    /**
     * Print truststore certificate information.
     */
    private void diagnoseTruststore() {
        logger.log(INFO, "--- Truststore Diagnosis ---");

        String truststorePath = config.getSSLTrustStore();
        if (truststorePath == null || truststorePath.isEmpty()) {
            truststorePath = System.getProperty("javax.net.ssl.trustStore");
        }

        if (truststorePath == null || truststorePath.isEmpty()) {
            logger.log(INFO, "No truststore path configured (using default trust manager)");
            return;
        }

        logger.log(INFO, "Truststore path: " + truststorePath);

        File truststoreFile = new File(truststorePath);
        if (!truststoreFile.exists()) {
            logger.log(WARNING, "Truststore file does not exist: " + truststorePath);
            return;
        }

        logger.log(INFO, "Truststore file exists, last modified: " +
                   new java.util.Date(truststoreFile.lastModified()));

        // Try to load and analyze the truststore
        try {
            String truststoreType = config.getSSLTrustStoreType();
            if (truststoreType == null || truststoreType.isEmpty()) {
                truststoreType = KeyStore.getDefaultType();
            }

            KeyStore truststore = KeyStore.getInstance(truststoreType);

            try (FileInputStream fis = new FileInputStream(truststoreFile)) {
                truststore.load(fis, null);
                analyzeKeyStore(truststore, "Truststore");
            } catch (Exception e) {
                logger.log(WARNING, "Could not load truststore for analysis: " + e.getMessage());
            }

        } catch (Exception e) {
            logger.log(WARNING, "Failed to analyze truststore: " + e.getMessage());
        }
    }

    /**
     * Analyze certificates in a keystore and print their information.
     */
    private void analyzeKeyStore(KeyStore keyStore, String type) {
        try {
            logger.log(INFO, type + " contains " + keyStore.size() + " entries");

            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                logger.log(INFO, "  Alias: " + alias);

                if (keyStore.isKeyEntry(alias)) {
                    Certificate[] certChain = keyStore.getCertificateChain(alias);
                    if (certChain != null && certChain.length > 0) {
                        X509Certificate cert = (X509Certificate) certChain[0];

                        logger.log(INFO, "    Subject: " + cert.getSubjectDN().getName());
                        logger.log(INFO, "    Issuer: " + cert.getIssuerDN().getName());
                        logger.log(INFO, "    Valid from: " + cert.getNotBefore());
                        logger.log(INFO, "    Valid to: " + cert.getNotAfter());

                        // Calculate and log certificate fingerprint
                        String fingerprint = getCertificateFingerprint(cert);
                        logger.log(INFO, "    SHA256 Fingerprint: " + fingerprint);
                    }
                } else if (keyStore.isCertificateEntry(alias)) {
                    Certificate cert = keyStore.getCertificate(alias);
                    if (cert instanceof X509Certificate) {
                        X509Certificate x509Cert = (X509Certificate) cert;
                        logger.log(INFO, "    Certificate Subject: " + x509Cert.getSubjectDN().getName());

                        String fingerprint = getCertificateFingerprint(x509Cert);
                        logger.log(INFO, "    SHA256 Fingerprint: " + fingerprint);
                    }
                }
            }
        } catch (Exception e) {
            logger.log(WARNING, "Failed to analyze " + type + ": " + e.getMessage());
        }
    }

    /**
     * Calculate SHA256 fingerprint of a certificate.
     */
    private String getCertificateFingerprint(X509Certificate cert) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(cert.getEncoded());

            StringBuilder hexString = new StringBuilder();
            for (byte b : digest) {
                String hex = Integer.toHexString(0xff & b).toUpperCase();
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
                hexString.append(':');
            }

            // Remove the last colon
            if (hexString.length() > 0) {
                hexString.setLength(hexString.length() - 1);
            }

            return hexString.toString();
        } catch (Exception e) {
            return "Unable to calculate fingerprint: " + e.getMessage();
        }
    }
}