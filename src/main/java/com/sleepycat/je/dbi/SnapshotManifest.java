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

import static com.sleepycat.je.dbi.BackupManager.SNAPSHOT_PATTERN;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import com.sleepycat.json_simple.JsonException;
import com.sleepycat.json_simple.JsonKey;
import com.sleepycat.json_simple.JsonObject;
import com.sleepycat.json_simple.Jsoner;
import com.sleepycat.utilint.StringUtils;

/**
 * A manifest that lists the contents and copy status of a snapshot.
 */
public class SnapshotManifest {

    private static final boolean PRETTY_PRINT = true;

    /** The message digest format used to compute the manifest checksum. */
    private static final String MD_FORMAT = "SHA-256";

    /**
     * The current version of the manifest format.
     *
     * <p>Version history:
     *
     * <dl>
     * <dt>Version 0, JE 18.2.8
     * <dd>No version field, so the 0 version is implicit, no validation,
     * checksum is always "0"
     *
     * <dt>Version 1, JE 18.2.11
     * <dd>Added version field, validation, and checksum
     * </dl>
     */
    private static final int CURRENT_VERSION = 1;

    /** The version of this manifest. Added in version 1. */
    private final int version;

    /** The sequence number of this snapshot, first value 1, no gaps. */
    private final int sequence;

    /** The name of the snapshot in yymmddhh format. */
    private final String snapshot;

    /**
     * The absolute time in milliseconds that the snapshot was created. Used to
     * determine whether files were erased while the snapshot was being copied.
     * Must be greater than zero.
     */
    private final long startTimeMs;

    /**
     * The absolute time in milliseconds when latest file copy was completed,
     * for either a snapshot file or an erased file, or zero if no files were
     * copied.
     */
    private final long lastFileCopiedTimeMs;

    /**
     * The node name of the environment for which this snapshot was created.
     */
    private final String nodeName;

    /**
     * A checksum of the contents of the manifest, for error checking. The
     * checksum is computed from the field names and values, not from the text
     * of the file. The value is an arbitrary precision integer in hex format.
     *
     * <p>For version 0, the value is always "0".
     *
     * <p>For version 1, the value is SHA-256 checksum computed from all field
     *  values except for the checksum. When parsing a manifest, if the value
     *  is "0", then the checksum will not be checked.
     */
    private final String checksum;

    /**
     * The approximate value of the end of log of the snapshot. This value may
     * be -1 if it was not known, otherwise it will be greater than -1.
     */
    private final long endOfLog;

    /**
     * Approximate information about whether the environment was the master of
     * a replication group at the time the snapshot was created. Always false
     * for a non-replicated environment.
     */
    private final boolean isMaster;

    /**
     * Whether the associated snapshot represents a complete snapshot, where
     * all files in the snapshot have been copied, or one that is still in
     * progress or that was terminated without being completed. Must be false
     * if not all snapshot files are marked as copied.
     */
    private final boolean isComplete;

    /** Information about all log files included in this snapshot. */
    private final SortedMap<String, LogFileInfo> snapshotFiles;

    /**
     * Information about log files that were erased while the snapshot was
     * being copied. These versions of the log files cannot be used to restore
     * this snapshot.
     */
    private final SortedMap<String, LogFileInfo> erasedFiles;

    /**
     * Utility class for creating {@link SnapshotManifest} instances.
     */
    public static class Builder {
        private int version = CURRENT_VERSION;
        private int sequence = 1;
        private String snapshot;
        private long startTimeMs;
        private long lastFileCopiedTimeMs;
        private String nodeName;
        private String checksum;
        private long endOfLog;
        private boolean isMaster;
        private boolean isComplete;
        private final SortedMap<String, LogFileInfo> snapshotFiles;
        private final SortedMap<String, LogFileInfo> erasedFiles;
        public Builder() {
            snapshotFiles = new TreeMap<>();
            erasedFiles = new TreeMap<>();
        }
        public Builder(final SnapshotManifest base) {
            sequence = base.getSequence();
            snapshot = base.getSnapshot();
            startTimeMs = base.getStartTimeMs();
            lastFileCopiedTimeMs = base.getLastFileCopiedTimeMs();
            nodeName = base.getNodeName();
            endOfLog = base.getEndOfLog();
            isMaster = base.getIsMaster();
            isComplete = base.getIsComplete();
            snapshotFiles = new TreeMap<>(base.getSnapshotFiles());
            erasedFiles = new TreeMap<>(base.getErasedFiles());

        }
        public SnapshotManifest build() {
            return new SnapshotManifest(version,
                                        sequence,
                                        snapshot,
                                        startTimeMs,
                                        lastFileCopiedTimeMs,
                                        nodeName,
                                        checksum,
                                        endOfLog,
                                        isMaster,
                                        isComplete,
                                        snapshotFiles,
                                        erasedFiles);
        }
        public Builder setVersion(final int version) {
            this.version = version;
            return this;
        }
        public Builder setSequence(final int sequence) {
            this.sequence = sequence;
            return this;
        }
        public Builder setSnapshot(final String snapshot) {
            this.snapshot = snapshot;
            return this;
        }
        public Builder setStartTimeMs(final long startTimeMs) {
            this.startTimeMs = startTimeMs;
            return this;
        }
        public Builder setLastFileCopiedTimeMs(
            final long lastFileCopiedTimeMs) {

            this.lastFileCopiedTimeMs = lastFileCopiedTimeMs;
            return this;
        }
        public Builder setNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }
        public Builder setChecksum(final String checksum) {
            this.checksum = checksum;
            return this;
        }
        public Builder setEndOfLog(final long endOfLog) {
            this.endOfLog = endOfLog;
            return this;
        }
        public Builder setIsMaster(final boolean isMaster) {
            this.isMaster = isMaster;
            return this;
        }
        public Builder setIsComplete(final boolean isComplete) {
            this.isComplete = isComplete;
            return this;
        }
        public SortedMap<String, LogFileInfo> getSnapshotFiles() {
            return snapshotFiles;
        }
        public SortedMap<String, LogFileInfo> getErasedFiles() {
            return erasedFiles;
        }
    }

    private SnapshotManifest(
        final int version,
        final int sequence,
        final String snapshot,
        final long startTimeMs,
        final long lastFileCopiedTimeMs,
        final String nodeName,
        final String checksum,
        final long endOfLog,
        final boolean isMaster,
        final boolean isComplete,
        final SortedMap<String, LogFileInfo> snapshotFiles,
        final SortedMap<String, LogFileInfo> erasedFiles)
    {
        this.version = version;
        this.sequence = sequence;
        this.snapshot = snapshot;
        this.startTimeMs = startTimeMs;
        this.lastFileCopiedTimeMs = lastFileCopiedTimeMs;
        this.nodeName = nodeName;
        this.endOfLog = endOfLog;
        this.isMaster = isMaster;
        this.isComplete = isComplete;
        this.snapshotFiles = snapshotFiles;
        this.erasedFiles = erasedFiles;
        validate();
        this.checksum = (checksum == null) ? computeChecksum() : checksum;
    }

    /**
     * Creates a manifest from a parsed Json object.
     *
     * @throws RuntimeException if there is a problem converting the JSON input
     * to a valid SnapshotManifest instance
     */
    private SnapshotManifest(final JsonObject json) {
        final Integer versionValue = json.getInteger(JsonField.version);
        version = (versionValue != null) ? versionValue : 0;
        sequence = getInteger(json, JsonField.sequence);
        snapshot = json.getString(JsonField.snapshot);
        startTimeMs = getLong(json, JsonField.startTimeMs);
        lastFileCopiedTimeMs = getLong(json, JsonField.lastFileCopiedTimeMs);
        nodeName = json.getString(JsonField.nodeName);
        checksum = json.getString(JsonField.checksum);
        endOfLog = getLong(json, JsonField.endOfLog);
        isMaster = getBoolean(json, JsonField.isMaster);
        isComplete = getBoolean(json, JsonField.isComplete);
        snapshotFiles = getLogFileMap(json, JsonField.snapshotFiles);
        erasedFiles = getLogFileMap(json, JsonField.erasedFiles);

        validate();
        if (!"0".equals(checksum)) {
            final String expectedChecksum = computeChecksum();
            if (!expectedChecksum.equals(checksum)) {
                throw new IllegalArgumentException(
                    "Incorrect checksum: expected " + expectedChecksum +
                    ", found " + checksum);
            }
        }
    }

    private static int getInteger(final JsonObject json, final JsonKey field) {
        final Integer value = json.getInteger(field);
        if (value == null) {
            throw new IllegalArgumentException("Missing field: " + field);
        }
        return value;
    }

    private static long getLong(final JsonObject json, final JsonKey field) {
        final Long value = json.getLong(field);
        if (value == null) {
            throw new IllegalArgumentException("Missing field: " + field);
        }
        return value;
    }

    private static boolean getBoolean(final JsonObject json,
                                      final JsonKey field) {
        final Boolean value = json.getBoolean(field);
        if (value == null) {
            throw new IllegalArgumentException("Missing field: " + field);
        }
        return value;
    }

    private static SortedMap<String, LogFileInfo> getLogFileMap(
        final JsonObject json, final JsonField field) {

        final Map<String, JsonObject> jsonMap = json.getMap(field);
        if (jsonMap == null) {
            throw new IllegalArgumentException("Missing field: " + field);
        }
        final SortedMap<String, LogFileInfo> logFileMap = new TreeMap<>();
        for (final Map.Entry<String, JsonObject> entry : jsonMap.entrySet()) {
            final String key = entry.getKey();
            final JsonObject value = entry.getValue();
            if (value == null) {
                throw new IllegalArgumentException(
                    "Key " + key + " missing for field " + field);
            }
            logFileMap.put(key, new LogFileInfo(value));
        }
        return logFileMap;
    }

    /**
     * Returns a map that can be used for Json serialization.
     * Returns a sorted map for predictable output order.
     */
    SortedMap<String, Object> toJsonMap() {

        final SortedMap<String, Object> map = new TreeMap<>();

        map.put(JsonField.version.name(), version);
        map.put(JsonField.sequence.name(), sequence);
        map.put(JsonField.snapshot.name(), snapshot);
        map.put(JsonField.startTimeMs.name(), startTimeMs);
        map.put(JsonField.lastFileCopiedTimeMs.name(), lastFileCopiedTimeMs);
        map.put(JsonField.nodeName.name(), nodeName);
        map.put(JsonField.checksum.name(), checksum);
        map.put(JsonField.endOfLog.name(), endOfLog);
        map.put(JsonField.isMaster.name(), isMaster);
        map.put(JsonField.isComplete.name(), isComplete);
        map.put(JsonField.snapshotFiles.name(), getJsonMap(snapshotFiles));
        map.put(JsonField.erasedFiles.name(), getJsonMap(erasedFiles));
        return map;
    }

    private SortedMap<String, Object> getJsonMap(
        final SortedMap<String, LogFileInfo> logFileMap) {

        final SortedMap<String, Object> jsonMap = new TreeMap<>();
        for (final Map.Entry<String, LogFileInfo> entry :
                 logFileMap.entrySet()) {
            jsonMap.put(entry.getKey(), entry.getValue().toJsonMap());
        }
        return jsonMap;
    }

    /** For use with the JsonObject API. */
    private enum JsonField implements JsonKey {
        version,
        sequence,
        snapshot,
        startTimeMs,
        lastFileCopiedTimeMs,
        nodeName,
        checksum,
        endOfLog,
        isMaster,
        isComplete,
        snapshotFiles,
        erasedFiles;

        @Override
        public String getKey() {
            return name();
        }

        @Override
        public Object getValue() {
            return null;
        }
    }

    public int getVersion() {
        return version;
    }

    public int getSequence() {
        return sequence;
    }

    public String getSnapshot() {
        return snapshot;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getLastFileCopiedTimeMs() {
        return lastFileCopiedTimeMs;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getChecksum() {
        return checksum;
    }

    public long getEndOfLog() {
        return endOfLog;
    }

    public boolean getIsMaster() {
        return isMaster;
    }

    public boolean getIsComplete() {
        return isComplete;
    }

    public SortedMap<String, LogFileInfo> getSnapshotFiles() {
        return snapshotFiles;
    }

    public SortedMap<String, LogFileInfo> getErasedFiles() {
        return erasedFiles;
    }

    /**
     * Create a serialized form of this instance.
     *
     * @return the serialized form
     * @throws IOException if a problem occurs during serialization
     */
    public byte[] serialize()
        throws IOException {

        return serialize(toJsonMap());
    }

    static byte[] serialize(final SortedMap<String, Object> map)
        throws IOException {

        if (PRETTY_PRINT) {
            final Writer writer = new StringWriter(512);
            Jsoner.serialize(map, writer);
            final String result = Jsoner.prettyPrint(writer.toString());
            return result.getBytes("UTF-8");
        } else {
            final ByteArrayOutputStream out = new ByteArrayOutputStream(512);
            final Writer writer = new OutputStreamWriter(out, "UTF-8");
            Jsoner.serialize(map, writer);
            writer.flush();
            return out.toByteArray();
        }
    }

    /**
     * Create a SnapshotManifest instance from bytes in serialized form.
     *
     * @return the instance
     * @throws IOException if a problem occurs during deserialization, in
     * particular if the data format is invalid
     */
    public static SnapshotManifest deserialize(byte[] bytes)
        throws IOException {

        try {
            return new SnapshotManifest(deserializeToJson(bytes));
        } catch (JsonException|RuntimeException e) {
            final String msg = e.getMessage();
            throw new IOException(msg != null ? msg : e.toString(), e);
        }
    }

    static JsonObject deserializeToJson(byte[] bytes)
        throws JsonException, IOException {

        final ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        final InputStreamReader reader = new InputStreamReader(in, "UTF-8");
        return (JsonObject) Jsoner.deserialize(reader);
    }

    /**
     * Checks if the format of the instance is valid, but does no validation
     * if the version is 0.
     *
     * @throws IllegalArgumentException if the format of the instance is
     * not valid
     */
    public void validate() {
        if (version == 0) {
            return;
        }
        if (version < 0) {
            throw new IllegalArgumentException(
                "version must not be less than 0: " + version);
        }
        if (sequence < 1) {
            throw new IllegalArgumentException(
                "sequence must not be less than 1: " + sequence);
        }
        checkNull("snapshot", snapshot);
        if (!SNAPSHOT_PATTERN.matcher(snapshot).matches()) {
            throw new IllegalArgumentException(
                "snapshot name is invalid: " + snapshot);
        }
        if (startTimeMs <= 0) {
            throw new IllegalArgumentException(
                "startTimeMs must be greater than 0: " + startTimeMs);
        }
        if (lastFileCopiedTimeMs < 0) {
            throw new IllegalArgumentException(
                "lastFileCopiedTimeMs must not be less than 0: " +
                lastFileCopiedTimeMs);
        }
        checkNull("nodeName", nodeName);
        /* Checksum will be checked after validation */
        if (endOfLog < -1) {
            throw new IllegalArgumentException(
                "endOfLog must not be less than -1");
        }
        checkNull("snapshotFiles", snapshotFiles);
        snapshotFiles.forEach((logFile, info) -> {
                checkNull("snapshotFile info for " + logFile, info);
                if (!info.getIsCopied()) {
                    if (isComplete) {
                        throw new IllegalArgumentException(
                            "snapshot cannot be complete when a log file was" +
                            " not copied: " + logFile);
                    }
                } else if (snapshot.equals(info.getSnapshot())) {
                    validateCopiedFile(logFile, info);
                }
            });
        checkNull("erasedFiles", erasedFiles);
        erasedFiles.forEach((logFile, info) -> {
                checkNull("erasedFile info for " + logFile, info);
                if (info.getIsCopied()) {
                    if (!snapshot.equals(info.getSnapshot())) {
                        throw new IllegalArgumentException(
                            "Snapshot " + snapshot + " does not match" +
                            " snapshot for copied erased file " + logFile +
                            ": " + info.getSnapshot());
                    }
                    validateCopiedFile(logFile, info);
                }
            });
    }

    private void validateCopiedFile(final String logFile,
                                    final LogFileInfo info) {
        assert info.getIsCopied();
        assert snapshot.equals(info.getSnapshot());
        final long copyStartTimeMs = info.getCopyStartTimeMs();
        if (copyStartTimeMs < startTimeMs) {
            throw new IllegalArgumentException(
                "copyStartTimeMs " + copyStartTimeMs +
                " for file " + logFile +
                " must not be less than startTimeMs " +
                startTimeMs);
        }
        if (copyStartTimeMs > lastFileCopiedTimeMs) {
            throw new IllegalArgumentException(
                "lastFileCopiedTimeMs " + lastFileCopiedTimeMs +
                " must not be less than copyStartTimeMs " + copyStartTimeMs +
                " for file " + logFile);
        }
    }

    private static void checkNull(final String fieldName, final Object value) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName +
                                               " must not be null");
        }
    }

    private String computeChecksum() {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance(MD_FORMAT);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unexpected failure: " + e, e);
        }
        tallyChecksum(md);
        return BackupManager.checksumToHex(md.digest());
    }

    private void tallyChecksum(final MessageDigest md) {
        tallyChecksumInt(md, version);
        tallyChecksumInt(md, sequence);
        tallyChecksumString(md, snapshot);
        tallyChecksumLong(md, startTimeMs);
        tallyChecksumLong(md, lastFileCopiedTimeMs);
        tallyChecksumString(md, nodeName);
        tallyChecksumLong(md, endOfLog);
        tallyChecksumBoolean(md, isMaster);
        tallyChecksumBoolean(md, isComplete);
        tallyChecksum(md, snapshotFiles);
        tallyChecksum(md, erasedFiles);
    }

    private static void tallyChecksumString(final MessageDigest md,
                                            final String value) {
        md.update(StringUtils.toUTF8(value));
    }

    private static void tallyChecksumBoolean(final MessageDigest md,
                                             final boolean value) {
        md.update((byte) (value ? 1 : 0));
    }

    private static void tallyChecksumInt(final MessageDigest md,
                                         final int value) {
        final byte[] bytes = new byte[4];
        bytes[0] = (byte) (value >>> 24);
        bytes[1] = (byte) (value >>> 16);
        bytes[2] = (byte) (value >>> 8);
        bytes[3] = (byte) value;
        md.update(bytes);
    }

    private static void tallyChecksumLong(final MessageDigest md,
                                          final long value) {
        final byte[] bytes = new byte[8];
        bytes[0] = (byte) (value >>> 56);
        bytes[1] = (byte) (value >>> 48);
        bytes[2] = (byte) (value >>> 40);
        bytes[3] = (byte) (value >>> 32);
        bytes[4] = (byte) (value >>> 24);
        bytes[5] = (byte) (value >>> 16);
        bytes[6] = (byte) (value >>> 8);
        bytes[7] = (byte) value;
        md.update(bytes);
    }

    private static void tallyChecksum(
        final MessageDigest md,
        final SortedMap<String, LogFileInfo> logFileMap) {

        tallyChecksumInt(md, logFileMap.size());
        logFileMap.forEach(
            (k, v) -> {
                tallyChecksumString(md, k);
                v.tallyChecksum(md);
            });
    }

    @Override
    public String toString() {
        return "SnapshotManifest[" +
            "version:" + version +
            " sequence:" + sequence +
            " snapshot:" + snapshot +
            " startTimeMs:" + startTimeMs +
            " lastFileCopiedTimeMs:" + lastFileCopiedTimeMs +
            " nodeName:" + nodeName +
            " checksum:" + checksum +
            " endOfLog:" + endOfLog +
            " isMaster:" + isMaster +
            " isComplete:" + isComplete +
            " snapshotFiles:" + snapshotFiles +
            " erasedFiles:" + erasedFiles +
            "]";
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SnapshotManifest)) {
            return false;
        }
        final SnapshotManifest other = (SnapshotManifest) object;
        return (version == other.version) &&
            (sequence == other.sequence) &&
            Objects.equals(snapshot, other.snapshot) &&
            (startTimeMs == other.startTimeMs) &&
            (lastFileCopiedTimeMs == other.lastFileCopiedTimeMs) &&
            Objects.equals(nodeName, other.nodeName) &&
            Objects.equals(checksum, other.checksum) &&
            (endOfLog == other.endOfLog) &&
            (isMaster == other.isMaster) &&
            (isComplete == other.isComplete) &&
            Objects.equals(snapshotFiles, other.snapshotFiles) &&
            Objects.equals(erasedFiles, other.erasedFiles);
    }

    @Override
    public int hashCode() {
        int hash = 41;
        hash = (43 * hash) + version;
        hash = (43 * hash) + sequence;
        hash = (43 * hash) + Objects.hashCode(snapshot);
        hash = (43 * hash) + Long.hashCode(startTimeMs);
        hash = (43 * hash) + Long.hashCode(lastFileCopiedTimeMs);
        hash = (43 * hash) + Objects.hashCode(nodeName);
        hash = (43 * hash) + Objects.hashCode(checksum);
        hash = (43 * hash) + Long.hashCode(endOfLog);
        hash = (43 * hash) + Boolean.hashCode(isMaster);
        hash = (43 * hash) + Boolean.hashCode(isComplete);
        hash = (43 * hash) + Objects.hashCode(snapshotFiles);
        hash = (43 * hash) + Objects.hashCode(erasedFiles);
        return hash;
    }

    /**
     * Information associated with a single log file in the snapshot.
     */
    public static class LogFileInfo {

        /**
         * A checksum of the contents of the log file, for error checking, or
         * "0" if not yet computed. The value is an arbitrary precision integer
         * in hex format. For erased files, the value represents the checksum
         * of the data stored in the archive, but that value may not match the
         * checksum for the local file if the file was erased while it was
         * being copied.
         */
        private final String checksum;

        /**
         * Whether this file has been copied to the archive.
         */
        private final boolean isCopied;

        /**
         * The time the file copy started or 0 if not copied.
         */
        private final long copyStartTimeMs;

        /**
         * The name of the snapshot containing the copied file in the archive.
         */
        private final String snapshot;

        /**
         * The node name of the environment from which the log file was
         * obtained.
         */
        private final String nodeName;

        public LogFileInfo(final String checksum,
                           final boolean isCopied,
                           final long copyStartTimeMs,
                           final String snapshot,
                           final String nodeName) {
            this.checksum = checksum;
            this.isCopied = isCopied;
            this.copyStartTimeMs = copyStartTimeMs;
            this.snapshot = snapshot;
            this.nodeName = nodeName;
            validate();
        }

        /**
         * Create initial information for a log file which records the snapshot
         * and node name but leaves other fields at default values to be filled
         * in after the file is copied.
         */
        public LogFileInfo(final String snapshot, final String nodeName) {
            checksum = "0";
            isCopied = false;
            copyStartTimeMs = 0;
            this.snapshot = snapshot;
            this.nodeName = nodeName;
            validate();
        }

        /**
         * Create final information for a log file after it is copied.
         */
        public LogFileInfo(final String checksum,
                           final long copyStartTimeMs,
                           final SnapshotManifest manifest) {
            this.checksum = checksum;
            this.isCopied = true;
            this.copyStartTimeMs = copyStartTimeMs;
            snapshot = manifest.getSnapshot();
            nodeName = manifest.getNodeName();
            validate();
        }

        /**
         * Creates a LogFileInfo from a parsed Json object.
         *
         * @throws RuntimeException if there is a problem converting the JSON
         * input to a valid LogFileInfo instance
         */
        LogFileInfo(final JsonObject json) {
            checksum = json.getString(JsonField.checksum);
            isCopied = getBoolean(json, JsonField.isCopied);
            copyStartTimeMs = getLong(json, JsonField.copyStartTimeMs);
            snapshot = json.getString(JsonField.snapshot);
            nodeName = json.getString(JsonField.nodeName);
            validate();
        }

        /**
         * Returns a map that can be used for Json serialization.
         * Returns a sorted map for predictable output order.
         */
        SortedMap<String, Object> toJsonMap() {

            final SortedMap<String, Object> map = new TreeMap<>();

            map.put(JsonField.checksum.name(), checksum);
            map.put(JsonField.isCopied.name(), isCopied);
            map.put(JsonField.copyStartTimeMs.name(), copyStartTimeMs);
            map.put(JsonField.snapshot.name(), snapshot);
            map.put(JsonField.nodeName.name(), nodeName);

            return map;
        }

        /** For use with the JsonObject API. */
        private enum JsonField implements JsonKey {
            checksum,
            isCopied,
            copyStartTimeMs,
            snapshot,
            nodeName;

            @Override
            public String getKey() {
                return name();
            }

            @Override
            public Object getValue() {
                return null;
            }
        }

        public String getChecksum() {
            return checksum;
        }

        public boolean getIsCopied() {
            return isCopied;
        }

        public long getCopyStartTimeMs() {
            return copyStartTimeMs;
        }

        public String getSnapshot() {
            return snapshot;
        }

        public String getNodeName() {
            return nodeName;
        }

        /**
         * Checks if the format of the instance is valid.
         *
         * @throws IllegalArgumentException if the format of the instance is
         * not valid
         */
        public void validate() {
            checkNull("checksum", checksum);
            if (isCopied) {
                if ("0".equals(checksum)) {
                    throw new IllegalArgumentException(
                        "checksum for copied entry must not be \"0\"");
                }
            }
            if (copyStartTimeMs < 0) {
                throw new IllegalArgumentException(
                    "copyStartTimeMs must not be negative: " +
                    copyStartTimeMs);
            }
            if (isCopied) {
                if (copyStartTimeMs == 0) {
                    throw new IllegalArgumentException(
                        "copyStartTimeMs for copied entry must not be 0");
                }
            }
            checkNull("snapshot", snapshot);
            if (!SNAPSHOT_PATTERN.matcher(snapshot).matches()) {
                throw new IllegalArgumentException(
                    "snapshot name is invalid: " + snapshot);
            }
            checkNull("nodeName", nodeName);
        }

        void tallyChecksum(final MessageDigest md) {
            tallyChecksumString(md, checksum);
            tallyChecksumBoolean(md, isCopied);
            tallyChecksumLong(md, copyStartTimeMs);
            tallyChecksumString(md, snapshot);
            tallyChecksumString(md, nodeName);
        }

        @Override
        public String toString() {
            return "LogFileInfo[" +
                "checksum:" + checksum +
                " isCopied:" + isCopied +
                " copyStartTimeMs:" + copyStartTimeMs +
                " snapshot:" + snapshot +
                " nodeName:" + nodeName +
                "]";
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof LogFileInfo)) {
                return false;
            }
            final LogFileInfo other = (LogFileInfo) object;
            return Objects.equals(checksum, other.checksum) &&
                (isCopied == other.isCopied) &&
                (copyStartTimeMs == other.copyStartTimeMs) &&
                Objects.equals(snapshot, other.snapshot) &&
                Objects.equals(nodeName, other.nodeName);
        }

        @Override
        public int hashCode() {
            int hash = 71;
            hash = (73 * hash) + Objects.hashCode(checksum);
            hash = (73 * hash) + Boolean.hashCode(isCopied);
            hash = (73 * hash) + Long.hashCode(copyStartTimeMs);
            hash = (73 * hash) + Objects.hashCode(snapshot);
            hash = (73 * hash) + Objects.hashCode(nodeName);
            return hash;
        }
    }
}
