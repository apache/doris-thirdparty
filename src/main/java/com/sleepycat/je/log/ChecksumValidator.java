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

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.Adler32;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Checksum validator is used to check checksums on log entries.
 */
public class ChecksumValidator {
    private static final boolean DEBUG = false;

    private final EnvironmentImpl envImpl;
    private Checksum cksum;

    public ChecksumValidator(final EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
        cksum = Adler32.makeChecksum();
    }

    public void reset() {
        cksum.reset();
    }

    /**
     * Add this byte buffer to the checksum. Assume the byte buffer is already
     * positioned at the data.
     * @param buf target buffer
     * @param length of data
     */
    public void update(ByteBuffer buf, int length)
        throws ChecksumException {

        if (buf == null) {
            throw new ChecksumException(
                "null buffer given to checksum validation, probably " +
                 " result of 0's in log file, len=" + length);
        }

        int bufStart = buf.position();

        if (DEBUG) {
            System.out.println("bufStart = " + bufStart +
                               " length = " + length);
        }

        update(buf.array(), bufStart + buf.arrayOffset(), length);
    }

    public void update(byte[] buf, int offset, int length) {
        cksum.update(buf, offset, length);
    }

    void validate(long expectedChecksum, long lsn)
        throws ChecksumException {

        if (expectedChecksum != cksum.getValue()) {

            /*
             * Check for erased log entry before throwing ChecksumException.
             * This handles the following reader scenario:
             * 1. Reader thread reads the header. The entry type is not
             *    LOG_ERASED, so checksum verification proceeds.
             * 2. Eraser thread erases the entry.
             * 3. Reader thread reads the rest of the log entry, which was
             *    overwritten in step 2. The erased data is used to update
             *    the checksum.
             * 4. Reader thread calls this method and checksum verification
             *    fails.
             */
            if (envImpl.getDataEraser().isEntryErased(lsn)) {
                return;
            }

            throw new ChecksumException
                ("Location " + DbLsn.getNoFormatString(lsn) +
                 " expected " + expectedChecksum + " got " + cksum.getValue());
        }
    }

    public void validate(long expectedChecksum, long fileNum, long fileOffset)
        throws ChecksumException {

        validate(expectedChecksum, DbLsn.makeLsn(fileNum, fileOffset));
    }
}
