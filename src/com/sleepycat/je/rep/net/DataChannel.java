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

package com.sleepycat.je.rep.net;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SocketChannel;

/**
 * @hidden
 * An interface that associates a delegate socketChannel for network I/O, which
 * provides ByteChannel, GatheringByteChannel, and ScatteringByteChannel,
 * interfaces for callers.
 *
 * The interface supports both blocking/non-blocking socketChannel as well as
 * normal and SSL connection, which complicates the semantics of its methods.
 */
public interface DataChannel extends ByteChannel,
                                     GatheringByteChannel,
                                     ScatteringByteChannel {

    /**
     * Checks whether the channel is connected.
     *
     * @return {@code true} if the channel is connected
     */
    public boolean isConnected();

    /**
     * Retrieves a socket associated with this channel.
     *
     * @return a socket associated with this channel.
     */
    public Socket socket();

    /**
     * Returns the remote address to which this channel's socket is connected.
     */
    public SocketAddress getRemoteAddress() throws IOException;

    /**
     * Adjusts this channel's blocking mode.
     */
    public void configureBlocking(boolean block) throws IOException;

    /**
     * Tells whether or not every I/O operation on this channel will block
     * until it completes.
     */
    public boolean isBlocking();

    /**
     * Checks whether the channel encrypted.
     *
     * @return true if the data channel provides network privacy
     */
    public boolean isSecure();

    /**
     * Checks whether  the channel capable of determining peer trust.
     *
     * @return true if the data channel implementation has the capability
     * to determine trust.
     */
    public boolean isTrustCapable();

    /**
     * Checks whether the channel peer is trusted.
     *
     * @return true if the channel has determined that the peer is trusted.
     */
    public boolean isTrusted();

    /**
     * The status of the flush method.
     */
    public enum FlushStatus {

        /** Flushes are not being used. */
        DISABLED,

        /** Nothing needs to be flushed. */
        DONE,

        /** Flush not complete because there is something left to flush. */
        AGAIN,

        /** Flush not complete because socket is busy. */
        SO_WAIT_WRITE,

        /** Flush not complete due to a concurrent competing operation. */
        CONTENTION,
    }

    /**
     * Reads a sequence of bytes from this channel into a subsequence of the
     * given buffers.
     *
     * <p>The method throws {@link ClosedChannelException} after channel is
     * closed. The channel is closed if any of the {@link #close}, {@link
     * #closeForcefully} or {@link #closeAsync} was called (regardless of
     * the return value or exception thrown). The method may throw {@link
     * AsynchronousCloseException} when another thread is closing the channel
     * concurrently.
     *
     * <p>The method should not block any of the close methods.
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
          throws IOException;

    /**
     * Writes a sequence of bytes to this channel from a subsequence of the
     * given buffers.
     *
     * <p>The behavior w.r.t the {@link #flush} method:
     * <ul>
     * <li>If the channel is configured blocking, the method should flush the
     * written data before it normally exits.</li>
     *
     * <li>If the channel is configured non-blocking, the write method does not
     * guarantee the written data is flushed. The caller should call the flush
     * method, or close the socket with close or closeAsync, if it needs to
     * make sure that the data has been completely written.</li>
     * </ul>
     *
     * <p>The behavior w.r.t the channel-close methods:
     *
     * <ul>
     * <li>The method throws {@link ClosedChannelException} after channel is
     * closed. The channel is closed if any of the {@link #close}, {@link
     * #closeForcefully} or {@link #closeAsync} was called and exited
     * (regardless of the return value or exception thrown). The method may
     * throw {@link AsynchronousCloseException} when another thread is closing
     * the channel concurrently.</li>
     *
     * <li>The method should not block any of the close methods.</li>
     * </ul>
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
        throws IOException;

    /**
     * Attempts to flush any pending writes to the underlying socket buffer.
     *
     * <p>Calling this method is not needed in cases where {@link #write}
     * itself will push the data to the transport. In such cases, the method is
     * equivalent to a no-op method and should return {@link
     * FlushStatus#DISABLED}.
     *
     * <p>The method throws {@link ClosedChannelException} after channel is
     * closed. The channel is closed if any of the {@link #close}, {@link
     * #closeForcefully} or {@link #closeAsync} was called (regardless of
     * the return value or exception thrown). The method may throw {@link
     * AsynchronousCloseException} when another thread is closing the channel
     * concurrently.
     *
     * @return the flush status
     */
    public FlushStatus flush() throws IOException;

    /**
     * Closes this channel, which should be a blocking channel.
     *
     * <p>It is considered a coding error to call this method if the channel is
     * configured as non-blocking: use {@link #closeAsync} or {@link
     * #closeForcefully} instead.  If called for a non-blocking channel, this
     * method closes the channel forcefully and throws {@link
     * IllegalStateException}.
     *
     * <p>The method cleanly closes the channel, blocking if necessary. The
     * method should only be blocked for its own connection operations (i.e.,
     * not by concurrent {@link #read},{@link #write}, {@link #flush} methods
     * or lock acquisitions, etc). The method throws {@link IOException} if the
     * connection is unresponsive and timed out.
     *
     * <p>If the method detects a concurrent {@link #read}, {@link #write} or
     * {@link #flush} method being called, the method throws {@link
     * IOException}.
     *
     * <p>Implementations of this method should flush written data before
     * closing the channel.
     *
     * <p>If an error occurs, the method attempts to forcefully close the
     * channel, before throwing the exception. Only the first encountered
     * exception is thrown.
     *
     * <p>The channel is closed (i.e., {@link isOpen} returns {@code false})
     * after this method returns, even if an exception is thrown. The {@link
     * #read}, {@link #write} and {@link #flush} methods will throw {@link
     * ClosedChannelException} when the channel is closed. These methods may
     * throw {@link AsynchronousCloseException} if called concurrently with
     * this method.
     *
     * @throws IOException if an error occurs
     */
    @Override
    public void close() throws IOException;

    /**
     * The status of the close async method.
     */
    public enum CloseAsyncStatus {

        /* Close not complete waiting for a read. */
        SO_WAIT_READ,

        /* Close not complete waiting for a write. */
        SO_WAIT_WRITE,

        /* Close complete. */
        DONE,
    }

    /**
     * Closes this channel, which should be a non-blocking channel.
     *
     * <p>It is considered a coding error to call this method if the channel is
     * configured as blocking: use {@link #close} or {@link #closeForcefully}
     * instead. If called for a blocking channel, this method closes the
     * channel forcefully and throws {@link IllegalStateException}.
     *
     * <p>The method cleanly closes the channel in a non-blocking manner. The
     * channel is closed (i.e., {@link isOpen} returns {@code false}) once the
     * method is called (regardless of return value or exception thrown). The
     * caller, however, should keep calling this method until it returns {@code
     * true} or throws exception. The channel is cleanly closed if the method
     * returns {@code true} or forcefully closed if the method throws
     * exception.
     *
     * <p>Implementations of this method should flush written data before
     * closing the channel.
     *
     * <p>If the method detects a concurrent {@link #read}, {@link #write} or
     * {@link #flush} method being called, the method throws {@link
     * IOException}.
     *
     * <p>If an error occurs, the method attempts to forcefully close the
     * channel, before throwing the exception. Only the first encountered
     * exception is thrown.
     *
     * <p> The {@link #read}, {@link #write} and {@link #flush} methods will
     * throw {@link ClosedChannelException} when the channel is closed. These
     * methods may throw {@link AsynchronousCloseException} if called
     * concurrently with this method.
     *
     * @return {@code true} if the channel is cleanly closed
     * @throws IOException if there is an error
     */
     public CloseAsyncStatus closeAsync() throws IOException;

    /**
     * Closes this channel forcefully.
     *
     * <p>The method returns immediately, i.e., it should not be blocked by
     * concurrent {@link #read}, {@link #write} or {@link #flush} methods, nor
     * should it be blocked by socket operations.
     *
     * <p>The method does not guarantee to flush written data before closing
     * the channel, nor does it guarantee to do any procedure required for a
     * clean close.
     *
     * <p>The channel is closed (i.e., {@link isOpen} returns {@code false})
     * after this method returns. The {@link #read}, {@link #write} and {@link
     * #flush} methods will throw {@link ClosedChannelException} when the
     * channel is closed. These methods may throw {@link
     * AsynchronousCloseException} if called concurrently with this method.
     *
     * @throws IOException if an error occurs
     */
    public void closeForcefully() throws IOException;

    /**
     * Accessor for the underlying SocketChannel.
     *
     * Use of this accessor is discouraged. An implementation may have special
     * treatment for methods in SocketChannel. For example, SSLDataChannel
     * caches the blocking mode to avoid some blocking issue. Therefore, using
     * the above wrap methods are preferrable.
     *
     * @return the socket channel underlying this data channel instance
     */
    public SocketChannel getSocketChannel();

}

