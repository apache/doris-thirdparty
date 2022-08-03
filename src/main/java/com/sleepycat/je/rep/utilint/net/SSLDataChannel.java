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

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.INFO;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus;
import static javax.net.ssl.SSLEngineResult.Status;

import java.io.IOException;
import java.net.SocketException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.rep.net.SSLAuthenticator;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * SSLDataChannel provides SSL-based communications on top of a SocketChannel.
 * We attempt to maintain a degree of compatibility with SocketChannel
 * in terms of request completion semantics.  In particular,
 *    If in blocking mode:
 *       read() will return at least one byte if the buffer has room
 *       write() will write the entire buffer
 *    If in non-blocking mode:
 *       read() and write are not guaranteed to consume or produce anything.
 */
public class SSLDataChannel extends AbstractDataChannel {

    /**
     * A null read channel.
     *
     * The channel is used as a sentinel to detect the cases when the caller is
     * switching between blocking and non-blocking channel and doing a read
     * operation at the same time. We do not support such cases.
     */
    private static final ReadableByteChannel NULL_READ_CHANNEL =
        new NullReadChannel();

    /**
     * A wrapped socket channel so that read can be timed out.
     *
     * SocketChannel#read does not time out with the channel configured
     * blocking and the underlying socket having SO_TIMEOUT set. A work-around
     * channel is needed.
     *
     * The channel points to the work-around channel to read for blocking
     * channels. It points to the normal socket channel otherwise.
     *
     * Write to the field is either at construction time or in a synchronized
     * block. Volatile for read thread-safety.
     */
    private volatile ReadableByteChannel wrappedReadChannel;

    /**
     * The SSLEngine that will manage the secure operations.
     */
    private final SSLEngine sslEngine;

    /**
     * raw bytes received from the SocketChannel - not yet unwrapped.
     */
    private final ByteBuffer netRecvBuffer;

    /**
     * raw bytes to be sent to the wire - already wrapped
     */
    private final ByteBuffer netXmitBuffer;

    /**
     * Bytes unwrapped and ready for application consumption.
     */
    private final ByteBuffer appRecvBuffer;

    /**
     * A dummy buffer used during handshake operations.
     */
    private final ByteBuffer emptyXmitBuffer;

    /**
     * Lock object for protection of appRecvBuffer, netRecvBuffer and SSLEngine
     * unwrap() operations
     */
    private final ReentrantLock readLock = new ReentrantLock();

    /**
     * Lock object for protection of netXmitBuffer and SSLEngine wrap()
     * operations
     */
    private final ReentrantLock writeLock = new ReentrantLock();

    /**
     * Set to true when any of the close methods is called.
     */
    private volatile boolean channelClosed = false;

    /*
     * Remember whether we did a closeInbound already.
     */
    private volatile boolean sslInboundClosed = false;

    /**
     * The String identifying the target host that we are connecting to, if
     * this channel was created in client context.
     */
    private final String targetHost;

    /**
     * Possibly null authenticator object used for checking whether the
     * peer for the negotiated session should be trusted.
     */
    private final SSLAuthenticator authenticator;

    /**
     * Possibly null host verifier object used for checking whether the
     * peer for the negotiated session is correct based on the connection
     * target.
     */
    private final HostnameVerifier hostVerifier;

    /**
     * Set to true when a handshake completes and a non-null authenticator
     * acknowledges the session as trusted.
     */
    private volatile boolean peerTrusted = false;

    private final InstanceLogger logger;

    /**
     * Construct an SSLDataChannel given a SocketChannel and an SSLEngine
     *
     * @param socketChannel a SocketChannel over which SSL communcation will
     *     occur.  This should generally be connected, but that is not
     *     absolutely required until the first read/write operation.
     * @param sslEngine an SSLEngine instance that will control the SSL
     *     interaction with the peer.
     */
    public SSLDataChannel(SocketChannel socketChannel,
                          SSLEngine sslEngine,
                          String targetHost,
                          HostnameVerifier hostVerifier,
                          SSLAuthenticator authenticator,
                          InstanceLogger logger) {

        super(socketChannel);
        this.sslEngine = sslEngine;
        this.targetHost = targetHost;
        this.authenticator = authenticator;
        this.hostVerifier = hostVerifier;
        this.logger = logger;
        SSLSession sslSession = sslEngine.getSession();

        /* Determine the required buffer sizes */
        int netBufferSize = sslSession.getPacketBufferSize();
        int appBufferSize = sslSession.getApplicationBufferSize();

        /* allocate the buffers */
        this.emptyXmitBuffer = ByteBuffer.allocate(1);
        this.netXmitBuffer = ByteBuffer.allocate(3*netBufferSize);
        this.appRecvBuffer = ByteBuffer.allocate(2*appBufferSize);
        this.netRecvBuffer = ByteBuffer.allocate(2*netBufferSize);

        try {
            if (isBlocking()) {
                this.wrappedReadChannel =
                    Channels.newChannel(
                            socketChannel.socket().getInputStream());
            } else {
                this.wrappedReadChannel = socketChannel;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                "Cannot get stream from connected socket " + socketChannel, e);
        }
    }

    /**
     * Configures the channel to be blocking.
     */
    @Override
    public synchronized void configureBlocking(boolean block)
        throws IOException {

        if (block == isBlocking()) {
            return;
        }

        /*
         * Sets the channel to a null channel so that we can detect concurrent
         * reads that we do not support.
         */
        wrappedReadChannel = NULL_READ_CHANNEL;

        socketChannel.configureBlocking(block);
        if (block) {
            wrappedReadChannel =
                Channels.newChannel(
                        socketChannel.socket().getInputStream());
        } else {
            wrappedReadChannel = socketChannel;
        }
        configuredBlocking = block;
    }

    /**
     * Is the channel encrypted?
     * @return true if the channel is encrypted
     */
    @Override
    public boolean isSecure() {
        return true;
    }

    /**
     * Is the channel capable of determining peer trust?
     * In this case, we are capable only if the application has configured an
     * SSL authenticator
     *
     * @return true if this data channel is capable of determining trust
     */
    @Override
    public boolean isTrustCapable() {
        return authenticator != null;
    }

    /**
     * Is the channel peer trusted?
     * A channel is trusted if the peer should be treated as authenticated.
     * The meaning of this is context dependent.  The channel will only be
     * trusted if the configured peer authenticator says it should be trusted,
     * so the creator of this SSLDataChannel knows what "trusted" means.
     *
     * @return true if the SSL peer should be trusted
     */
    @Override
    public boolean isTrusted() {
        return peerTrusted;
    }

    /**
     * Read data into the toFill data buffer.
     *
     * @param toFill the data buffer into which data will be read.  This buffer
     *        is expected to be ready for a put.  It need not be empty.
     * @return the count of bytes read into toFill.
     */
    @Override
    public int read(ByteBuffer toFill) throws IOException, SSLException {
        return (int) read(new ByteBuffer[] { toFill }, 0, 1);
    }

    @Override
    public long read(ByteBuffer[] toFill) throws IOException, SSLException {
        return read(toFill, 0, toFill.length);
    }

    @Override
    public long read(ByteBuffer toFill[], int offset, int length)
        throws IOException, SSLException {

        if ((offset < 0) ||
            (length < 0) ||
            (offset > toFill.length - length)) {
            throw new IndexOutOfBoundsException();
        }

        if (channelClosed) {
            throw new ClosedChannelException();
        }

        /*
         * Short-circuit if there's no work to be done at this time.  This
         * avoids an unnecessary read() operation from blocking.
         */
        int toFillRemaining = 0;
        for (int i = offset; i < offset + length; ++i) {
            toFillRemaining += toFill[i].remaining();
        }
        if (toFillRemaining <= 0) {
            return 0;
        }

        /*
         * In non-blocking mode, a preceding write operation might not have
         * completed.
         */
        if (!isBlocking()) {
            flush_internal();
        }

        /*
         * If we have data that is already unwrapped and ready to transfer, do
         * it now
         */
        readLock.lock();
        try {
            if (appRecvBuffer.position() > 0) {
                appRecvBuffer.flip();
                final int count = transfer(appRecvBuffer,
                        toFill, offset, length);
                appRecvBuffer.compact();
                return count;
            }
        } finally {
            readLock.unlock();
        }

        int readCount = 0;

        while (readCount == 0) {
            if (sslEngine.isInboundDone()) {
                return -1;
            }

            final HSProcStatus pstatus = processAnyHandshakes(false);

            /* See if we have unwrapped data available */
            readLock.lock();
            try {
                if (appRecvBuffer.position() > 0) {
                    appRecvBuffer.flip();
                    readCount = transfer(appRecvBuffer,
                            toFill, offset, length);
                    appRecvBuffer.compact();
                    break;
                }
            } finally {
                readLock.unlock();
            }

            if (sslEngine.getHandshakeStatus() ==
                HandshakeStatus.NOT_HANDSHAKING) {
                boolean progress = false;
                readLock.lock();
                try {
                    if (netRecvBuffer.position() > 0) {
                        /* There is some data in the network buffer that may be
                         * able to be unwrapped.  If so, we'll try to unwrap it.
                         * If that fails, then we may need more network data.
                         */
                        final int initialPos = netRecvBuffer.position();
                        netRecvBuffer.flip();
                        final SSLEngineResult engineResult =
                            sslEngine.unwrap(netRecvBuffer, appRecvBuffer);
                        netRecvBuffer.compact();

                        final int updatedPos = netRecvBuffer.position();
                        if (updatedPos != initialPos) {
                            /* We did something */
                            progress = true;
                        }

                        switch (engineResult.getStatus()) {
                        case BUFFER_UNDERFLOW:
                            /* Not enough data to do anything useful. */
                            break;

                        case BUFFER_OVERFLOW:
                            /* Shouldn't happen, but apparently there's not
                             * enough space in the application receive buffer */
                            throw new BufferOverflowException();

                        case CLOSED:
                            /* We apparently got a CLOSE_NOTIFY */
                            socketChannel.socket().shutdownInput();
                            break;

                        case OK:
                            break;
                        }
                    }

                    if (!progress) {
                        final int count = readFromChannel();

                        if (count < 0) {
                            readCount = count;
                        } else if (count == 0) {
                            /* Presumably we are in non-blocking mode */
                            break;
                        }
                    }
                } finally {
                    readLock.unlock();
                }
            } else {
                /*
                 * We just did handshake, but we are still in the handshaking
                 * state. The possible states are FINISHED, NEED_WRAP,
                 * NEED_TASK and NEED_UNWRAP. Here we need to make a decision
                 * whether to continue the loop for further handshaking or
                 * break. We do not want to break too early when we can process
                 * more handshakes; we also need to be cautious not to busy
                 * loop.
                 */
                if (isBlocking()) {
                    /*
                     * We cannot break the loop and exit for blocking channels.
                     * And this will not result in a busy loop since we will be
                     * blocked on socket read/write. So continue.
                     */
                    continue;
                }

                /* Non-blocking channel */
                if (pstatus == HSProcStatus.APP_WAIT) {
                    /*
                     * The appRecvBuffer was full when we did our handshake,
                     * but we transferred data out after that, so try again.
                     */
                    continue;
                }
                /*
                 * We have the following states here:
                 * - CONTENTION:
                 *   We cannot finish our handshake because the
                 *   netXmitBuffer is full, but we cannot flush it to the
                 *   transport because some one else is flushing. We should
                 *   stop here for two reasons: (1) we will not miss a
                 *   future read because after the other's flush the peer
                 *   will send us another handshake message and we will get
                 *   notified; (2) the other's flush can take some time so
                 *   we do not want to busy loop and wait for them.
                 * - SO_WAIT_READ:
                 *   Waiting on socket read, continue the loop will not make
                 *   any progress.
                 * - SO_WAIT_WRITE:
                 *   Waiting on socket write, continue the loop will not make
                 *   any progress.
                 * - AGAIN:
                 *   We should not be here
                 * - DONE:
                 *   We are done
                 */
                if (pstatus == HSProcStatus.AGAIN) {
                    throw new AssertionError();
                }
                break;
            }
        }

        if (readCount < 0) {
            /*
             * This will throw an SSLException if we haven't yet received a
             * close_notify.
             */
            sslEngine.closeInbound();
            sslInboundClosed = true;
        }

        if (sslEngine.isInboundDone()) {
            return -1;
        }

        return readCount;
    }

    @Override
    public int write(ByteBuffer toSend) throws IOException, SSLException {
        return (int) write(new ByteBuffer[] { toSend }, 0, 1);
    }

    @Override
    public long write(ByteBuffer[] toSend) throws IOException, SSLException {
        return write(toSend, 0, toSend.length);
    }

    @Override
    public long write(ByteBuffer[] toSend, int offset, int length)
        throws IOException, SSLException {

        if ((offset < 0) ||
            (length < 0) ||
            (offset > toSend.length - length)) {
            throw new IndexOutOfBoundsException();
        }

        if (channelClosed) {
            throw new ClosedChannelException();
        }

        int toSendRemaining = 0;
        for (int i = offset; i < offset + length; ++i) {
            toSendRemaining += toSend[i].remaining();
        }
        if (toSendRemaining == 0) {
            return 0;
        }
        final int toSendTotal = toSendRemaining;

        /*
         * Probably not needed, but just in case there's a backlog, start with
         * a flush to clear out the network transmit buffer.
         */
        flush_internal();

        while (true) {
            writeLock.lock();
            try {
                final SSLEngineResult engineResult =
                    sslEngine.wrap(toSend, offset, length, netXmitBuffer);

                toSendRemaining -= engineResult.bytesConsumed();

                switch (engineResult.getStatus()) {
                case BUFFER_OVERFLOW:
                    /*
                     * Although we are flushing as part of the loop, we can
                     * still receive this because flush_internal isn't
                     * guaranteed to flush everything.
                     */
                    break;

                case BUFFER_UNDERFLOW:
                    /* Should not be possible here */
                    throw new BufferUnderflowException();

                case CLOSED:
                    throw new SSLException(
                        "Attempt to write to a closed SSL Channel");

                case OK:
                    break;
                }
            } finally {
                writeLock.unlock();
            }

            processAnyHandshakes(false);
            flush_internal();

            if (toSendRemaining == 0 || !isBlocking()) {
                break;
            }
        }

        return toSendTotal - toSendRemaining;
    }

    /**
     * Attempt to flush any pending writes to the underlying socket buffer.
     * The caller should ensure that it is the only thread accessing the
     * DataChannel in order that the return value be meaningful.
     *
     * @return flush status
     */
    @Override
    public FlushStatus flush() throws IOException {

        return flush_internal();
    }

    /**
     * If any data is queued up to be sent in the network transmit buffer, try
     * to push it out.
     *
     * @return flush status
     */
    private FlushStatus flush_internal() throws IOException {
        /*
         * Don't insist on getting a lock.  If someone else has it, they will
         * probably flush it for us.
         */
        if (writeLock.tryLock()) {
            try {
                if (netXmitBuffer.position() == 0) {
                    /* We do not have any data to flush. */
                    return FlushStatus.DONE;
                }
                netXmitBuffer.flip();

                /*
                 * try/finally to keep things clean, in case the socket channel
                 * gets closed
                 */
                try {
                    /*
                     * Writing to the socket channel can block if this is a
                     * blocking channel and the OS buffer is full. This can
                     * pose a problem for close since we do not want the close
                     * method to block for a long time. Yet there seems to be
                     * no work-around in Java to set a write timeout for a
                     * blocking channel. We can only hope that the peer has
                     * responsive reads.
                     */
                    final int count = socketChannel.write(netXmitBuffer);

                    if (netXmitBuffer.remaining() == 0) {
                        /* Flushed everything, done. */
                        return FlushStatus.DONE;
                    }

                    if (count != 0) {
                        /*
                         * Still things to flush, and we flushed something.
                         * Flush again in case we can flush more next time.
                         */
                        return FlushStatus.AGAIN;
                    }

                    /*
                     * We have things to flush, but we did not flush, socket
                     * busy for write.
                     */
                    return FlushStatus.SO_WAIT_WRITE;

                } finally {
                    netXmitBuffer.compact();
                }
            } finally {
                writeLock.unlock();
            }
        }

        /* Failed to acquire the lock, contention. */
        return FlushStatus.CONTENTION;
    }

    @Override
    public void close() throws IOException, SSLException {
        try {
            ensureCloseForBlocking();

            final FlushStatus fstatus = flush_internal();
            if (fstatus == FlushStatus.CONTENTION) {
                throw new IOException("Concurrent operations during close");
            }

            final HSProcStatus pstatus;
            if (!sslEngine.isOutboundDone()) {
                sslEngine.closeOutbound();
                pstatus = processAnyHandshakes(true);
            } else if (!sslEngine.isInboundDone() && sslInboundClosed) {
                pstatus = processOneHandshake(true);
            } else {
                pstatus = HSProcStatus.DONE;
            }
            if (pstatus == HSProcStatus.CONTENTION) {
                throw new IOException("Concurrent operations during close");
            }
        } finally {
            try {
                socketChannel.close();
            } catch (IOException ioe) {
                /*
                 * Do a catch here so that we will not overwrite the real
                 * exception of the above code, if there were any, with this
                 * exception.
                 */
            } finally {
                channelClosed = true;
            }
        }
    }

    @Override
    public CloseAsyncStatus closeAsync() throws IOException {
        try {
            ensureCloseAsyncForNonBlocking();

            boolean continueFlush = true;
            while (continueFlush) {
                final FlushStatus fstatus =
                    flush_internal();
                switch (fstatus) {
                case CONTENTION:
                    throw new IOException(
                            "Concurrent write or flush operation " +
                            "during close async");
                case AGAIN:
                    break;
                case SO_WAIT_WRITE:
                    return CloseAsyncStatus.SO_WAIT_WRITE;
                case DISABLED:
                case DONE:
                    continueFlush = false;
                    break;
                default:
                    throw new AssertionError(
                            "Unknown flush status: " + fstatus);
                }
            }

            HSProcStatus pstatus = HSProcStatus.DONE;
            if (!sslEngine.isOutboundDone()) {
                sslEngine.closeOutbound();
                pstatus = processAnyHandshakes(true);
            } else if (!sslEngine.isInboundDone() && sslInboundClosed) {
                pstatus = processOneHandshake(true);
            }

            while (true) {
                switch (pstatus) {
                case CONTENTION:
                    throw new IOException(
                            "Concurrent operations during close async.");
                case DONE:
                    socketChannel.close();
                    return CloseAsyncStatus.DONE;
                case APP_WAIT:
                    /*
                     * For APP_WAIT state, we want the application to read out
                     * the buffer before we can proceed but that might not be
                     * possible, so just discard anything in the appRecvBuffer
                     * and let the handshake continue.
                     */
                    appRecvBuffer.clear();
                    pstatus = processAnyHandshakes(true);
                    continue;
                case SO_WAIT_READ:
                   return CloseAsyncStatus.SO_WAIT_READ;
                case SO_WAIT_WRITE:
                   return CloseAsyncStatus.SO_WAIT_WRITE;
                case AGAIN:
                   /*
                    * Should not be here since processAnyHandshakes loops on
                    * this condition
                    */
                   throw new AssertionError();
                default:
                   throw new AssertionError(
                           "Unknown handshake process status: " + pstatus);
                }
            }
        } catch (Throwable t) {
            try {
                socketChannel.close();
            } catch (IOException ioe) {
                /*
                 * Do a catch here so that we will not overwrite the real
                 * exception of the above code, if there were any, with this
                 * exception.
                 */
            } finally {
                if (t instanceof RuntimeException) {
                    throw (RuntimeException) t;
                } else if (t instanceof Error) {
                    throw (Error) t;
                } else if (t instanceof IOException) {
                    throw (IOException) t;
                }
                throw new RuntimeException(
                        "Unexpected exception: " + t.getMessage(), t);
            }
        } finally {
            channelClosed = true;
        }

    }

    @Override
    public void closeForcefully() throws IOException {
        try {
            sslEngine.closeOutbound();
            socketChannel.close();
        } finally {
            channelClosed = true;
        }
    }

    @Override
    public boolean isOpen() {
        if (!socketChannel.isOpen()) {
            return false;
        }
        return !channelClosed;
    }

    /**
     * Transfer as much data as possible from the src buffer to the dst
     * buffers.
     *
     * @param src the source ByteBuffer - it is expected to be ready for a get.
     * @param dsts the destination array of ByteBuffers, each of which is
     * expected to be ready for a put.
     * @param offset the offset within the buffer array of the first buffer
     * into which bytes are to be transferred.
     * @param length the maximum number of buffers to be accessed
     * @return The number of bytes transfered from src to dst
     */
    private int transfer(ByteBuffer src,
                         ByteBuffer[] dsts,
                         int offset,
                         int length) {

        int transferred = 0;
        for (int i = offset; i < offset + length; ++i) {
            final ByteBuffer dst = dsts[i];
            final int space = dst.remaining();

            if (src.remaining() > space) {
                /* not enough room for it all */
                final ByteBuffer slice = src.slice();
                slice.limit(space);
                dst.put(slice);
                src.position(src.position() + space);
                transferred += space;
            } else {
                transferred += src.remaining();
                dst.put(src);
                break;
            }
        }
        return transferred;
    }

    /**
     * Handshake process status.
     */
    private enum HSProcStatus {
        /* Processing stopped due to a concurrent competing operation */
        CONTENTION,

        /* Processing stopped due to waiting for socket read ready */
        SO_WAIT_READ,

        /* Processing stopped due to waiting for socket write ready */
        SO_WAIT_WRITE,

        /* Processing stopped but try again may make progress */
        AGAIN,

        /* Processing stopped expecting application to do operation */
        APP_WAIT,

        /* Processing stopped because handshake is done or channel closed */
        DONE,

    }

    /**
     * Repeatedly perform handshake operations while there is still more work
     * to do. That is, The method loops until processOneHandshake returns a
     * status other than HSProcStatus.AGAIN.
     *
     * @param isClosing {@code true} if the method is called during channel is
     * closing
     */
    private HSProcStatus processAnyHandshakes(boolean isClosing)
        throws IOException {

        while (true) {
            final HSProcStatus pstatus = processOneHandshake(isClosing);
            switch (pstatus) {
            case AGAIN:
                continue;
            case CONTENTION:
            case SO_WAIT_READ:
            case SO_WAIT_WRITE:
            case APP_WAIT:
            case DONE:
                return pstatus;
            default:
                throw new AssertionError("Unknown HSProcStatus: " + pstatus);
            }
        }
    }

    /**
     * Attempt a handshake step.
     *
     *
     * @param isClosing {@code true} if the method is called during channel is
     * closing
     * @return the status of the processing
     */
    private HSProcStatus processOneHandshake(boolean isClosing)
        throws IOException {

        int readCount = 0;
        FlushStatus fstatus = FlushStatus.DONE;
        SSLEngineResult engineResult = null;

        switch (sslEngine.getHandshakeStatus()) {
        case FINISHED:
            /*
             * Just finished handshaking. We shouldn't actually see this here
             * as it is only supposed to be produced by a wrap or unwrap.
             */
            return HSProcStatus.DONE;

        case NEED_TASK:
            /*
             * Need results from delegated tasks before handshaking can
             * continue, so do them now.  We assume that the tasks are done
             * inline, and so we can return AGAIN here.
             */
            runDelegatedTasks();
            return HSProcStatus.AGAIN;

        case NEED_UNWRAP:
            {
                boolean unwrapped = false;

                /* Attempt to flush anything that is pending */
                try {
                    flush_internal();
                } catch (SocketException se) {
                }

                /*
                 * Attempt to process anything that is pending in the
                 * netRecvBuffer.
                 */
                if (isClosing) {
                    /*
                     * Use try lock for close operations so that a read
                     * operation (with socketChannel#read in blocking mode)
                     * acquired the readLock will not block the close
                     * operation. [#26450]
                     */
                    if (!readLock.tryLock()) {
                        return HSProcStatus.CONTENTION;
                    }
                } else {
                    readLock.lock();
                }
                try {
                    if (netRecvBuffer.position() > 0) {
                        netRecvBuffer.flip();
                        engineResult =
                            sslEngine.unwrap(netRecvBuffer, appRecvBuffer);
                        netRecvBuffer.compact();
                        if (engineResult.getStatus() == Status.OK) {
                            unwrapped = true;
                        }
                    }

                    if (!unwrapped && !sslEngine.isInboundDone()) {
                        /*
                         * Either we had nothing in the netRecvBuffer or there
                         * was not enough data to unwrap, so let's try getting
                         * some more.
                         *
                         * If a re-negotiation is happening and the
                         * appRecvBuffer was full, we could have received a
                         * BUFFER_OVERFLOW engineResult, in which case a read()
                         * is not really helpful here, but it's harmless and is
                         * a rare occurrence, so we won't worry about it.
                         */
                        readCount = readFromChannel();
                        if (readCount < 0) {
                            try {
                                sslEngine.closeInbound();
                                sslInboundClosed = true;
                            } catch (SSLException ssle) {
                                // ignore
                            }
                        }

                        netRecvBuffer.flip();
                        engineResult =
                            sslEngine.unwrap(netRecvBuffer, appRecvBuffer);
                        netRecvBuffer.compact();
                    }
                } finally {
                    readLock.unlock();
                }
            }

            break;

        case NEED_WRAP:
            /*
             * Must send data to the remote side before handshaking can
             * continue, so wrap() must be called.
             */
            if (isClosing) {
                if (!writeLock.tryLock()) {
                    /*
                     * Use try lock for close operations so that a write
                     * operation (calling flush_internal with
                     * socketChannel#write in blocking mode) acquired the
                     * writeLock will not block the close operation. [#26450]
                     */
                    return HSProcStatus.CONTENTION;
                }
            } else {
                writeLock.lock();
            }
            try {
                engineResult = sslEngine.wrap(emptyXmitBuffer, netXmitBuffer);
            } finally {
                writeLock.unlock();
            }

            if (engineResult.getStatus() == SSLEngineResult.Status.CLOSED) {
                /*
                 * If the engine is already closed, flush may fail, and that's
                 * ok, so squash any exceptions that happen
                 */
                try {
                    /* ignore the flush status */
                    flush_internal();
                } catch (SocketException se) {
                }
            } else {
                fstatus = flush_internal();
            }
            break;

        case NOT_HANDSHAKING:
            /* Not currently handshaking */
            return HSProcStatus.DONE;
        }

        /*
         * We may have done a wrap or unwrap above.  Check the engineResult
         */

        if (engineResult != null) {
            if (engineResult.getHandshakeStatus() == HandshakeStatus.FINISHED) {
                /*
                 * Handshaking just completed.   Here is our chance to do any
                 * session validation that might be required.
                 */
                if (sslEngine.getUseClientMode()) {
                    if (hostVerifier != null) {
                        peerTrusted =
                            hostVerifier.verify(targetHost,
                                                sslEngine.getSession());
                        if (peerTrusted) {
                            logger.log(FINE,
                                          "SSL host verifier reports that " +
                                          "connection target is valid");
                        } else {
                            logger.log(INFO,
                                       "SSL host verifier reports that " +
                                       "connection target is NOT valid");
                            throw new IOException(
                                "Server identity could not be verified");
                        }
                    }
                } else {
                    if (authenticator != null) {
                        peerTrusted =
                            authenticator.isTrusted(sslEngine.getSession());
                        if (peerTrusted) {
                            logger.log(FINE,
                                       "SSL authenticator reports that " +
                                       "channel is trusted");
                        } else {
                            logger.log(INFO,
                                       "SSL authenticator reports that " +
                                       "channel is NOT trusted");
                        }
                    }
                }
            }

            switch (engineResult.getStatus()) {
            case BUFFER_UNDERFLOW:
                /*
                 * This must have resulted from an unwrap, meaning we need to
                 * do another read.  If the last read did something useful,
                 * tell the caller to call us again.
                 */
                if (readCount > 0) {
                    return HSProcStatus.AGAIN;
                } else {
                    return HSProcStatus.SO_WAIT_READ;
                }

            case BUFFER_OVERFLOW:
                /*
                 * Either we were processing an unwrap and the appRecvBuffer is
                 * full or we were processing a wrap and the netXmitBuffer is
                 * full.  For the unwrap case, the only way we can make progress
                 * is for the application to receive control.  For the wrap
                 * case, we may be able to make progress if the flush
                 * did something useful.
                 */
                if (sslEngine.getHandshakeStatus() ==
                    HandshakeStatus.NEED_UNWRAP) {
                    return HSProcStatus.APP_WAIT;
                }
                switch (fstatus) {
                case DISABLED:
                    throw new AssertionError();
                case DONE:
                case AGAIN:
                    return HSProcStatus.AGAIN;
                case SO_WAIT_WRITE:
                    return HSProcStatus.SO_WAIT_WRITE;
                case CONTENTION:
                    return HSProcStatus.CONTENTION;
                default:
                    throw new AssertionError(
                            "Unknown flush status: " + fstatus);
                }
            case CLOSED:
                if (sslEngine.isOutboundDone()) {
                    try {
                        socketChannel.socket().shutdownOutput();
                    } catch (Exception e) {
                    }
                }
                return HSProcStatus.DONE;

            case OK:
                break;
            }
        }

        /*
         * Tell the caller to try again.  Cases where no handshake progress
         * can be made should return another value above.
         */
        return HSProcStatus.AGAIN;
    }

    private void runDelegatedTasks() {
        Runnable task;
        /*
         * In theory, we could run these as a background job, but no need for
         * that level of complication.  Our server doesn't serve a large number
         * of clients.
         */
        while ((task = sslEngine.getDelegatedTask()) != null) {
            task.run();
        }
    }

    /**
     * Reads from the channel into netRecvBuffer.
     */
    private int readFromChannel() throws IOException {
        return wrappedReadChannel.read(netRecvBuffer);
    }

    /**
     * A null read channel that should not be used.
     */
    private static class NullReadChannel implements ReadableByteChannel {
        @Override
        public int read(ByteBuffer dst) {
            throw new IllegalStateException(
                    "Reading from a channel that should not be used. " +
                    "This indicates that a channel is switching " +
                    "between blocking and non-blocking mode " +
                    "while a concurrent read happens. " +
                    "We do not support such behavior");
        }

        @Override
        public void close() {

        }

        @Override
        public boolean isOpen() {
            return true;
        }
    }
}


