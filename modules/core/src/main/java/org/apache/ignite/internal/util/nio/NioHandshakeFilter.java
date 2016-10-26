/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.nio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Filter implements handshake in the following steps:
 * <ol>
 * <li>Client connects to remote node.</li>
 * <li>Remote node sends node ID to the client.</li>
 * <li>Client sends recovery descriptor to the remote node.</li>
 * <li>Remote node sends recovery count to the client node.</li>
 * </ol>
 */
public class NioHandshakeFilter extends GridNioFilterAdapter {
    /** Node ID meta key. */
    public static final int NODE_ID_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Node id future meta key. */
    public static final int NODE_ID_FUT_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Recovery future meta key. */
    public static final int RECOVERY_FUT_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Buffer meta key. */
    private static final int BUF_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param log Logger.
     */
    public NioHandshakeFilter(IgniteLogger log) {
        super("NioHandshakeFilter");

        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionOpened(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionClosed(ses);
    }

    /** {@inheritDoc} */
    @Override public void onExceptionCaught(
        GridNioSession ses,
        IgniteCheckedException ex
    ) throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws IgniteCheckedException {
        return proceedSessionWrite(ses, msg);
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        if (!(msg instanceof ByteBuffer))
            throw new GridNioException("Failed to read incoming message (incoming message is not a byte buffer, " +
                "is filter properly placed?): " + msg.getClass());

        if (!ses.accepted()) {
            GridNioFutureImpl<UUID> nodeIdFut = ses.meta(NODE_ID_FUT_META_KEY);

            if (nodeIdFut == null)
                ses.addMeta(NODE_ID_FUT_META_KEY, nodeIdFut = new GridNioFutureImpl<>());

            ByteBuffer input = (ByteBuffer)msg;

            if (!nodeIdFut.isDone()) {
                UUID rmtNodeId = ses.meta(NODE_ID_META_KEY);

                if (rmtNodeId == null) {
                    IgniteCheckedException e =
                        new IgniteCheckedException("Session should have expected remote node ID");

                    nodeIdFut.onDone(e);

                    throw e;
                }

                ByteBuffer buf = ses.meta(BUF_META_KEY);

                if (buf == null) {
                    buf = ByteBuffer.allocate(17);

                    buf.order(ByteOrder.nativeOrder());

                    ses.addMeta(BUF_META_KEY, buf);
                }

                int remaining = Math.min(buf.limit(), input.remaining());

                for (int i = buf.position(); i < remaining; i++)
                    buf.put(input.get());

                if (!buf.hasRemaining()) {
                    UUID rmtNodeId0 = U.bytesToUuid(buf.array(), 1);

                    if (!rmtNodeId.equals(rmtNodeId0)) {
                        IgniteCheckedException e =
                            new IgniteCheckedException("Remote node ID is not as expected [expected=" +  rmtNodeId +
                                ", rcvd=" + rmtNodeId0 + ']');

                        nodeIdFut.onDone(e);

                        throw e;
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Received remote node ID: " + rmtNodeId0);

                        nodeIdFut.onDone(rmtNodeId0);
                    }
                }

                return;
            }

            GridNioFutureImpl<Long> recoveryFut = ses.meta(RECOVERY_FUT_META_KEY);

            assert recoveryFut != null;

            if (!recoveryFut.isDone()) {
                ByteBuffer buf = ses.meta(BUF_META_KEY);

                assert buf != null;

                buf.clear();

                buf.limit(9);

                int remaining = Math.min(buf.limit(), input.remaining());

                for (int i = buf.position(); i < remaining; i++)
                    buf.put(input.get());

                if (!buf.hasRemaining())
                    recoveryFut.onDone(buf.getLong(1));
            }
            else
                proceedMessageReceived(ses, msg);
        }
        else
            proceedMessageReceived(ses, msg);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
        return proceedSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionWriteTimeout(ses);
    }
}
