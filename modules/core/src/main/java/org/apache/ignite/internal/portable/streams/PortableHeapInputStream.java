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

package org.apache.ignite.internal.portable.streams;

import java.util.Arrays;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Portable off-heap input stream.
 */
public final class PortableHeapInputStream extends PortableAbstractInputStream {
    /**
     * Create stream with pointer set at the given position.
     *
     * @param data Data.
     * @param pos Position.
     * @return Stream.
     */
    public static PortableHeapInputStream create(byte[] data, int pos) {
        assert pos < data.length;

        PortableHeapInputStream stream = new PortableHeapInputStream(data);

        stream.pos = pos;

        return stream;
    }

    /** Data. */
    private byte[] data;

    /**
     * Constructor.
     *
     * @param data Data.
     */
    public PortableHeapInputStream(byte[] data) {
        this.data = data;

        len = data.length;
    }

    /**
     * @return Copy of this stream.
     */
    public PortableHeapInputStream copy() {
        PortableHeapInputStream in = new PortableHeapInputStream(Arrays.copyOf(data, data.length));

        in.position(pos);

        return in;
    }

    /**
     * Method called from JNI to resize stream.
     *
     * @param len Required length.
     * @return Underlying byte array.
     */
    public byte[] resize(int len) {
        if (data.length < len) {
            byte[] data0 = new byte[len];

            GridUnsafe.copyMemory(data, GridUnsafe.BYTE_ARR_OFF, data0, GridUnsafe.BYTE_ARR_OFF, data.length);

            data = data0;
        }

        return data;
    }

    /** {@inheritDoc} */
    @Override public int remaining() {
        return data.length - pos;
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        byte[] res = new byte[len];

        GridUnsafe.copyMemory(data, GridUnsafe.BYTE_ARR_OFF, res, GridUnsafe.BYTE_ARR_OFF, res.length);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected byte readByteAndShift() {
        return data[pos++];
    }

    /** {@inheritDoc} */
    @Override protected void copyAndShift(Object target, long off, int len) {
        GridUnsafe.copyMemory(data, GridUnsafe.BYTE_ARR_OFF + pos, target, off, len);

        shift(len);
    }

    /** {@inheritDoc} */
    @Override protected short readShortFast() {
        return GridUnsafe.getShortAligned(data, GridUnsafe.BYTE_ARR_OFF + pos);
    }

    /** {@inheritDoc} */
    @Override protected char readCharFast() {
        return GridUnsafe.getCharAligned(data, GridUnsafe.BYTE_ARR_OFF + pos);
    }

    /** {@inheritDoc} */
    @Override protected int readIntFast() {
        return GridUnsafe.getIntAligned(data, GridUnsafe.BYTE_ARR_OFF + pos);
    }

    /** {@inheritDoc} */
    @Override protected long readLongFast() {
        return GridUnsafe.getLongAligned(data, GridUnsafe.BYTE_ARR_OFF + pos);
    }

    /** {@inheritDoc} */
    @Override protected byte readBytePositioned0(int pos) {
        return GridUnsafe.getByte(data, GridUnsafe.BYTE_ARR_OFF + pos);
    }

    /** {@inheritDoc} */
    @Override protected short readShortPositioned0(int pos) {
        short res = GridUnsafe.getShortAligned(data, GridUnsafe.BYTE_ARR_OFF + pos);

        if (!LITTLE_ENDIAN)
            res = Short.reverseBytes(res);

        return res;
    }

    /** {@inheritDoc} */
    @Override protected int readIntPositioned0(int pos) {
        int res = GridUnsafe.getIntAligned(data, GridUnsafe.BYTE_ARR_OFF + pos);

        if (!LITTLE_ENDIAN)
            res = Integer.reverseBytes(res);

        return res;
    }
}