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

package org.apache.ignite.internal.jdbc2;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;

/**
 *
 */
public class JdbcBlob implements Blob {
    /** Byte buffer. */
    private final ByteBuffer bb;

    /**
     * @param arr Array.
     */
    public JdbcBlob(byte[] arr) {
        this.bb = ByteBuffer.wrap(arr);
    }

    /** {@inheritDoc} */
    @Override public long length() throws SQLException {
        return bb.capacity();
    }

    /** {@inheritDoc} */
    @Override public byte[] getBytes(long pos, int len) throws SQLException {
        return bb.array();
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(bb.array());
    }

    /** {@inheritDoc} */
    @Override public long position(byte[] ptrn, long start) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long position(Blob ptrn, long start) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int setBytes(long pos, byte[] bytes) throws SQLException {
        if (pos < 0 || pos > Integer.MAX_VALUE)
            throw new IllegalArgumentException();

        bb.position((int)pos);

        bb.put(bytes, 0, bytes.length);

        return bytes.length;
    }

    /** {@inheritDoc} */
    @Override public int setBytes(long pos, byte[] bytes, int off, int len) throws SQLException {
        if (pos < 0 || pos > Integer.MAX_VALUE)
            throw new IllegalArgumentException();

        bb.position((int)pos);

        bb.put(bytes, off, len);

        return len;
    }

    /** {@inheritDoc} */
    @Override public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void truncate(long len) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void free() throws SQLException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream(long pos, long len) throws SQLException {
        throw new UnsupportedOperationException("getBinaryStream(long pos, long len)");
    }
}
