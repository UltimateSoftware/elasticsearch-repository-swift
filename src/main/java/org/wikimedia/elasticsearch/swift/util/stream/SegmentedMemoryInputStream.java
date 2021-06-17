/*
 * Copyright 2017 Wikimedia and BigData Boutique
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.elasticsearch.swift.util.stream;

import java.io.IOException;
import java.io.InputStream;

public class SegmentedMemoryInputStream extends InputStream {
    private final SegmentedBuffer buffer;
    long markedPos = 0;
    boolean closed = false;

    public SegmentedMemoryInputStream(SegmentedMemoryOutputStream mos) {
        buffer = mos.getBuffer();
        buffer.setPos(0);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        markedPos = buffer.getPos();
    }

    @Override
    public void reset() throws IOException {
        checkClosed();
        buffer.setPos(markedPos);
    }

    @Override
    public int available() throws IOException {
        checkClosed();
        long available = buffer.available();
        return available > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) available;
    }

    private void checkClosed() throws IOException {
        if (closed){
            throw new IOException("stream is closed");
        }
    }

    @Override
    public int read() throws IOException {
        checkClosed();
        return buffer.get();
    }

    @Override
    public int read(byte[] b) throws IOException {
        checkClosed();
        return buffer.get(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkClosed();
        return buffer.get(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        checkClosed();
        long skip = Math.min(n, buffer.available());
        buffer.setPos(buffer.getPos()+skip);
        return skip;
    }
}
