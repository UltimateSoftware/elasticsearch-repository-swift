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
import java.io.OutputStream;

public class SegmentedMemoryOutputStream extends OutputStream {
    private final SegmentedBuffer buffer;
    private boolean closed = false;

    public SegmentedMemoryOutputStream() {
        buffer = new SegmentedBuffer();
    }

    public SegmentedMemoryOutputStream(long sizeHint) {
        buffer = new SegmentedBuffer(sizeHint);
    }

    @Override
    public void close() {
        closed = true;
    }

    private void checkClosed() throws IOException {
        if (closed){
            throw new IOException("stream closed");
        }
    }

    // keep the method package private
    SegmentedBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void write(int b) throws IOException {
        checkClosed();
        buffer.put((byte) b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        checkClosed();
        int written = 0;

        while (written < len) {
            written += buffer.put(b, off+written, len-written);
        }
    }
}
