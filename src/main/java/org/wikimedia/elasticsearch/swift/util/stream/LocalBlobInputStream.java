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

import org.elasticsearch.common.io.Channels;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LocalBlobInputStream extends InputStream {
    private final FileChannel channel;
    private long markedPos;

    public LocalBlobInputStream(Path path) throws IOException {
        channel = FileChannel.open(path, StandardOpenOption.READ);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return Channels.readFromFileChannel(channel, channel.position(), b, off, len);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1);
        int result = Channels.readFromFileChannel(channel, channel.position(), buf);
        return result == -1 ? -1 : buf.get();
    }

    @Override
    public long skip(long n) throws IOException {
        channel.position(channel.position() + n);
        return n;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        try {
            markedPos = channel.position();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        channel.position(markedPos);
    }
}
