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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

public class InputStreamWrapperWithDataHash extends InputStream {
    private final String objectName;
    private final InputStream innerStream;
    private final String dataHash;
    private String actualDataHash;
    private final MessageDigest digest = DigestUtils.getMd5Digest();
    private boolean isEof = false;

    public InputStreamWrapperWithDataHash(String objectName, InputStream innerStream, String dataHash) {
        this.objectName = objectName;
        this.innerStream = innerStream;
        this.dataHash = dataHash;
    }

    /**
     * Elasticsearch favors this method
     * @param b buffer
     * @param off offset in b
     * @param len max length to read
     * @return bytes actually read or -1 on EOF
     * @throws IOException on error in wrapped stream or md5 mismatch
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        final int bytesRead = innerRead(b, off, len);
        if (bytesRead != -1){
            digest.update(b, off, bytesRead);
            return bytesRead;
        }

        if (!isEof) {
            isEof = true;
            actualDataHash = Hex.encodeHexString(digest.digest());
        }

        if (!dataHash.equalsIgnoreCase(actualDataHash)) {
            throw new IOException("Mismatched hash on stream for [" + objectName + "], expected [" + dataHash +
                    "], actual [" + actualDataHash + "]");
        }

        return bytesRead;
    }

    /**
     * All other read methods in <code>InputStream</code> default to abstract read().
     * DO NOT call innerRead() here, instead route the logic through read(byte[], int, int).
     * This method does not appear to be called.
     * @return byte read or -1 on EOF
     * @throws IOException on I/O error or md5 mismatch
     * @see InputStream#read()
     */
    @Override
    public int read() throws IOException {
        byte[] b=new byte[1];
        int result = read(b, 0, 1);
        return result == -1 ? -1 : b[0];
    }

    /**
     * Invoke read(byte[], int, int) on inner stream. DO NOT implement overloads.
     * @param b byte buffer
     * @param off offset in buffer
     * @param len max length to read
     * @return bytes actually read, or -1 on EOF
     * @throws IOException on I/O error
     * @see     InputStream#read(byte[], int, int)
     */
    protected int innerRead(byte[] b, int off, int len) throws IOException {
        return innerStream.read(b, off, len);
    }

    @Override
    public int available() throws IOException {
        return innerStream.available();
    }

    @Override
    public void close() throws IOException {
        innerStream.close();
    }
}
