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

    public InputStreamWrapperWithDataHash(String objectName, InputStream innerStream, String dataHash) {
        this.objectName = objectName;
        this.innerStream = innerStream;
        this.dataHash = dataHash;
    }

    /**
     * Elasticsearch favors this method. Route it through read(byte[]), due to latter implemented in JOSS.
     * @param b buffer
     * @param off offset in b
     * @param len max length to read
     * @return bytes actually read or -1 on EOF
     * @throws IOException on error in wrapped stream or md5 mismatch
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (off == 0 && b.length == len){
            return read(b);
        }

        byte[] buffer = new byte[Math.min(b.length-off, len)];
        int bytesRead = read(buffer);
        if (bytesRead > 0){
            System.arraycopy(buffer, 0, b, off, bytesRead);
        }
        return bytesRead;
    }

    /**
     * This method maps into JOSS implementation, which is lacking read(byte[], int, int) method implementation.
     * @param b byte buffer to read into
     * @return bytes read, -1 on EOF
     * @throws IOException on I/O error or md5 mismatch
     */
    @Override
    public int read(byte[] b) throws IOException {
        final int bytesRead = innerRead(b);
        if (bytesRead != -1){
            digest.update(b, 0, bytesRead);
            return bytesRead;
        }

        // EOF
        if (actualDataHash == null) {
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
     * Do NOT call innerRead() here, instead route the logic through read(byte[]).
     * This method does not appear to be called.
     * @return byte read or -1 on EOF
     * @throws IOException on I/O error or md5 mismatch
     * @see InputStream#read()
     */
    @Override
    public int read() throws IOException {
        byte[] b=new byte[1];
        return read(b) == -1 ? -1 : b[0]&0xff;
    }

    /**
     * Invoke read(byte[]) on inner stream(implemented by JOSS).
     * Close inner stream as soon as EOF is reached.
     * @param b byte buffer
     * @return bytes actually read, or -1 on EOF
     * @throws IOException on I/O error
     * @see     InputStream#read(byte[])
     */
    protected int innerRead(byte[] b) throws IOException {
        int bytesRead = innerStream.read(b);
        if (bytesRead == -1){
            innerStream.close();
        }
        return bytesRead;
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
