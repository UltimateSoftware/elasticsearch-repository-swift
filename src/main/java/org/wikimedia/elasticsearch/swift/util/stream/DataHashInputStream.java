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
import org.apache.commons.io.input.ProxyInputStream;
import org.elasticsearch.common.blobstore.BlobStoreException;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

public class DataHashInputStream extends ProxyInputStream {
    private final String objectName;
    private final String dataHash;
    private String actualDataHash;
    private final MessageDigest digest = DigestUtils.getMd5Digest();

    public DataHashInputStream(String objectName, InputStream jossStream, String dataHash) {
        super(jossStream);
        this.objectName = objectName;
        this.dataHash = dataHash;
    }

    private void digestAfterRead(int b) {
        if (b == -1) {
            onEof();
        }
        else{
            digest.update((byte)b);
        }
    }

    private void digestAfterRead(final byte[] buf, final int off, final int len) {
        if (len == -1) {
            onEof();
        }
        else {
            digest.update(buf, off, len);
        }
    }

    private void onEof() {
        if (actualDataHash == null) {
            actualDataHash = Hex.encodeHexString(digest.digest());
        }

        if (!dataHash.equalsIgnoreCase(actualDataHash)) {
            throw new BlobStoreException("Mismatched hash on stream for [" + objectName + "], expected [" + dataHash +
                "], actual [" + actualDataHash + "]");
        }
    }

    @Override
    protected void handleIOException(IOException e) {
        throw new BlobStoreException("failure reading from [" + objectName + "]", e);
    }

    @Override
    public int read() throws IOException {
        final int result = super.read();
        digestAfterRead(result);
        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        final int bytesRead = super.read(b);
        digestAfterRead(b, 0, bytesRead);
        return bytesRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        final int bytesRead = super.read(b, off, len);
        digestAfterRead(b, off, bytesRead);
        return bytesRead;
    }
}
