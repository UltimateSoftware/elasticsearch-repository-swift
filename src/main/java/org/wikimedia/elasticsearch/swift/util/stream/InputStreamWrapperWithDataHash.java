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
import java.util.function.BiFunction;

public class InputStreamWrapperWithDataHash extends InputStream {
    private final String objectName;
    private final InputStream innerStream;
    private final String dataHash;
    private String actualDataHash;
    private final BiFunction<String, String, Void> onHashMismatch;
    private final MessageDigest digest = DigestUtils.getMd5Digest();
    private boolean isEof = false;

    public InputStreamWrapperWithDataHash(String objectName, InputStream innerStream, String dataHash) {
        this.objectName = objectName;
        this.innerStream = innerStream;
        this.dataHash = dataHash;
        onHashMismatch = null;
    }

    public InputStreamWrapperWithDataHash(String objectName, InputStream innerStream, String dataHash,
                                          BiFunction<String, String, Void> onHashMismatch) {
        this.objectName = objectName;
        this.innerStream = innerStream;
        this.dataHash = dataHash;
        this.onHashMismatch = onHashMismatch;
    }

    @Override
    public int read() throws IOException {
        final int result = innerStream.read();
        if (result != -1){
            digest.update((byte) result);
            return result;
        }

        if (!isEof) {
            isEof = true;
            actualDataHash = Hex.encodeHexString(digest.digest());
        }

        if (!dataHash.equalsIgnoreCase(actualDataHash)){
            if (onHashMismatch != null){
                onHashMismatch.apply(dataHash, actualDataHash);
            }
            else {
                throw new IOException("Mismatched hash on stream for [" + objectName + "], expected [" + dataHash +
                                      "], actual [" + actualDataHash + "]");
            }
        }

        return result;
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
