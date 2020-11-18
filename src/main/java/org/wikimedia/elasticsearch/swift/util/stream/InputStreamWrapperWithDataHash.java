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

    public InputStreamWrapperWithDataHash(String objectName, InputStream innerStream, String dataHash, BiFunction<String, String, Void> onHashMismatch) {
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
                throw new IOException("Mismatched hash on stream for [" + objectName + "], expected [" + dataHash + "], actual [" + actualDataHash + "]");
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
