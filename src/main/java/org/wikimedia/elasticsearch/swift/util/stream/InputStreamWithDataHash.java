package org.wikimedia.elasticsearch.swift.util.stream;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.function.BiFunction;

public class InputStreamWithDataHash extends InputStream {
    private final InputStream innerStream;
    private final String dataHash;
    private String actualHash;
    private final BiFunction<String, String, Void> onHashMismatch;
    private final MessageDigest digest;
    private boolean isEof = false;

    public InputStreamWithDataHash(InputStream innerStream, String dataHash) {
        this.innerStream = innerStream;
        this.dataHash = dataHash;
        onHashMismatch = null;
        digest = DigestUtils.getMd5Digest();
    }

    public InputStreamWithDataHash(InputStream innerStream, String dataHash, BiFunction<String, String, Void> onHashMismatch) {
        this.innerStream = innerStream;
        this.dataHash = dataHash;
        this.onHashMismatch = onHashMismatch;
        digest = DigestUtils.getMd5Digest();
    }

    @Override
    public int read() throws IOException {
        final int result = innerStream.read();
        if (result >= 0){
            digest.update((byte) result);
            return result;
        }

        if (!isEof) {
            isEof = true;
            actualHash = Hex.encodeHexString(digest.digest());
        }

        if (!dataHash.equalsIgnoreCase(actualHash)){
            if (onHashMismatch != null){
                onHashMismatch.apply(dataHash, actualHash);
            }
            else {
                throw new IOException("Mismatched hash on stream data, expected [" + dataHash + "], actual [" + actualHash + "]");
            }
        }

        return result;
    }
}
