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
package org.wikimedia.elasticsearch.swift;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.ESBlobStoreContainerTestCase;
import org.javaswift.joss.client.mock.AccountMock;
import org.javaswift.joss.exception.NotFoundException;
import org.javaswift.joss.swift.Swift;
import org.junit.Before;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;
import org.wikimedia.elasticsearch.swift.repositories.blobstore.SwiftBlobStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Locale;
import java.util.Map;

public class SwiftBlobContainerTests extends ESBlobStoreContainerTestCase {
    private Swift swift;
    private AccountMock account;
    private Settings blobStoreSettings;
    private Settings globalSettings;

    @Before
    public void setup() {
        this.swift = new Swift();
        this.account = new AccountMock(swift);
        blobStoreSettings = Settings.EMPTY;
        globalSettings = Settings.EMPTY;
    }

    @Override
    protected BlobStore newBlobStore() {
        String container = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        return new SwiftBlobStore(null, blobStoreSettings, globalSettings, this.account, container);
    }

    public void testCommandExceptionDuringRead() throws IOException {
        try(BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath());
            expectThrows(NoSuchFileException.class, NotFoundException.class,
                         () -> container.readBlob("foobar"));
        }
    }

    public void testBlobExistsCheckAllowed() throws IOException {
        globalSettings = Settings.builder()
            .put(SwiftRepository.Swift.MINIMIZE_BLOB_EXISTS_CHECKS_SETTING.getKey(), false)
            .build();

        try(BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath().add("/path"));
            final int blobSize = 8;
            try(InputStream in = new ByteArrayInputStream(randomByteArrayOfLength(blobSize))){
                container.writeBlob("blob", in, blobSize, true);
                expectThrows(FileAlreadyExistsException.class, () -> container.writeBlob("blob", in, blobSize, true));
            }
        }
    }

    public void testBlobExistsCheckNotAllowed() throws IOException {
        try(BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath().add("/path"));
            final int blobSize = 8;
            try(InputStream in = new ByteArrayInputStream(randomByteArrayOfLength(blobSize))){
                container.writeBlob("blob", in, blobSize, true);
                container.writeBlob("blob", in, blobSize, true);
            }
        }
    }

    public void testDeleteReturnsDeleteResult() throws IOException {
        try(BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath().add("/path"));
            DeleteResult result = container.delete();

            assertNotNull(result);
        }
    }

    public void testChildrenReturnsMap() throws IOException {
        try(BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath().add("/path"));
            Map<String, BlobContainer> result = container.children();

            assertNotNull(result);
        }
    }
}
