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

package org.wikimedia.elasticsearch.swift.repositories.blobstore;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.DirectoryOrObject;
import org.wikimedia.elasticsearch.swift.SwiftPerms;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;
import org.wikimedia.elasticsearch.swift.util.retry.WithTimeout;

/**
 * Our blob store
 */
public class SwiftBlobStore implements BlobStore {
    private static final Logger logger = LogManager.getLogger(SwiftBlobStore.class);

    // How much to buffer our blobs by
    private final long bufferSizeInBytes;

    // Our Swift container. This is important.
    private final String containerName;

    private final Settings envSettings;
    private final Boolean allowConcurrentIO;

    public Settings getEnvSettings() {
        return envSettings;
    }

    private final AtomicReference<Container> container = new AtomicReference<>(null);

    private final Settings settings;
    private final Account auth;
    
    private final SwiftRepository repository;
    private final WithTimeout.Factory withTimeoutFactory;

    private final int retryIntervalS;
    private final int shortOperationTimeoutS;
    private final int retryCount;

    /**
     * Constructor. Sets up the container mostly.
     * @param repository owning repository
     * @param settings Settings for our repository. Only care about buffer size.
     * @param envSettings global settings
     * @param auth swift account info
     * @param containerName swift container
     */
    public SwiftBlobStore(SwiftRepository repository,
                          Settings settings,
                          Settings envSettings,
                          final Account auth,
                          final String containerName) {
        this.repository = repository;
        this.settings = settings;
        this.envSettings = envSettings;
        this.auth = auth;
        this.containerName = containerName;
        bufferSizeInBytes = settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).getBytes();
        withTimeoutFactory = new WithTimeout.Factory();
        retryIntervalS = SwiftRepository.Swift.RETRY_INTERVAL_S_SETTING.get(envSettings);
        retryCount = SwiftRepository.Swift.RETRY_COUNT_SETTING.get(envSettings);
        shortOperationTimeoutS = SwiftRepository.Swift.SHORT_OPERATION_TIMEOUT_S_SETTING.get(envSettings);
        allowConcurrentIO = SwiftRepository.Swift.ALLOW_CONCURRENT_IO_SETTING.get(envSettings);
    }

    private WithTimeout withTimeout(){
        return withTimeoutFactory.from(repository != null && allowConcurrentIO ? repository.threadPool() : null);
    }

    /**
     * Initialize container lazily. Do not produce a storm of Swift requests.
     * @return the container
     * @throws Exception from retry()
     */
    public Container getContainer() throws Exception {
        if (container.get() != null) {
            return container.get();
        }

        container.compareAndSet(null, internalGetContainer());
        return container.get();
    }

    private Container internalGetContainer() throws Exception {
        return withTimeout().retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, retryCount, () -> {
            try {
                return SwiftPerms.exec(() -> {
                    Container container = auth.getContainer(containerName);
                    if (!container.exists()) {
                        container.create();
                        container.makePublic();
                    }
                    return container;
                });
            }
            catch (Exception e) {
                logger.warn("cannot get container [" + containerName + "]", e);
                throw e;
            }
        });
    }

    /**
     * @return buffer size
     */
    public long getBufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    /**
     * Factory for getting blob containers for a path
     * @param path The blob path to search
     */
    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new SwiftBlobContainer(this, path);
    }

    //TODO method seems unused. Remove?
    private void deleteByPrefix(Collection<DirectoryOrObject> directoryOrObjects) {
        for (DirectoryOrObject directoryOrObject : directoryOrObjects) {
            if (directoryOrObject.isObject()) {
                directoryOrObject.getAsObject().delete();
            } else {
                deleteByPrefix(container.get().listDirectory(directoryOrObject.getAsDirectory()));
            }
        }
    }

    /**
     * Close the store. No-op for us.
     */
    @Override
    public void close() {
    }

    public Settings getSettings() {
        return settings;
    }

    public SwiftRepository getRepository() {
        return repository;
    }
}
