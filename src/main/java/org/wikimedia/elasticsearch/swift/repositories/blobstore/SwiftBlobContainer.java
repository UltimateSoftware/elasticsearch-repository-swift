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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.javaswift.joss.client.core.ContainerPaginationMap;
import org.javaswift.joss.exception.NotFoundException;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.Directory;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.StoredObject;
import org.wikimedia.elasticsearch.swift.SwiftPerms;
import org.wikimedia.elasticsearch.swift.WithTimeout;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.FileAlreadyExistsException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Swift's implementation of the AbstractBlobContainer
 */
public class SwiftBlobContainer extends AbstractBlobContainer {
    private static final Logger logger = LogManager.getLogger(SwiftBlobContainer.class);

    // Our local swift blob store instance
    private final SwiftBlobStore blobStore;
    private final SwiftRepository repository;

    // The root path for blobs. Used by buildKey to build full blob names
    private final String keyPath;

    private final boolean blobExistsCheckAllowed;
    private final int retryIntervalS;
    private final int shortOperationTimeoutS;

    private final ExecutorService executor;

    /**
     * Constructor
     * @param blobStore The blob store to use for operations
     * @param path The BlobPath to find blobs in
     */
    protected SwiftBlobContainer(SwiftBlobStore blobStore, BlobPath path) {
        super(path);
        this.blobStore = blobStore;
        this.repository = blobStore.getRepository();
        this.executor = repository != null ? repository.threadPool().executor(ThreadPool.Names.SNAPSHOT) : null;

        String keyPath = path.buildAsString();
        this.keyPath = keyPath.isEmpty() || keyPath.endsWith("/") ? keyPath : keyPath + "/";

        boolean minimizeBlobExistsChecks = SwiftRepository.Swift.MINIMIZE_BLOB_EXISTS_CHECKS_SETTING.get(blobStore.getSettings());
        this.blobExistsCheckAllowed = keyPath.isEmpty() || !minimizeBlobExistsChecks;
        this.retryIntervalS = SwiftRepository.Swift.RETRY_INTERVAL_S.get(blobStore.getSettings());
        this.shortOperationTimeoutS = SwiftRepository.Swift.SHORT_OPERATION_TIMEOUT_S.get(blobStore.getSettings());
    }

    /**
     * Delete a blob. Straightforward.
     * @param blobName A blob to delete
     */
    @Override
    public void deleteBlob(final String blobName) throws IOException {
        if (executor == null) {
            try {
                internalDeleteBlob(blobName);
                return;
            }
            catch (IOException e){
                throw e;
            }
            catch (Exception e) {
                throw new IOException(e);
            }
        }

        Future<DeleteResult> task = executor.submit(() -> internalDeleteBlob(blobName));
        repository.addDeletion(blobName, task);
    }

    private DeleteResult internalDeleteBlob(String blobName) throws Exception {
        return new WithTimeout(repository).retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, () -> {
            try {
                return SwiftPerms.exec(() -> {
                    StoredObject object = blobStore.getContainer().getObject(buildKey(blobName));
                    long contentLength = object.getContentLength();
                    object.delete();
                    return new DeleteResult(1, contentLength);
                });
            }
            catch (NotFoundException e) {
                String message = "Blob [" + buildKey(blobName) + "] cannot be deleted";
                logger.warn(message, e);
                NoSuchFileException e2 = new NoSuchFileException(message);
                e2.initCause(e);
                throw e2;
            }
        });
    }

    @Override
    public DeleteResult delete() throws IOException {
        Collection<StoredObject> containerObjects = SwiftPerms.exec(() -> {
            Container container = blobStore.getContainer();
            ContainerPaginationMap containerPaginationMap = new ContainerPaginationMap(container, keyPath, container.getMaxPageSize());
            return containerPaginationMap.listAllItems();
        });

        DeleteResult results = DeleteResult.ZERO;
        ArrayList<Exception> errors = new ArrayList<>();

        for (StoredObject so: containerObjects) {
            try {
                long size = SwiftPerms.exec(so::getContentLength);
                deleteBlob(so.getName().substring(keyPath.length())); //SwiftPerms checked internally
                results = results.add(1, size);
            } catch (Exception e) {
                errors.add(e);
            }
        }

        if (errors.isEmpty()) {
            return results;
        }

        String message = errors.stream().map(Exception::getMessage).collect(Collectors.joining(","));
        throw new IOException(message);
    }

    /**
     * Get the blobs matching a given prefix
     * @param blobNamePrefix The prefix to look for blobs with
     * @return blobs metadata
     */
    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(@Nullable final String blobNamePrefix) throws IOException {
        String directoryKey = blobNamePrefix == null ? keyPath : buildKey(blobNamePrefix);
        try {
            Collection<DirectoryOrObject> directoryList = SwiftPerms.exec(() ->
                blobStore.getContainer().listDirectory(new Directory(directoryKey, '/'))
            );
            HashMap<String, PlainBlobMetaData> blobMap = new HashMap<>();

            for (DirectoryOrObject obj: directoryList) {
                if (obj.isObject()) {
                    String name = obj.getName().substring(keyPath.length());
                    Long length = SwiftPerms.exec(() -> obj.getAsObject().getContentLength());
                    PlainBlobMetaData meta = new PlainBlobMetaData(name, length);
                    blobMap.put(name, meta);
                }
            }

            return Collections.unmodifiableMap(blobMap);
        } catch (NotFoundException e) {
            NoSuchFileException e2 = new NoSuchFileException("Cannot list blobs in directory [" + directoryKey + "]");
            e2.initCause(e);
            throw e2;
        }
    }

    /**
     * Get all the blobs
     */
    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() {
        Collection<DirectoryOrObject> objects = SwiftPerms.exec(() -> blobStore.getContainer().listDirectory(new Directory(keyPath, '/')));
        HashMap<String, BlobContainer> blobMap = new HashMap<>();

        for (DirectoryOrObject obj: objects) {
            if (obj.isDirectory()){
                String name = obj.getBareName();
                BlobContainer blobContainer = blobStore.blobContainer(new BlobPath().add(obj.getName()));
                blobMap.put(name, blobContainer);
            }
        }

        return Collections.unmodifiableMap(blobMap);
    }

    /**
     * Build a key for a blob name, based on the keyPath
     * @param blobName The blob name to build a key for
     * @return the key
     */
    private String buildKey(String blobName) {
        return keyPath + blobName;
    }

    /**
     * Fetch a given blob into a BufferedInputStream
     * @param blobName The blob name to read
     * @return a stream
     */
    @Override
    public InputStream readBlob(final String blobName) throws IOException {
        try {
            return new WithTimeout(repository).retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, () -> {
                try {
                    InputStream downloadStream = SwiftPerms.exec(() ->
                        blobStore.getContainer().getObject(buildKey(blobName)).downloadObjectAsInputStream()
                    );
                    return new BufferedInputStream(downloadStream, (int) blobStore.getBufferSizeInBytes());
                }
                catch (NotFoundException e) {
                    String message = "Blob object [" + buildKey(blobName) + "] cannot be read";
                    logger.warn(message, e);
                    NoSuchFileException e2 = new NoSuchFileException(message);
                    e2.initCause(e);
                    throw e2;
                }
            });
        }
        catch (IOException e){
            throw e;
        }
        catch(Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeBlob(final String blobName,
                          final InputStream in,
                          final long blobSize,
                          boolean failIfAlreadyExists) throws IOException {
        if (executor == null) {
            internalWriteBlob(blobName, in, failIfAlreadyExists);
            return;
        }

        Future<Void> task = executor.submit(() -> {
            internalWriteBlob(blobName, in, failIfAlreadyExists);
            return null;
        });

        repository.addWrite(blobName, task);
    }

    private void internalWriteBlob(String blobName, InputStream in, boolean failIfAlreadyExists) throws IOException {
        try {
            SwiftPerms.execThrows(() -> {
                StoredObject blob = blobStore.getContainer().getObject(buildKey(blobName));

                if (failIfAlreadyExists && blobExistsCheckAllowed && blob.exists()) {
                    throw new FileAlreadyExistsException("blob [" + buildKey(blobName) + "] already exists, cannot overwrite");
                }

                blob.uploadObject(in);
            });

        }
        catch (IOException | RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeBlobAtomic(String blobName,
                                InputStream inputStream,
                                long blobSize,
                                boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }
}
