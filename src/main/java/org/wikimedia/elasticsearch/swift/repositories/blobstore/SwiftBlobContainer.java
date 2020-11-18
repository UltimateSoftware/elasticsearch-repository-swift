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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.javaswift.joss.client.core.ContainerPaginationMap;
import org.javaswift.joss.exception.NotFoundException;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.Directory;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.StoredObject;
import org.wikimedia.elasticsearch.swift.SwiftPerms;
import org.wikimedia.elasticsearch.swift.util.retry.WithTimeout;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;
import org.wikimedia.elasticsearch.swift.util.stream.InputStreamWrapperWithDataHash;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
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
    private final boolean allowConcurrentIO;
    private final boolean streamRead;
    private final boolean streamWrite;

    private final ExecutorService executor;
    private final WithTimeout.Factory withTimeoutFactory;

    /**
     * Constructor
     * @param blobStore The blob store to use for operations
     * @param path The BlobPath to find blobs in
     */
    protected SwiftBlobContainer(SwiftBlobStore blobStore, BlobPath path) {
        super(path);
        this.blobStore = blobStore;
        repository = blobStore.getRepository();
        final Settings envSettings = blobStore.getEnvSettings();

        executor = repository != null ? repository.threadPool().executor(ThreadPool.Names.SNAPSHOT) : null;

        // if executor runs on 2 threads or less, using it will cause deadlock
        allowConcurrentIO = SwiftRepository.Swift.ALLOW_CONCURRENT_IO_SETTING.get(envSettings) &&
            executor instanceof ThreadPoolExecutor && ((ThreadPoolExecutor)executor).getMaximumPoolSize() > 2;

        withTimeoutFactory = new WithTimeout.Factory();

        String pathString = path.buildAsString();
        keyPath = pathString.isEmpty() || pathString.endsWith("/") ? pathString : pathString + "/";

        boolean minimizeBlobExistsChecks = SwiftRepository.Swift.MINIMIZE_BLOB_EXISTS_CHECKS_SETTING.get(envSettings);
        blobExistsCheckAllowed = pathString.isEmpty() || !minimizeBlobExistsChecks;
        retryIntervalS = SwiftRepository.Swift.RETRY_INTERVAL_S_SETTING.get(envSettings);
        shortOperationTimeoutS = SwiftRepository.Swift.SHORT_OPERATION_TIMEOUT_S_SETTING.get(envSettings);
        streamRead = SwiftRepository.Swift.STREAM_READ_SETTING.get(envSettings);
        streamWrite = SwiftRepository.Swift.STREAM_WRITE_SETTING.get(envSettings);
    }

    private WithTimeout withTimeout() {
        return withTimeoutFactory.from(repository != null && allowConcurrentIO ? repository.threadPool() : null);
    }

    /**
     * Delete a blob. Straightforward.
     * @param blobName A blob to delete
     */
    @Override
    public void deleteBlob(final String blobName) throws IOException {
        if (blobName.startsWith("tests-") && keyPath.isEmpty()){
            logger.info("ignoring deletion of pseudo-folder [" + blobName + "]");
            return;
        }

        if (executor != null && allowConcurrentIO) {
            Future<DeleteResult> task = executor.submit(() -> internalDeleteBlob(blobName));
            repository.addDeletion(blobName, task);
            return;
        }

        try {
            internalDeleteBlob(blobName);
        }
        catch (IOException | RuntimeException e){
            throw e;
        }
        catch (Exception e) {
            throw new BlobStoreException("cannot delete blob [" + blobName + "]", e);
        }
    }

    private DeleteResult internalDeleteBlob(String blobName) throws Exception {
        return withTimeout().retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, () -> {
            try {
                return SwiftPerms.execThrows(() -> {
                    StoredObject object = blobStore.getContainer().getObject(buildKey(blobName));
                    long contentLength = object.getContentLength();
                    object.delete();
                    return new DeleteResult(1, contentLength);
                });
            }
            catch (NotFoundException e) {
                String message = "Blob [" + buildKey(blobName) + "] cannot be deleted";
                logger.warn(message);
                NoSuchFileException e2 = new NoSuchFileException(message);
                e2.initCause(e);
                throw e2;
            }
        });
    }

    @Override
    public DeleteResult delete() throws IOException {
        try {
            Container container = blobStore.getContainer();
            ContainerPaginationMap containerPaginationMap = new ContainerPaginationMap(container, keyPath, container.getMaxPageSize());
            Collection<StoredObject> containerObjects = withTimeout().retry(retryIntervalS,
                shortOperationTimeoutS,
                TimeUnit.SECONDS,
                () -> {
                    try {
                        return SwiftPerms.exec(containerPaginationMap::listAllItems);
                    }
                    catch (Exception e) {
                        logger.warn("cannot list items in [" + keyPath + "]", e);
                        throw e;
                    }
                });

            DeleteResult results = DeleteResult.ZERO;
            ArrayList<Exception> errors = new ArrayList<>();

            for (StoredObject so: containerObjects) {
                try {
                    long size = SwiftPerms.exec(so::getContentLength); // length already cached, no need to retry
                    deleteBlob(so.getName().substring(keyPath.length())); //retry happens inside the method
                    results = results.add(1, size);
                } catch (Exception e) {
                    errors.add(e);
                }
            }

            if (errors.isEmpty()) {
                return results;
            }

            String message = errors.stream().map(Exception::getMessage).collect(Collectors.joining(","));
            throw new BlobStoreException("cannot delete blobs in path [" + keyPath + "]: " + message);
        }
        catch (IOException | RuntimeException e) {
           throw e;
        }
        catch (Exception e) {
            throw new BlobStoreException("cannot delete blobs in path [" + keyPath + "]", e);
        }
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
            Collection<DirectoryOrObject> directoryList = withTimeout().retry(retryIntervalS,
                shortOperationTimeoutS,
                TimeUnit.SECONDS,
                () -> {
                    try {
                        return SwiftPerms.execThrows(() -> blobStore.getContainer().listDirectory(new Directory(directoryKey, '/')));
                    }
                    catch (Exception e) {
                        logger.warn("Cannot list blobs in directory [" + directoryKey + "]", e);
                        throw e;
                    }
                });

            HashMap<String, PlainBlobMetaData> blobMap = new HashMap<>();

            for (DirectoryOrObject obj: directoryList) {
                if (obj.isObject()) {
                    String name = obj.getName().substring(keyPath.length());
                    Long length = withTimeout().retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, () -> {
                        try {
                            return SwiftPerms.exec(() -> obj.getAsObject().getContentLength());
                        }
                        catch (Exception e) {
                            logger.warn("Cannot get object [" + obj.getName() + "]", e);
                            throw e;
                        }
                    });
                    PlainBlobMetaData meta = new PlainBlobMetaData(name, length);
                    blobMap.put(name, meta);
                }
            }

            return Collections.unmodifiableMap(blobMap);
        }
        catch (IOException | RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new BlobStoreException("Cannot list blobs in directory [" + directoryKey + "]", e);
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
    public Map<String, BlobContainer> children() throws IOException{
        Collection<DirectoryOrObject> objects;
        try {
            objects = withTimeout().retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, () -> {
                try {
                    return SwiftPerms.execThrows(() -> blobStore.getContainer().listDirectory(new Directory(keyPath, '/')));
                }
                catch (Exception e) {
                    logger.warn("cannot list children for [" + keyPath + "]", e);
                    throw e;
                }
            });
        }
        catch (IOException | RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new BlobStoreException("cannot list children for [" + keyPath + "]", e);
        }

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
     * Fetch a given blob into memory, verify etag, and return InputStream.
     * @param blobName The blob name to read
     * @return a stream
     */
    @Override
    public InputStream readBlob(final String blobName) throws IOException {
        String objectName = buildKey(blobName);

        try {
            return withTimeout().retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, () -> {
                try {
                    return SwiftPerms.execThrows(() -> {
                        StoredObject storedObject = blobStore.getContainer().getObject(objectName);
                        InputStream rawInputStream = storedObject.downloadObjectAsInputStream();
                        String objectEtag = storedObject.getEtag();

                        return streamRead ? new InputStreamWrapperWithDataHash(objectName, rawInputStream, objectEtag)
                                : objectToReentrantStream(objectName, rawInputStream, storedObject.getContentLength(), objectEtag);
                    });
                }
                catch (NotFoundException e) {
                    String message = "cannot read blob [" + objectName + "]";
                    logger.warn(message);
                    NoSuchFileException e2 = new NoSuchFileException(message);
                    e2.initCause(e);
                    throw e2;
                }
            });
        }
        catch (IOException | RuntimeException e){
            throw e;
        }
        catch(Exception e) {
            throw new BlobStoreException("cannot read blob [" + objectName + "]", e);
        }
    }

    @Override
    public void writeBlob(final String blobName,
                          final InputStream in,
                          final long blobSize,
                          boolean failIfAlreadyExists) throws IOException {
        if (executor != null && allowConcurrentIO && !streamWrite) {
            // async execution races against the InputStream closed in the caller. Read all data locally.
            InputStream capturedStream = streamToReentrantStream(blobName, in, blobSize);

            Future<Void> task = executor.submit(() -> {
                internalWriteBlob(blobName, capturedStream, failIfAlreadyExists);
                return null;
            });

            repository.addWrite(blobName, task);
            return;
        }

        internalWriteBlob(blobName, in, failIfAlreadyExists);
    }


    /**
     * Read object entirely into memory and check its etag.
     * @param objectName full path to the object
     * @param rawInputStream server input stream
     * @param size size hint, do not trust it
     * @param objectEtag server etag (MD5 hash as hex)
     * @return object data as memory stream
     * @throws IOException on I/O errors or etag mismatch
     */
    private InputStream objectToReentrantStream(String objectName,
                                                InputStream rawInputStream,
                                                long size,
                                                String objectEtag) throws IOException {
        InputStream objectStream = streamToReentrantStream(objectName, rawInputStream, size);
        String dataEtag = DigestUtils.md5Hex(objectStream);

        if (dataEtag.equalsIgnoreCase(objectEtag)) {
            objectStream.reset();
            return objectStream;
        }

        String message = "cannot read blob [" + objectName + "]: server etag [" + objectEtag +
                "] does not match calculated etag [" + dataEtag + "]";
        logger.warn(message);
        throw new IOException(message);
    }


    /**
     * If the original stream was re-entrant, do nothing; otherwise, read blob entirely into memory.
     *
     * @param objectName object path
     * @param in server input stream
     * @param sizeHint size hint, do not trust it
     * @return re-entrant stream holding the blob
     * @throws IOException on I/O error
     */
    private InputStream streamToReentrantStream(final String objectName,
                                                InputStream in,
                                                final long sizeHint) throws IOException {
        if (in.markSupported()) {
            logger.debug("Reusing reentrant stream of class [" + in.getClass().getName() + "]");
            return in;
        }
        logger.debug("Reading blob ["+ objectName +"], expected size ["+ sizeHint + "] bytes");

        final int bufferSize = (int) blobStore.getBufferSizeInBytes();
        final byte[] buffer = new byte[bufferSize];
        long totalBytesRead = 0;
        int read;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(sizeHint > 0 ? (int) sizeHint : bufferSize) {
            @Override
            public synchronized byte[] toByteArray() {
                return buf;
            }
        };

        while ((read = in.read(buffer)) != -1) {
            totalBytesRead += read;
            if (sizeHint > 0 && totalBytesRead > sizeHint){
                logger.warn("Exceeded expected allocation : [" + objectName + "], totalBytesRead [" + totalBytesRead +
                            "] instead of [" + sizeHint + "]");
            }
            baos.write(buffer, 0, read);
        }

        return new ByteArrayInputStream(baos.toByteArray());
    }

    private void internalWriteBlob(String blobName, InputStream fromStream, boolean failIfAlreadyExists) throws IOException {
        final String objectName = buildKey(blobName);
        try {
            IOException exception = withTimeout().retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, () -> {
                try {
                    return SwiftPerms.execThrows(() -> {
                        StoredObject object = blobStore.getContainer().getObject(objectName);

                        if (failIfAlreadyExists && blobExistsCheckAllowed && object.exists()) {
                            return new FileAlreadyExistsException("object [" + objectName + "] already exists, cannot overwrite");
                        }

                        object.uploadObject(fromStream);
                        return null;
                    });
                }
                catch (Exception e) {
                    logger.warn("cannot write blob [" + objectName + "]", e);
                    throw e;
                }
            });

            if (exception != null) {
                throw exception;
            }
        }
        catch (IOException | RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new BlobStoreException("cannot write object [" + objectName + "]", e);
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
