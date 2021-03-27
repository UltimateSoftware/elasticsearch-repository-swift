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
import org.javaswift.joss.instructions.UploadInstructions;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private final int retryCount;
    private final int shortOperationTimeoutS;
    private final int longOperationTimeoutS;
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

        withTimeoutFactory = new WithTimeout.Factory(logger);

        String pathString = path.buildAsString();
        keyPath = pathString.isEmpty() || pathString.endsWith("/") ? pathString : pathString + "/";

        boolean minimizeBlobExistsChecks = SwiftRepository.Swift.MINIMIZE_BLOB_EXISTS_CHECKS_SETTING.get(envSettings);
        blobExistsCheckAllowed = pathString.isEmpty() || !minimizeBlobExistsChecks;
        retryIntervalS = SwiftRepository.Swift.RETRY_INTERVAL_S_SETTING.get(envSettings);
        retryCount = SwiftRepository.Swift.RETRY_COUNT_SETTING.get(envSettings);
        shortOperationTimeoutS = SwiftRepository.Swift.SHORT_OPERATION_TIMEOUT_S_SETTING.get(envSettings);
        longOperationTimeoutS = SwiftRepository.Swift.LONG_OPERATION_TIMEOUT_S_SETTING.get(envSettings);
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
        final String objectName = buildKey(blobName);

        Object result = withTimeout().retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, retryCount,
            () -> SwiftPerms.execThrows(() -> {
                try {
                    StoredObject object = blobStore.getContainer().getObject(objectName);
                    long contentLength = object.getContentLength();
                    object.delete();
                    return new DeleteResult(1, contentLength);
                }
                catch (NotFoundException e) {
                    // this conversion is necessary for tests to run
                    String message = "object cannot be deleted, it does not exist [" + objectName + "]";
                    logger.warn(message);
                    NoSuchFileException e2 = new NoSuchFileException(message);
                    e2.initCause(e);
                    return e2;
                }
                catch (Exception e) {
                    logger.warn("object cannot be deleted [" + objectName + "]", e);
                    throw e;
                }
            }));

        if (result instanceof DeleteResult){
            return (DeleteResult) result;
        }
        throw (Exception) result;
    }

    @Override
    public DeleteResult delete() throws IOException {
        try {
            Container container = blobStore.getContainer();
            ContainerPaginationMap containerPaginationMap = new ContainerPaginationMap(container, keyPath, container.getMaxPageSize());
            Collection<StoredObject> containerObjects = withTimeout().retry(
                retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, retryCount,
                () -> SwiftPerms.exec( () -> {
                    try {
                        return containerPaginationMap.listAllItems();
                    }
                    catch (Exception e) {
                        logger.warn("cannot list items in [" + keyPath + "]", e);
                        throw e;
                    }
                }));

            DeleteResult results = DeleteResult.ZERO;

            for (StoredObject so: containerObjects) {
                final String blobName = so.getName().substring(keyPath.length());

                try {
                    long size = SwiftPerms.exec(so::getContentLength); // length already cached, no need to retry
                    deleteBlob(blobName); //retry happens inside the method
                    results = results.add(1, size);
                } catch (Exception e) {
                    logger.error("Cannot delete blob [" + blobName + "]", e);
                }
            }

            return results;
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
            Collection<DirectoryOrObject> directoryList = withTimeout().retry(
                retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, retryCount,
                () -> SwiftPerms.execThrows(() -> {
                    try {
                        return blobStore.getContainer().listDirectory(new Directory(directoryKey, '/'));
                    }
                    catch (Exception e) {
                        logger.warn("Cannot list blobs in directory [" + directoryKey + "]", e);
                        throw e;
                    }
                }));

            HashMap<String, PlainBlobMetaData> blobMap = new HashMap<>();

            for (DirectoryOrObject obj: directoryList) {
                if (obj.isObject()) {
                    String name = obj.getName().substring(keyPath.length());
                    Long length = withTimeout().retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, retryCount,
                        () -> SwiftPerms.exec(() -> {
                            try {
                                return obj.getAsObject().getContentLength();
                            }
                            catch (Exception e) {
                                logger.warn("Cannot get object [" + obj.getName() + "]", e);
                                throw e;
                            }
                        }));
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
            objects = withTimeout().retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, retryCount,
                    () -> SwiftPerms.execThrows(() -> {
                        try {
                            return blobStore.getContainer().listDirectory(new Directory(keyPath, '/'));
                        }
                        catch (Exception e) {
                            logger.warn("cannot list children for [" + keyPath + "]", e);
                            throw e;
                        }
                    }));
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

    private static class ObjectInfo {
        long size;
        InputStream stream;
        String tag;
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
            return withTimeout().retry(retryIntervalS, longOperationTimeoutS, TimeUnit.SECONDS, retryCount, () -> {
                try {
                    ObjectInfo object = getObjectInfo(objectName); //retries internally

                    if (streamRead) {
                        return wrapObjectStream(objectName, object);
                    }

                    // I/O operations are timed. May throw on hash mismatch
                    try {
                        return objectToReentrantStream(objectName, object.stream, object.size, object.tag);
                    } finally {
                        object.stream.close();
                    }
                }
                catch(Exception e){
                    logger.warn("failed to read object [" + objectName + "]", e);
                    throw e;
                }});
        }
        catch (IOException | RuntimeException e){
            throw e;
        }
        catch(Exception e) {
            throw new BlobStoreException("cannot read object [" + objectName + "]", e);
        }
    }

    /**
     * Write blob to Swift
     * @param blobName blob name
     * @param in in-memory stream with blob data. Do not rely on it staying open after this call!
     * @param blobSize blob size
     * @param failIfAlreadyExists throw exception if blob exists
     * @throws IOException can be thrown from various operations
     */
    @Override
    public void writeBlob(final String blobName,
                          final InputStream in,
                          final long blobSize,
                          boolean failIfAlreadyExists) throws IOException {
        if (executor != null && allowConcurrentIO && !streamWrite) {
            // async execution races against the InputStream closed in the caller. Read all data locally.
            InputStream capturedStream = streamToReentrantStream(in, blobSize, true, false);

            Future<Void> task = executor.submit(() -> {
                internalWriteBlob(blobName, capturedStream, blobSize, failIfAlreadyExists);
                return null;
            });

            repository.addWrite(blobName, task);
            return;
        }

        internalWriteBlob(blobName, in, blobSize, failIfAlreadyExists);
    }

    /**
     * Obtain metadata for named object, with retries and timeout
     * @param objectName object name
     * @return object metadata from Swift
     * @throws Exception on timeout, I/O errors
     */
    private ObjectInfo getObjectInfo(String objectName) throws Exception {
        Object result = withTimeout().retry(retryIntervalS, shortOperationTimeoutS, TimeUnit.SECONDS, retryCount,
            () -> SwiftPerms.execThrows(() -> {
                ObjectInfo objectInfo = null;
                try {
                    StoredObject storedObject = blobStore.getContainer().getObject(objectName);
                    objectInfo = new ObjectInfo();
                    objectInfo.stream = storedObject.downloadObjectAsInputStream();
                    objectInfo.size = storedObject.getContentLength();
                    objectInfo.tag = storedObject.getEtag();
                    return objectInfo;
                } catch (NotFoundException e) {
                    // this conversion is necessary for tests to pass
                    String message = "cannot read object, it does not exist [" + objectName + "]";
                    logger.warn(message);
                    NoSuchFileException e2 = new NoSuchFileException(message);
                    e2.initCause(e);
                    return e2;
                }
                catch (Exception e){
                    logger.warn("cannot read object [" + objectName + "]", e);
                    throw e;
                }
                finally {
                    if (objectInfo != null && objectInfo.stream != null){
                        objectInfo.stream.close();
                    }
                }
            }));

        if (result instanceof ObjectInfo){
            return (ObjectInfo) result;
        }

        throw (Exception) result;
    }

    private InputStreamWrapperWithDataHash wrapObjectStream(String objectName, ObjectInfo object) {
        if (logger.isDebugEnabled()){
            logger.debug("wrapping object in unbuffered stream [" + objectName + "], size=[" + object.size +
                    "], md5=[" + object.tag + "]");
        }
        return new InputStreamWrapperWithDataHash(objectName, object.stream, object.tag){
            @Override
            protected int innerRead(byte[] b) {
                try {
                    return withTimeout().timeout(shortOperationTimeoutS, TimeUnit.SECONDS, () -> super.innerRead(b));
                } catch (Exception e) {
                    try {
                        close();
                    } catch (IOException ioe) {
                        logger.error("Exception closing inner stream", ioe);
                    }
                    throw new BlobStoreException("failure reading from [" + objectName + "]", e);
                }
            }
        };
    }

    /**
     * Read object entirely into memory and check its etag. Elementary read operations are timed.
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
        InputStream objectStream = streamToReentrantStream(rawInputStream, size, false, true);
        String dataEtag = DigestUtils.md5Hex(objectStream);

        if (dataEtag.equalsIgnoreCase(objectEtag)) {
            objectStream.reset();
            if (logger.isDebugEnabled()) {
                logger.debug("read object into memory [" + objectName + "], size=[" + size + "]");
            }
            return objectStream;
        }

        String message = "cannot read object [" + objectName + "]: server etag [" + objectEtag +
                "] does not match calculated etag [" + dataEtag + "]";
        logger.warn(message);
        throw new BlobStoreException(message);
    }

    /**
     * If the original stream was re-entrant, do nothing; otherwise, read blob entirely into memory.
     *
     * @param in server input stream
     * @param sizeHint size hint, do not trust it
     * @param forceRead force reading into memory
     * @param useReadTimeout should read be timed? (only if actual I/O is happening, not memory duplication)
     * @return re-entrant stream holding the blob
     * @throws IOException on I/O error
     */
    private InputStream streamToReentrantStream(InputStream in,
                                                long sizeHint,
                                                boolean forceRead,
                                                boolean useReadTimeout) throws IOException {
        if (!forceRead && in.markSupported()) {
            return in;
        }

        final int bufferSize = (int) blobStore.getBufferSizeInBytes();
        final byte[] buffer = new byte[bufferSize];
        ByteArrayOutputStream baos = new ByteArrayOutputStream(sizeHint > 0 ? (int) sizeHint : bufferSize) {
            @Override
            public byte[] toByteArray() {
                return buf;
            }
        };

        while (true) {
            int read = useReadTimeout ? readWithTimeout(in, buffer) : in.read(buffer);
            if (read == -1){
                break;
            }
            baos.write(buffer, 0, read);
        }

        return new ByteArrayInputStream(baos.toByteArray());
    }

    private int readWithTimeout(InputStream in, final byte[] buffer) throws IOException {
        try {
            return withTimeout().timeout(shortOperationTimeoutS, TimeUnit.SECONDS, () -> in.read(buffer));
        }
        catch (IOException e){
            throw e;
        }
        catch(Exception e){
            throw new BlobStoreException("stream read failed", e);
        }
    }

    private void internalWriteBlob(String blobName, InputStream fromStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        final String objectName = buildKey(blobName);

        if (fromStream.markSupported()){
            fromStream.mark((int)blobSize);
        }

        try {
            IOException exception = withTimeout().retry(retryIntervalS, longOperationTimeoutS, TimeUnit.SECONDS, retryCount,
                    () -> SwiftPerms.execThrows(() -> {
                        try {
                            StoredObject object = blobStore.getContainer().getObject(objectName);

                            if (failIfAlreadyExists && blobExistsCheckAllowed && object.exists()) {
                                return new FileAlreadyExistsException("object [" + objectName + "] already exists, cannot overwrite");
                            }

                            UploadInstructions instructions = new UploadInstructions(fromStream);

                            if (fromStream.markSupported()){
                                String dataEtag = DigestUtils.md5Hex(fromStream);
                                instructions.setMd5(dataEtag);
                                fromStream.reset();
                            }

                            object.uploadObject(instructions);
                            if (logger.isDebugEnabled()){
                                logger.debug("uploaded object [" + objectName + "], size=[" + blobSize + "], md5=[" +
                                             instructions.getMd5() + "]");
                            }
                            return null;
                        }
                        catch (Exception e) {
                            logger.warn("cannot write object [" + objectName + "]", e);

                            if (fromStream.markSupported()){
                                fromStream.reset();
                            }

                            throw e;
                        }
                    }));

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
