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

package org.wikimedia.elasticsearch.swift.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.threadpool.ThreadPool;
import org.javaswift.joss.model.Account;
import org.wikimedia.elasticsearch.swift.repositories.blobstore.SwiftBlobStore;

import java.util.List;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * The blob store repository. A glorified settings wrapper.
 */
public class SwiftRepository extends BlobStoreRepository {
    // The internal "type" for Elasticsearch
    public static final String TYPE = "swift";

    /**
     * Swift repository settings
     */
    public interface Swift {
        Setting<String> CONTAINER_SETTING = Setting.simpleString("swift_container");
        Setting<String> URL_SETTING = Setting.simpleString("swift_url");
        Setting<String> AUTHMETHOD_SETTING = Setting.simpleString("swift_authmethod");
        Setting<String> PASSWORD_SETTING = Setting.simpleString("swift_password");
        Setting<String> TENANTNAME_SETTING = Setting.simpleString("swift_tenantname");
        Setting<String> USERNAME_SETTING = Setting.simpleString("swift_username");
        Setting<String> PREFERRED_REGION_SETTING = Setting.simpleString("swift_preferred_region");
        Setting<String> AUTHSCOPE_SETTING = Setting.simpleString("swift_authscope");
        Setting<String> AUTHDOMAIN_SETTING = Setting.simpleString("swift_authdomain");
        Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting("chunk_size",
                                                                            new ByteSizeValue(5, ByteSizeUnit.GB));
        Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false);
        Setting<Boolean> MINIMIZE_BLOB_EXISTS_CHECKS_SETTING = Setting.boolSetting("repository_swift.minimize_blob_exists_checks",
                                                                                   true,
                                                                                    Setting.Property.NodeScope);
        Setting<Boolean> ALLOW_CACHING_SETTING = Setting.boolSetting("repository_swift.allow_caching",
                                                                     true,
                                                                     Setting.Property.NodeScope);

        Setting<Long> DELETE_TIMEOUT_MIN_SETTING = Setting.longSetting("repository_swift.delete_timeout_min",
                60,
                0,
                Setting.Property.NodeScope);

        Setting<Integer> SNAPSHOT_TIMEOUT_MIN_SETTING = Setting.intSetting("repository_swift.snapshot_timeout_min",
                360,
                Setting.Property.NodeScope);

        Setting<Integer> SHORT_OPERATION_TIMEOUT_S_SETTING = Setting.intSetting("repository_swift.short_operation_timeout_s",
                30,
                Setting.Property.NodeScope);

        Setting<Integer> RETRY_INTERVAL_S_SETTING = Setting.intSetting("repository_swift.retry_interval_s",
                10,
                Setting.Property.NodeScope);

        Setting<Boolean> ALLOW_CONCURRENT_IO_SETTING = Setting.boolSetting("repository_swift.allow_concurrent_io",
                true,
                Setting.Property.NodeScope);
    }

    private static final Logger logger = LogManager.getLogger(SwiftRepository.class);

    // Base path for blobs
    private final BlobPath basePath;

    // Chunk size.
    private final ByteSizeValue chunkSize;

    private final Settings settings;

    private final Settings envSettings;

    public Settings getSettings() {
        return settings;
    }

    public Settings getEnvSettings() {
        return envSettings;
    }
    private final SwiftService swiftService;

    private final ConcurrentHashMap<String, Future<DeleteResult>> blobDeletionTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Future<Void>> blobWriteTasks = new ConcurrentHashMap<>();

    /**
     * Constructs new BlobStoreRepository
     *
     * @param metadata
     *            repository meta data
     * @param settings
     *            repo settings
     * @param envSettings
     *            global settings
     * @param namedXContentRegistry
     *            an instance of NamedXContentRegistry
     * @param swiftService
     *            an instance of SwiftService
     * @param threadPool
     *            an elastic search ThreadPool
     */
    @Inject
    public SwiftRepository(final RepositoryMetaData metadata,
                           final Settings settings,
                           final Settings envSettings,
                           final NamedXContentRegistry namedXContentRegistry,
                           final SwiftService swiftService,
                           final ThreadPool threadPool) {
        super(metadata, Swift.COMPRESS_SETTING.get(settings), namedXContentRegistry, threadPool);
        this.settings = settings;
        this.envSettings = envSettings;
        this.swiftService = swiftService;
        this.chunkSize = Swift.CHUNK_SIZE_SETTING.get(settings);
        this.basePath = BlobPath.cleanPath();
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData clusterMetaData) {
        initializeBlobTasks();
        super.initializeSnapshot(snapshotId, indices, clusterMetaData);
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId, ActionListener<Void> listener) {
        initializeBlobTasks();
        super.deleteSnapshot(snapshotId, repositoryStateId, listener);
        finalizeBlobTasks(snapshotId.toString(), listener);
    }

    private void initializeBlobTasks() {
        blobDeletionTasks.clear();
        blobWriteTasks.clear();
    }

    public void addDeletion(String blobName, Future<DeleteResult> task) {
        blobDeletionTasks.put(blobName, task);
    }

    //
    // Intent of this method is to provide a wait that delays completion of potentially mutually exclusive operations
    // in Elasticsearch
    //
    private void finalizeBlobDeletion(String operationId, ActionListener<?> listener, final long timeLimit) {
        long failedCount = 0;

        for (Map.Entry<String, Future<DeleteResult>> entry: blobDeletionTasks.entrySet()) {
            try {
                long remaining_ns = timeLimit - System.nanoTime();
                if (remaining_ns < 0) {
                    throw new TimeoutException();
                }

                entry.getValue().get(remaining_ns, TimeUnit.NANOSECONDS);
            }
            catch (TimeoutException e){
                long notDoneCount = blobDeletionTasks.values().stream().filter(t -> !t.isDone()).count();
                if (listener != null){
                    listener.onFailure(new RepositoryException(metadata.name(),
                        "failed to delete snapshot [" + operationId + "]: timed out, " + notDoneCount + " deletions in progress"));
                }
                return; // Stop processing
            }
            catch (Exception e) {
                logger.warn("failed to delete blob [" + entry.getKey() + "]", e);
                failedCount++;
            }
        }

        if (failedCount > 0 && listener != null){
            listener.onFailure(new RepositoryException(metadata.name(),
                "failed to delete snapshot [" + operationId + "]: failed to delete " + failedCount + " blobs"));
        }
    }

    private void finalizeBlobTasks(String operationId, ActionListener<?> listener) {
        long now = System.nanoTime();
        final long deleteTimeLimit = now + TimeUnit.NANOSECONDS.convert(Swift.DELETE_TIMEOUT_MIN_SETTING.get(envSettings),
                                                                        TimeUnit.MINUTES);
        final long snapshotTimeLimit = now + TimeUnit.NANOSECONDS.convert(Swift.SNAPSHOT_TIMEOUT_MIN_SETTING.get(envSettings),
                                                                          TimeUnit.MINUTES);

        finalizeBlobWrite(operationId, listener, snapshotTimeLimit);
        finalizeBlobDeletion(operationId, listener, deleteTimeLimit);
    }

    public void addWrite(String blobName, Future<Void> task) {
        blobWriteTasks.put(blobName, task);
    }

    //
    // Intent of this method is to provide a wait that delays completion of potentially mutually exclusive operations
    // in Elasticsearch
    //
    private void finalizeBlobWrite(String operationId, ActionListener<?> listener, final long timeLimit) {
        long failedCount = 0;

        for (Map.Entry<String, Future<Void>> entry: blobWriteTasks.entrySet()) {
            try {
                long remaining_ns = timeLimit - System.nanoTime();
                if (remaining_ns < 0) {
                    throw new TimeoutException();
                }

                entry.getValue().get(remaining_ns, TimeUnit.NANOSECONDS);
            }
            catch (TimeoutException e){
                long notDoneCount = blobWriteTasks.values().stream().filter(t -> !t.isDone()).count();
                if (listener != null){
                    listener.onFailure(new RepositoryException(metadata.name(),
                            "failed to complete writes [" + operationId + "]: timed out, " + notDoneCount + " writes in progress"));
                }
                return; // Stop processing
            }
            catch (Exception e) {
                logger.warn("failed to complete writes [" + entry.getKey() + "]", e);
                failedCount++;
            }
        }

        if (failedCount > 0 && listener != null){
            listener.onFailure(new RepositoryException(metadata.name(),
                    "failed to complete snapshot [" + operationId + "]: failed to write " + failedCount + " blobs"));
        }
    }

    @Override
    public void cleanup(long repositoryStateId, ActionListener<RepositoryCleanupResult> listener) {
        initializeBlobTasks();
        super.cleanup(repositoryStateId, listener);
        finalizeBlobTasks(String.valueOf(repositoryStateId), listener);
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure,
                                 int totalShards, List<SnapshotShardFailure> shardFailures, long repositoryStateId,
                                 boolean includeGlobalState, MetaData clusterMetaData, Map<String, Object> userMetadata,
                                 ActionListener<SnapshotInfo> listener) {
        super.finalizeSnapshot(snapshotId, indices, startTime, failure, totalShards, shardFailures, repositoryStateId,
                includeGlobalState, clusterMetaData, userMetadata, listener);
        finalizeBlobTasks(snapshotId.toString(), listener);
    }

    @Override
    public String startVerification() {
        initializeBlobTasks();
        return super.startVerification();
    }

    @Override
    public void endVerification(String seed) {
        super.endVerification(seed);
        finalizeBlobTasks("verification", null);
    }

    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus,
                              ActionListener<Void> listener) {
        super.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, snapshotStatus, listener);
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
        return super.getShardSnapshotStatus(snapshotId, version, indexId, shardId);
    }

    protected BlobStore createBlobStore() {
        String username = Swift.USERNAME_SETTING.get(settings);
        String password = Swift.PASSWORD_SETTING.get(settings);
        String tenantName = Swift.TENANTNAME_SETTING.get(settings);
        String authMethod = Swift.AUTHMETHOD_SETTING.get(settings);
        String preferredRegion = Swift.PREFERRED_REGION_SETTING.get(settings);
        String authScope = Swift.AUTHSCOPE_SETTING.get(settings);
        String authDomain = Swift.AUTHDOMAIN_SETTING.get(settings);

        String containerName = Swift.CONTAINER_SETTING.get(settings);
        if (containerName == null) {
            throw new RepositoryException(metadata.name(), "No container defined for swift repository");
        }

        String url = Swift.URL_SETTING.get(settings);
        if (url == null) {
            throw new RepositoryException(metadata.name(), "No url defined for swift repository");
        }

        Account account = SwiftAccountFactory.createAccount(swiftService,
                url,
                username,
                password,
                tenantName,
                authMethod,
                preferredRegion,
                authScope,
                authDomain);

        return new SwiftBlobStore(this, settings, envSettings, account, containerName);
    }

    /**
     * Get the base blob path
     */
    @Override
    public BlobPath basePath() {
        return basePath;
    }

    /**
     * Get the chunk size
     */
    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }
}
