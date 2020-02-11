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

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.javaswift.joss.model.Account;
import org.wikimedia.elasticsearch.swift.repositories.blobstore.SwiftBlobStore;

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
        Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting("chunk_size", new ByteSizeValue(5,
                ByteSizeUnit.GB));
        Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false);
        Setting<Boolean> MINIMIZE_BLOB_EXISTS_CHECKS_SETTING = Setting.boolSetting("repository_swift.minimize_blob_exists_checks",
                                                                                   true,
                                                                                    Setting.Property.NodeScope);
        Setting<Boolean> ALLOW_CACHING_SETTING = Setting.boolSetting("repository_swift.allow_caching",
                                                                     true,
                                                                     Setting.Property.NodeScope);


    }



    // Base path for blobs
    private final BlobPath basePath;

    // Chunk size.
    private final ByteSizeValue chunkSize;

    protected final Settings settings;
    protected final SwiftService swiftService;

    /**
     * Constructs new BlobStoreRepository
     *
     * @param metadata
     *            repository meta data
     * @param settings
     *            global settings
     * @param namedXContentRegistry
     *            an instance of NamedXContentRegistry
     * @param swiftService
     *            an instance of SwiftService
     * @param threadPool
     *            an elastic search ThreadPool
     */
    @Inject
    public SwiftRepository(RepositoryMetaData metadata, Settings settings,
                           NamedXContentRegistry namedXContentRegistry, SwiftService swiftService, ThreadPool threadPool) {
        super(metadata, Swift.COMPRESS_SETTING.get(metadata.settings()), namedXContentRegistry, threadPool);
        this.settings = settings;
        this.swiftService = swiftService;
        this.chunkSize = Swift.CHUNK_SIZE_SETTING.get(metadata.settings());
        this.basePath = BlobPath.cleanPath();
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        String username = Swift.USERNAME_SETTING.get(metadata.settings());
        String password = Swift.PASSWORD_SETTING.get(metadata.settings());
        String tenantName = Swift.TENANTNAME_SETTING.get(metadata.settings());
        String authMethod = Swift.AUTHMETHOD_SETTING.get(metadata.settings());
        String preferredRegion = Swift.PREFERRED_REGION_SETTING.get(metadata.settings());

        String container = Swift.CONTAINER_SETTING.get(metadata.settings());
        if (container == null) {
            throw new RepositoryException(metadata.name(), "No container defined for swift repository");
        }

        String url = Swift.URL_SETTING.get(metadata.settings());
        if (url == null) {
            throw new RepositoryException(metadata.name(), "No url defined for swift repository");
        }

        Account account = SwiftAccountFactory.createAccount(swiftService, url, username, password, tenantName,
                authMethod, preferredRegion);

        return new SwiftBlobStore(settings, account, container);
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
