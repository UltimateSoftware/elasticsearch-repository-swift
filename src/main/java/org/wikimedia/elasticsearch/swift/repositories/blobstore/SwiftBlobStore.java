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

/**
 * Our blob store
 */
public class SwiftBlobStore implements BlobStore {
    // How much to buffer our blobs by
    private final long bufferSizeInBytes;

    // Our Swift container. This is important.
    private final String containerName;
    private Container container;

    private final Settings settings;
    private final Account auth;

    /**
     * Constructor. Sets up the container mostly.
     * @param settings Settings for our repository. Only care about buffer size.
     * @param auth swift account info
     * @param containerName swift container
     */
    public SwiftBlobStore(Settings settings, final Account auth, final String containerName) {
        this.settings = settings;
        this.auth = auth;
        this.containerName = containerName;
        this.bufferSizeInBytes = settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).getBytes();
    }

    /**
     * Initialize container lazily.
     * @return the container
     */
    public Container getContainer() {
        if (container != null) {
            return container;
        }

        synchronized (this){
            if (container != null) {
                return container;
            }

            SwiftPerms.exec(() -> {
                container = auth.getContainer(containerName);
                if (!container.exists()) {
                    container.create();
                    container.makePublic();
                }
            });
        }

        return container;
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
        return new SwiftBlobContainer(path, this);
    }

    //TODO method seems unused. Remove?
    private void deleteByPrefix(Collection<DirectoryOrObject> directoryOrObjects) {
        for (DirectoryOrObject directoryOrObject : directoryOrObjects) {
            if (directoryOrObject.isObject()) {
                directoryOrObject.getAsObject().delete();
            } else {
                deleteByPrefix(container.listDirectory(directoryOrObject.getAsDirectory()));
            }
        }
    }

    /**
     * Close the store. No-op for us.
     */
    @Override
    public void close() {
    }

    protected Settings getSettings() {
        return settings;
    }
}
