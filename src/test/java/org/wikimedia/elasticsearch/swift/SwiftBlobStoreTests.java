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

import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.ESBlobStoreTestCase;
import org.javaswift.joss.client.mock.AccountMock;
import org.javaswift.joss.swift.Swift;
import org.junit.Before;
import org.wikimedia.elasticsearch.swift.repositories.blobstore.SwiftBlobStore;

import java.util.Locale;

public class SwiftBlobStoreTests extends ESBlobStoreTestCase {
    private Swift swift;
    private AccountMock account;

    @Before
    public void setup() {
        this.swift = new Swift();
        this.account = new AccountMock(swift);
    }

    @Override
    protected BlobStore newBlobStore() {
        String container = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        return new SwiftBlobStore(null, Settings.EMPTY, Settings.EMPTY, this.account, container);
    }
}
