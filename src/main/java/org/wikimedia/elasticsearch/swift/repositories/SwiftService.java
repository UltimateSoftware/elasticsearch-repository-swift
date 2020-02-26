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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.exception.CommandException;
import org.javaswift.joss.model.Account;
import org.wikimedia.elasticsearch.swift.SwiftPerms;

public class SwiftService extends AbstractLifecycleComponent {
    // The account we'll be connecting to Swift with
    private Account swiftUser;

    private final boolean allowCaching;

    /**
     * Constructor
     * 
     * @param settings
     *            Settings for our repository. Injected.
     */
    @Inject
    public SwiftService(Settings settings) {
        allowCaching = settings.getAsBoolean(SwiftRepository.Swift.ALLOW_CACHING_SETTING.getKey(),
                                 true);
    }

    /**
     * Create a Swift account object and connect it to Swift
     * 
     * @param url
     *            The auth url (eg: localhost:8080/auth/v1.0/)
     * @param username
     *            The username
     * @param password
     *            The password
     * @param preferredRegion
     *            The preferred region set
     * @return swift Account
     */
    public synchronized Account swiftBasic(String url, String username, String password, String preferredRegion) {
        if (swiftUser != null) {
            return swiftUser;
        }

        try {
            AccountConfig conf = getStandardConfig(url, username, password, AuthenticationMethod.BASIC,
                    preferredRegion);
            swiftUser = createAccount(conf);
        } catch (CommandException ce) {
            throw new ElasticsearchException("Unable to authenticate to Swift Basic " + url + "/" + username +
                    "/" + password, ce);
        }
        return swiftUser;
    }

    private Account createAccount(final AccountConfig conf) {
        return SwiftPerms.exec(() -> new AccountFactory(conf).createAccount());
    }

    public synchronized Account swiftKeyStone(String url, String username, String password, String tenantName,
                                              String preferredRegion) {
        if (swiftUser != null) {
            return swiftUser;
        }

        try {
            AccountConfig conf = getStandardConfig(url, username, password, AuthenticationMethod.KEYSTONE,
                    preferredRegion);
            conf.setTenantName(tenantName);
            swiftUser = createAccount(conf);
        } catch (CommandException ce) {
            throw new ElasticsearchException(
                    "Unable to authenticate to Swift Keystone " + url + "/" + username + "/" + password + "/"
                            + tenantName, ce);
        }
        return swiftUser;
    }

    public synchronized Account swiftTempAuth(String url, String username, String password, String preferredRegion) {
        if (swiftUser != null) {
            return swiftUser;
        }

        try {
            AccountConfig conf = getStandardConfig(url, username, password, AuthenticationMethod.TEMPAUTH,
                    preferredRegion);
            swiftUser = createAccount(conf);
        } catch (CommandException ce) {
            throw new ElasticsearchException("Unable to authenticate to Swift Temp", ce);
        }
        return swiftUser;
    }

    private AccountConfig getStandardConfig(String url, String username, String password, AuthenticationMethod method,
                                            String preferredRegion) {
        AccountConfig conf = new AccountConfig();
        conf.setAuthUrl(url);
        conf.setUsername(username);
        conf.setPassword(password);
        conf.setAuthenticationMethod(method);
        conf.setAllowContainerCaching(allowCaching);
        conf.setAllowCaching(allowCaching);
        conf.setPreferredRegion(preferredRegion);
        return conf;
    }

    /**
     * Start the service. No-op here.
     */
    @Override
    protected void doStart() throws ElasticsearchException {
    }

    /**
     * Stop the service. No-op here.
     */
    @Override
    protected void doStop() throws ElasticsearchException {
    }

    /**
     * Close the service. No-op here.
     */
    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
