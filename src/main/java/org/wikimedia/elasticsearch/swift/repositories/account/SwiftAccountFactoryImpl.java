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

package org.wikimedia.elasticsearch.swift.repositories.account;

import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.wikimedia.elasticsearch.swift.repositories.SwiftService;

public class SwiftAccountFactoryImpl implements SwiftAccountFactory {
    private final SwiftService swiftService;

    public SwiftAccountFactoryImpl(SwiftService swiftService){
        this.swiftService = swiftService;
    }

    @Override
    public Account createAccount(String url,
                                 String username,
                                 String password,
                                 String tenantName,
                                 String domainName,
                                 String authMethod,
                                 String preferredRegion) {
        if (AuthenticationMethod.KEYSTONE_V3.name().equalsIgnoreCase(authMethod)) {
            return swiftService.swiftKeyStoneV3(url, username, password, tenantName, domainName, preferredRegion);
        }

        if (AuthenticationMethod.KEYSTONE.name().equalsIgnoreCase(authMethod)) {
            return swiftService.swiftKeyStone(url, username, password, tenantName, preferredRegion);
        }

        if (AuthenticationMethod.TEMPAUTH.name().equalsIgnoreCase(authMethod)) {
            return swiftService.swiftTempAuth(url, username, password, preferredRegion);
        }

        return swiftService.swiftBasic(url, username, password, preferredRegion);
    }
}
