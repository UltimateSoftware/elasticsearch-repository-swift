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

package org.wikimedia.elasticsearch.swift.util.retry;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

//
// This is a non-retry implementation
//
class WithTimeoutDirectImpl implements WithTimeout {
    @Override
    public <T> T retry(long interval, long timeout, TimeUnit timeUnit, Callable<T> callable) throws Exception {
        return callable.call();
    }

    @Override
    public void retry(long interval, long timeout, TimeUnit timeUnit, Runnable runnable) {
        runnable.run();
    }
}
