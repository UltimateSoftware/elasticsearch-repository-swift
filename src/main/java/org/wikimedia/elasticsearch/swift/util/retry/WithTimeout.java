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

import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public interface WithTimeout {
    <T> T retry(long interval, long timeout, TimeUnit timeUnit, Callable<T> callable) throws Exception;

    <T> T retry(long interval, long timeout, TimeUnit timeUnit, int attempts, Callable<T> callable) throws Exception;

    <T> T timeout(long timeout, TimeUnit timeUnit, Callable<T> callable) throws Exception;

    class Factory {
        public WithTimeout from(ThreadPool threadPool){
            if (threadPool == null){
                return new WithTimeoutDirectImpl();
            }

            return new WithTimeoutExecutorImpl(threadPool.generic());
        }
    }
}
