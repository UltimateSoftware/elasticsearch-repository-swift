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

import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class WithTimeout {
    private final ExecutorService executorService;

    public WithTimeout(SwiftRepository repository) {
        executorService = repository != null ? repository.threadPool().generic() : null;
    }

    public <T> T retry(long interval, long timeout, TimeUnit timeUnit, Callable<T> callable) throws Exception {
        if (executorService == null){
            return callable.call();
        }

        Future<T> task = executorService.submit(() -> internalRetry(interval, timeout, timeUnit, callable));
        T result = task.get(timeout, timeUnit);
        return result;
    }

    public void retry(long interval, long timeout, TimeUnit timeUnit, Runnable runnable)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (executorService == null){
            runnable.run();
            return;
        }

        Future<Void> task = executorService.submit(() -> internalRetry(interval, timeout, timeUnit, () -> {runnable.run();
                                                                                                           return null;}));
        task.get(timeout, timeUnit);
    }

    private <T> T internalRetry(long interval, long timeout, TimeUnit timeUnit, Callable<T> callable)
            throws TimeoutException, InterruptedException {
        final long sleepMillis = TimeUnit.MILLISECONDS.convert(interval, timeUnit);
        final int sleepNanos = (int)(TimeUnit.NANOSECONDS.convert(interval, timeUnit) - sleepMillis * 1_000_000);
        final long nanoTimeLimit = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, timeUnit);

        while (System.nanoTime() < nanoTimeLimit) {
            try {
                return callable.call();
            }
            catch (InterruptedException e) {
                throw e;
            }
            catch (Exception e) {
                Thread.sleep(sleepMillis, sleepNanos);
            }
        }

        throw new TimeoutException("retry timed out");
    }
}
