package org.wikimedia.elasticsearch.swift.util.retry;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
