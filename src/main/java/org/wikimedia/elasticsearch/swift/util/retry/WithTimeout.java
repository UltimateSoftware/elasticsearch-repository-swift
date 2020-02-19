package org.wikimedia.elasticsearch.swift.util.retry;

import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface WithTimeout {
    <T> T retry(long interval, long timeout, TimeUnit timeUnit, Callable<T> callable) throws Exception;

    void retry(long interval, long timeout, TimeUnit timeUnit, Runnable runnable)
            throws InterruptedException, ExecutionException, TimeoutException;

    class Factory {
        public WithTimeout from(SwiftRepository repository){
            if (repository == null){
                return new WithTimeoutDirectImpl();
            }

            return new WithTimeoutExecutorImpl(repository.threadPool().generic());
        }
    }
}
