package org.wikimedia.elasticsearch.swift;


import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class WithTimeout {
    public static <T> T retry(long interval, long timeout, TimeUnit timeUnit, Callable<T> callable) throws TimeoutException, ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            Future<T> task = executorService.submit(() -> internalRetry(interval, timeout, timeUnit, callable));

            T result = task.get(timeout, timeUnit);
            return result;
        }
        finally {
            executorService.shutdownNow();
        }
    }

    public static void retry(long interval, long timeout, TimeUnit timeUnit, Runnable runnable) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            Future<Void> task = executorService.submit(() -> internalRetry(interval, timeout, timeUnit, () -> {runnable.run();
                                                                                                                return null;}));

            task.get(timeout, timeUnit);
        }
        finally {
            executorService.shutdownNow();
        }
    }

    private static <T> T internalRetry(long interval, long timeout, TimeUnit timeUnit, Callable<T> callable) throws TimeoutException, InterruptedException {
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
