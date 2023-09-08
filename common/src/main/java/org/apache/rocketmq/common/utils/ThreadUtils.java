/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.FutureTaskExtThreadPoolExecutor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public final class ThreadUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TOOLS_LOGGER_NAME);

    public static ExecutorService newSingleThreadExecutor(String processName, boolean isDaemon) {
        return ThreadUtils.newThreadPoolExecutor(1,
            1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            newThreadFactory(processName, isDaemon));
    }

    public static ExecutorService newThreadPoolExecutor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit, BlockingQueue<Runnable> workQueue,
        String processName,
        boolean isDaemon) {
        return ThreadUtils.newThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, newThreadFactory(processName, isDaemon));
    }

    public static ExecutorService newThreadPoolExecutor(final int corePoolSize,
        final int maximumPoolSize,
        final long keepAliveTime,
        final TimeUnit unit,
        final BlockingQueue<Runnable> workQueue,
        final ThreadFactory threadFactory) {
        return ThreadUtils.newThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, new ThreadPoolExecutor.AbortPolicy());
    }

    public static ExecutorService newThreadPoolExecutor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory,
        RejectedExecutionHandler handler) {
        return new FutureTaskExtThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(String processName, boolean isDaemon) {
        return ThreadUtils.newFixedThreadScheduledPool(1, processName, isDaemon);
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(ThreadFactory threadFactory) {
        return ThreadUtils.newFixedThreadScheduledPool(1, threadFactory);
    }

    public static ScheduledExecutorService newFixedThreadScheduledPool(int nThreads, String processName,
        boolean isDaemon) {
        return ThreadUtils.newFixedThreadScheduledPool(nThreads, newThreadFactory(processName, isDaemon));
    }

    public static ScheduledExecutorService newFixedThreadScheduledPool(int corePoolSize, ThreadFactory threadFactory) {
        return new ScheduledThreadPoolExecutor(corePoolSize, threadFactory);
    }

    public static ThreadFactory newThreadFactory(String processName, boolean isDaemon) {
        return newGenericThreadFactory("ThreadUtils-" + processName, isDaemon);
    }

    public static ThreadFactory newGenericThreadFactory(String processName) {
        return newGenericThreadFactory(processName, false);
    }

    public static ThreadFactory newGenericThreadFactory(String processName, int threads) {
        return newGenericThreadFactory(processName, threads, false);
    }

    public static ThreadFactory newGenericThreadFactory(final String processName, final boolean isDaemon) {
        return new ThreadFactoryImpl(processName + "_", isDaemon);
    }

    public static ThreadFactory newGenericThreadFactory(final String processName, final int threads,
        final boolean isDaemon) {
        return new ThreadFactoryImpl(String.format("%s_%d_", processName, threads), isDaemon);
    }

    /**
     * Create a new thread
     *
     * @param name     The name of the thread
     * @param runnable The work for the thread to do
     * @param daemon   Should the thread block JVM stop?
     * @return The unstarted thread
     */
    public static Thread newThread(String name, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                LOGGER.error("Uncaught exception in thread '" + t.getName() + "':", e);
            }
        });
        return thread;
    }

    /**
     * Shutdown passed thread using isAlive and join.
     *
     * @param t Thread to stop
     */
    public static void shutdownGracefully(final Thread t) {
        shutdownGracefully(t, 0);
    }

    /**
     * Shutdown passed thread using isAlive and join.
     *
     * @param millis Pass 0 if we're to wait forever.
     * @param t      Thread to stop
     */
    public static void shutdownGracefully(final Thread t, final long millis) {
        if (t == null)
            return;
        while (t.isAlive()) {
            try {
                t.interrupt();
                t.join(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * An implementation of the graceful stop sequence recommended by
     * {@link ExecutorService}.
     *
     * @param executor executor
     * @param timeout  timeout
     * @param timeUnit timeUnit
     */
    public static void shutdownGracefully(ExecutorService executor, long timeout, TimeUnit timeUnit) {
        // Disable new tasks from being submitted.
        executor.shutdown();
        try {
            // Wait a while for existing tasks to terminate.
            if (!executor.awaitTermination(timeout, timeUnit)) {
                executor.shutdownNow();
                // Wait a while for tasks to respond to being cancelled.
                if (!executor.awaitTermination(timeout, timeUnit)) {
                    LOGGER.warn(String.format("%s didn't terminate!", executor));
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted.
            executor.shutdownNow();
            // Preserve interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Shutdown the specific ExecutorService
     *
     * @param executorService the executor
     */
    public static void shutdown(ExecutorService executorService) {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    /**
     * A constructor to stop this class being constructed.
     */
    private ThreadUtils() {
        // Unused

    }
}
