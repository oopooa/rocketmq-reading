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


import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;

public abstract class VirtualThreadFactorySupport {
    private static final Logger logger = LoggerFactory.getLogger(VirtualThreadFactorySupport.class);

    /**
     * Create a virtual thread factory, returns null when failed.
     */
    public static final ThreadFactory create(final String prefix) {
        try {
            Class<?> builderClass = ClassLoader.getSystemClassLoader().loadClass("java.lang.Thread$Builder");
            Class<?> ofVirtualClass = ClassLoader.getSystemClassLoader()
                .loadClass("java.lang.Thread$Builder$OfVirtual");
            Method ofVirtualMethod = Thread.class.getDeclaredMethod("ofVirtual");
            Object builder = ofVirtualMethod.invoke(null);
            Method nameMethod = ofVirtualClass.getDeclaredMethod("name", String.class, long.class);
            Method factoryMethod = builderClass.getDeclaredMethod("factory");
            builder = nameMethod.invoke(builder, prefix + "-virtual-thread-", 0L);
            final ThreadFactory threadFactory = (ThreadFactory) factoryMethod.invoke(builder);
            return runnable -> {
                Objects.requireNonNull(runnable, "Runnable is null");
                final Thread thread = threadFactory.newThread(runnable);
                thread.setUncaughtExceptionHandler((t, e) -> {
                    logger.error("Uncaught exception in virtual thread,prefix:{}", prefix, e);
                });
                return thread;
            };
        } catch (Exception e) {
            logger.error("Create virtual thread factory failed", e);
            return null;
        }
    }
}
