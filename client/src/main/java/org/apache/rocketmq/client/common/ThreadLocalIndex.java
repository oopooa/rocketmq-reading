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

package org.apache.rocketmq.client.common;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadLocalIndex {

    private final Random random = new Random();
    private final static int POSITIVE_MASK = 0x7FFFFFFF;
    private final ThreadLocal<AtomicInteger> threadLocalIndex = new ThreadLocal<AtomicInteger>() {
        @Override
        protected AtomicInteger initialValue(){
            return new AtomicInteger(random.nextInt());
        }
    };

    public int incrementAndGet() {
        Integer index = this.threadLocalIndex.get().getAndIncrement();
        return Math.abs(index & POSITIVE_MASK);
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" +
            "threadLocalIndex=" + threadLocalIndex.get().get() +
            '}';
    }
}
