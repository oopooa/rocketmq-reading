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
package org.apache.rocketmq.store.config;

public enum BrokerRole {

    /**
     * 异步主节点
     * 接收到生产者的消息之后立即返回确认, 再异步地将消息复制到 Slave 节点。
     * 该模式可以提供更低的延时, 但会存在消息丢失的风险。
     */
    ASYNC_MASTER,

    /**
     * 同步主节点
     * 接收到生产者的消息后, 会将消息同步到 Slave 节点后才返回确认。
     * 该确保了消息的可靠性, 但会有更高的延时。
     */
    SYNC_MASTER,

    /**
     * 从节点
     * 负责对主节点的消息备份, 不参与消息的写入。
     * 消费者可以从 Slave 节点读取消息, 分担主节点的读取压力。
     */
    SLAVE;
}
