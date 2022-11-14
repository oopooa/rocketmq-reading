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
package org.apache.rocketmq.client;

import java.util.Set;
import java.util.TreeSet;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.shade.org.slf4j.Logger;
import org.apache.rocketmq.shade.org.slf4j.LoggerFactory;

public class MQHelper {
    private static final Logger logger = LoggerFactory.getLogger(MQHelper.class);

    @Deprecated
    public static void resetOffsetByTimestamp(
        final MessageModel messageModel,
        final String consumerGroup,
        final String topic,
        final long timestamp) throws Exception {
        resetOffsetByTimestamp(messageModel, "DEFAULT", consumerGroup, topic, timestamp);
    }

    /**
     * Reset consumer topic offset according to time
     *
     * @param messageModel  which model
     * @param instanceName  which instance
     * @param consumerGroup consumer group
     * @param topic         topic
     * @param timestamp     time
     */
    public static void resetOffsetByTimestamp(
        final MessageModel messageModel,
        final String instanceName,
        final String consumerGroup,
        final String topic,
        final long timestamp) throws Exception {

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
        consumer.setInstanceName(instanceName);
        consumer.setMessageModel(messageModel);
        consumer.start();

        Set<MessageQueue> mqs = null;
        try {
            mqs = consumer.fetchSubscribeMessageQueues(topic);
            if (mqs != null && !mqs.isEmpty()) {
                TreeSet<MessageQueue> mqsNew = new TreeSet<>(mqs);
                for (MessageQueue mq : mqsNew) {
                    long offset = consumer.searchOffset(mq, timestamp);
                    if (offset >= 0) {
                        consumer.updateConsumeOffset(mq, offset);
                        logger.info("resetOffsetByTimestamp updateConsumeOffset success, {} {} {}",
                            consumerGroup, offset, mq);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("resetOffsetByTimestamp Exception", e);
            throw e;
        } finally {
            if (mqs != null) {
                consumer.getDefaultMQPullConsumerImpl().getOffsetStore().persistAll(mqs);
            }
            consumer.shutdown();
        }
    }
}
