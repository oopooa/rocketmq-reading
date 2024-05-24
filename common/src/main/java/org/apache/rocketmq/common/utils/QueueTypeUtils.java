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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class QueueTypeUtils {

    public static boolean isBatchCq(Optional<TopicConfig> topicConfig) {
        return Objects.equals(CQType.BatchCQ, getCQType(topicConfig));
    }

    public static CQType getCQType(Optional<TopicConfig> topicConfig) {
        // 如果 Topic 配置不存在
        if (!topicConfig.isPresent()) {
            // 默认返回简单消费队列
            return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
        }

        String attributeName = TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName();

        // 获取 Topic 配置属性
        Map<String, String> attributes = topicConfig.get().getAttributes();
        // 如果没有额外的属性
        if (attributes == null || attributes.size() == 0) {
            // 默认返回简单消费队列
            return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
        }

        // 如果包含 queue.type, 则转换为对应队列类型
        if (attributes.containsKey(attributeName)) {
            return CQType.valueOf(attributes.get(attributeName));
        } else {
            return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
        }
    }
}