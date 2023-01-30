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
package org.apache.rocketmq.common.topic;

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.common.UtilAll;

public class TopicValidator {

    public static final String AUTO_CREATE_TOPIC_KEY_TOPIC = "TBW102"; // Will be created at broker when isAutoCreateTopicEnable
    public static final String RMQ_SYS_SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    public static final String RMQ_SYS_BENCHMARK_TOPIC = "BenchmarkTest";
    public static final String RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC";
    public static final String RMQ_SYS_TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC";
    public static final String RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    public static final String RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = "TRANS_CHECK_MAX_TIME_TOPIC";
    public static final String RMQ_SYS_SELF_TEST_TOPIC = "SELF_TEST_TOPIC";
    public static final String RMQ_SYS_OFFSET_MOVED_EVENT = "OFFSET_MOVED_EVENT";

    public static final String SYSTEM_TOPIC_PREFIX = "rmq_sys_";
    public static final String SYNC_BROKER_MEMBER_GROUP_PREFIX = SYSTEM_TOPIC_PREFIX + "SYNC_BROKER_MEMBER_";

    public static final boolean[] VALID_CHAR_BIT_MAP = new boolean[128];
    private static final int TOPIC_MAX_LENGTH = 127;

    private static final Set<String> SYSTEM_TOPIC_SET = new HashSet<>(32);

    /**
     * Set of topic which client can not send message
     */
    private static final Set<String> NOT_ALLOWED_SEND_TOPIC_SET = new HashSet<>();

    static {
        SYSTEM_TOPIC_SET.add(AUTO_CREATE_TOPIC_KEY_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_SCHEDULE_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_BENCHMARK_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRANS_HALF_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRACE_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRANS_OP_HALF_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_SELF_TEST_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_OFFSET_MOVED_EVENT);

        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_SCHEDULE_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TRANS_HALF_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TRANS_OP_HALF_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_SELF_TEST_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_OFFSET_MOVED_EVENT);

        // regex: ^[%|a-zA-Z0-9_-]+$
        // %
        VALID_CHAR_BIT_MAP['%'] = true;
        // -
        VALID_CHAR_BIT_MAP['-'] = true;
        // _
        VALID_CHAR_BIT_MAP['_'] = true;
        // |
        VALID_CHAR_BIT_MAP['|'] = true;
        for (int i = 0; i < VALID_CHAR_BIT_MAP.length; i++) {
            if (i >= '0' && i <= '9') {
                // 0-9
                VALID_CHAR_BIT_MAP[i] = true;
            } else if (i >= 'A' && i <= 'Z') {
                // A-Z
                VALID_CHAR_BIT_MAP[i] = true;
            } else if (i >= 'a' && i <= 'z') {
                // a-z
                VALID_CHAR_BIT_MAP[i] = true;
            }
        }
    }

    public static boolean isTopicOrGroupIllegal(String str) {
        int strLen = str.length();
        int len = VALID_CHAR_BIT_MAP.length;
        boolean[] bitMap = VALID_CHAR_BIT_MAP;
        for (int i = 0; i < strLen; i++) {
            char ch = str.charAt(i);
            if (ch >= len || !bitMap[ch]) {
                return true;
            }
        }
        return false;
    }

    public static ValidateTopicResult validateTopic(String topic) {

        if (UtilAll.isBlank(topic)) {
            return new ValidateTopicResult(false, "The specified topic is blank.");
        }

        if (isTopicOrGroupIllegal(topic)) {
            return new ValidateTopicResult(false, "The specified topic contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$");
        }

        if (topic.length() > TOPIC_MAX_LENGTH) {
            return new ValidateTopicResult(false, "The specified topic is longer than topic max length.");
        }

        return new ValidateTopicResult(true, "");
    }

    public static class ValidateTopicResult {
        private final boolean valid;
        private final String remark;

        public ValidateTopicResult(boolean valid, String remark) {
            this.valid = valid;
            this.remark = remark;
        }

        public boolean isValid() {
            return valid;
        }

        public String getRemark() {
            return remark;
        }
    }

    public static boolean isSystemTopic(String topic) {
        return SYSTEM_TOPIC_SET.contains(topic) || topic.startsWith(SYSTEM_TOPIC_PREFIX);
    }

    public static boolean isNotAllowedSendTopic(String topic) {
        return NOT_ALLOWED_SEND_TOPIC_SET.contains(topic);
    }

    public static void addSystemTopic(String systemTopic) {
        SYSTEM_TOPIC_SET.add(systemTopic);
    }

    public static Set<String> getSystemTopicSet() {
        return SYSTEM_TOPIC_SET;
    }

    public static Set<String> getNotAllowedSendTopicSet() {
        return NOT_ALLOWED_SEND_TOPIC_SET;
    }
}
