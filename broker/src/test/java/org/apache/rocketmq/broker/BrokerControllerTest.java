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

package org.apache.rocketmq.broker;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.latency.FutureTaskExt;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerControllerTest {

    @Test
    public void testBrokerRestart() throws Exception {
        BrokerController brokerController = new BrokerController(
            new BrokerConfig(),
            new NettyServerConfig(),
            new NettyClientConfig(),
            new MessageStoreConfig());
        assertThat(brokerController.initialize()).isTrue();
        brokerController.start();
        brokerController.shutdown();
    }

    @Test
    public void testMultiPathStart() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(
                "target/unit_test_store/a/store" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/b/store" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/c/store"
        );
        BrokerController brokerController = new BrokerController(
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig(),
                messageStoreConfig);
        assertThat(brokerController.initialize()).isTrue();
        brokerController.start();
        brokerController.shutdown();
        UtilAll.deleteFile(new File(new MessageStoreConfig().getStorePathRootDir()));
    }

    @Test
    public void testMultiPathLoadConfig() {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(
                "target/unit_test_store/a/store" + MixAll.MULTI_PATH_SPLITTER
                        + "target/unit_test_store/b/store" + MixAll.MULTI_PATH_SPLITTER
                        + "target/unit_test_store/c/store"
        );
        BrokerController brokerController = new BrokerController(
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig(),
                messageStoreConfig);
        // pre-write config/topic.json
        TopicConfigManager topicConfigManager = new TopicConfigManager(brokerController);
        String topic = "test_multi_path";
        TopicConfig topicConfig = new TopicConfig(topic);
        TopicValidator.addSystemTopic(topic);
        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        topicConfigManager.createTopicIfAbsent(topicConfig);

        assertThat(brokerController.initializeLoadRootPath().equals("target/unit_test_store/a/store")).isTrue();

        // change the path, should load the newest path
        messageStoreConfig.setStorePathRootDir(
                "target/unit_test_store/b/store" + MixAll.MULTI_PATH_SPLITTER
                        + "target/unit_test_store/a/store" + MixAll.MULTI_PATH_SPLITTER
                        + "target/unit_test_store/c/store"
        );
        brokerController = new BrokerController(
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig(),
                messageStoreConfig);
        assertThat(brokerController.initializeLoadRootPath().equals("target/unit_test_store/a/store")).isTrue();

        UtilAll.deleteFile(new File(new MessageStoreConfig().getStorePathRootDir()));
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(new MessageStoreConfig().getStorePathRootDir()));
    }

    @Test
    public void testHeadSlowTimeMills() throws Exception {
        BrokerController brokerController = new BrokerController(
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig(),
                new MessageStoreConfig());
        brokerController.initialize();
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

        //create task is not instance of FutureTaskExt;
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

            }
        };

        RequestTask requestTask = new RequestTask(runnable, null, null);
        // the requestTask is not the head of queue;
        queue.add(new FutureTaskExt<>(requestTask, null));

        long headSlowTimeMills = 100;
        TimeUnit.MILLISECONDS.sleep(headSlowTimeMills);
        assertThat(brokerController.headSlowTimeMills(queue)).isGreaterThanOrEqualTo(headSlowTimeMills);
    }
}
