/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * HATest
 *
 * @author yanglibo@qccr.com
 * @version HATest.java 2019年01月14日 17:34:31
 */
public class HATest {
    private final String StoreMessage = "Once, there was a chance for me!";
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost;
    private SocketAddress StoreHost;
    private byte[] MessageBody;

    private MessageStore messageStore;
    private MessageStore slaveMessageStore;
    private MessageStoreConfig masterMessageStoreConfig;
    private MessageStoreConfig slaveStoreConfig;
    private BrokerStatsManager brokerStatsManager = new BrokerStatsManager("simpleTest");
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";
    private HAService haService;
    @Before
    public void init() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        masterMessageStoreConfig = new MessageStoreConfig();
        masterMessageStoreConfig.setBrokerRole(BrokerRole.SYNC_MASTER);
        masterMessageStoreConfig.setStorePathRootDir(storePathRootDir+File.separator+"master");
        masterMessageStoreConfig.setStorePathCommitLog(storePathRootDir+File.separator+"master"+ File.separator+"commitlog");
        buildMessageStoreConfig(masterMessageStoreConfig);
        slaveStoreConfig = new MessageStoreConfig();
        slaveStoreConfig.setBrokerRole(BrokerRole.SLAVE);
        slaveStoreConfig.setStorePathRootDir(storePathRootDir+File.separator+"slave");
        slaveStoreConfig.setStorePathCommitLog(storePathRootDir+File.separator+"slave"+ File.separator+"commitlog");
        slaveStoreConfig.setHaListenPort(10943);
        buildMessageStoreConfig(slaveStoreConfig);
        messageStore = buildMessageStore(masterMessageStoreConfig,0L);
        slaveMessageStore = buildMessageStore(slaveStoreConfig,1L);
        boolean load = messageStore.load();
        boolean slaveLoad = slaveMessageStore.load();
        slaveMessageStore.updateHaMasterAddress("127.0.0.1:10912");
        assertTrue(load);
        assertTrue(slaveLoad);
        messageStore.start();
        Thread.sleep(10000L);//slave start after the master start
        slaveMessageStore.start();
        Thread.sleep(5000L);//slave start after the master start
        System.out.println(haService.getConnectionCount());
    }

    @Test
    public void testHandleHA() throws Exception{
        long totalMsgs = 10;
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();
        for (long i = 0; i < totalMsgs; i++) {
            MessageExtBrokerInner msg = buildMessage();
            PutMessageResult putMessageResult = messageStore.putMessage(msg);
            System.out.println(putMessageResult.getPutMessageStatus());
            System.out.println(msg.isWaitStoreMsgOK());
        }

        Thread.sleep(1000L);//sleep 1000 ms
        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = slaveMessageStore.getMessage("GROUP_A", "FooBar", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            System.out.println(result.getStatus());
            assertTrue(GetMessageStatus.FOUND.equals(result.getStatus()));
            result.release();
        }
    }

    @After
    public void destroy() throws Exception{
        Thread.sleep(5000L);
        slaveMessageStore.shutdown();
        slaveMessageStore.destroy();
        messageStore.shutdown();
        messageStore.destroy();
        File file = new File(storePathRootDir);
        UtilAll.deleteFile(file);
    }

    private MessageStore buildMessageStore(MessageStoreConfig messageStoreConfig,long brokerId) throws Exception {

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerId(brokerId);
        DefaultMessageStore defaultMessageStore =
                new DefaultMessageStore(messageStoreConfig, brokerStatsManager, null, brokerConfig);
        haService = defaultMessageStore.getHaService();
        return defaultMessageStore;
    }

    private void buildMessageStoreConfig(MessageStoreConfig messageStoreConfig){
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setKeys("myHello");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        return msg;
    }

}
