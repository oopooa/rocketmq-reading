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

package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BatchConsumeMessageTest extends QueueTestBase {
    private MessageStore messageStore;

    @Before
    public void init() throws Exception {
        messageStore = createMessageStore(null, true);
        messageStore.load();
        messageStore.start();
    }

    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        File file = new File(messageStore.getMessageStoreConfig().getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    @Test
    public void testNextBeginOffsetConsumeBatchMessage() throws InterruptedException {
        String topic = UUID.randomUUID().toString();
        createTopic(topic, CQType.BatchCQ, messageStore);
        Random random = new Random();
        int putMessageCount = 1000;

        Queue<Integer> queue = new ArrayDeque<>();
        for (int i = 0; i < putMessageCount; i++) {
            int batchNum = random.nextInt(1000) + 2;
            MessageExtBrokerInner messageExtBrokerInner = buildMessage(topic, batchNum);
            PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            queue.add(batchNum);
        }

        Thread.sleep(2 * 1000);

        long pullOffset = 0L;
        int getMessageCount = 0;
        while (true) {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, pullOffset, 1, null);
            if (Objects.equals(getMessageResult.getStatus(), GetMessageStatus.OFFSET_OVERFLOW_ONE)) {
                break;
            }
            Assert.assertEquals(1, getMessageResult.getMessageQueueOffset().size());
            Long baseOffset = getMessageResult.getMessageQueueOffset().get(0);
            Integer batchNum = queue.poll();
            Assert.assertNotNull(batchNum);
            Assert.assertEquals(baseOffset + batchNum, getMessageResult.getNextBeginOffset());
            pullOffset = getMessageResult.getNextBeginOffset();
            getMessageCount++;
        }
        Assert.assertEquals(putMessageCount, getMessageCount);
    }

    @Test
    public void testGetOffsetInQueueByTime() throws Exception {
        String topic = "testGetOffsetInQueueByTime";

        createTopic(topic, CQType.BatchCQ, messageStore);
        Assert.assertTrue(QueueTypeUtils.isBatchCq(messageStore.getTopicConfig(topic)));

        // The initial min max offset, before and after the creation of consume queue
        Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(-1, messageStore.getMinOffsetInQueue(topic, 0));

        int batchNum = 10;
        long timeMid = -1;
        for (int i = 0; i < 19; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(topic, batchNum));
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Thread.sleep(2);
            if (i == 7)
                timeMid = System.currentTimeMillis();
        }

        Thread.sleep(2 * 1000);

        Assert.assertEquals(80, messageStore.getOffsetInQueueByTime(topic, 0, timeMid));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(190, messageStore.getMaxOffsetInQueue(topic, 0));

        Thread.sleep(5 * 1000);
        int maxBatchDeleteFilesNum = messageStore.getMessageStoreConfig().getMaxBatchDeleteFilesNum();
        messageStore.getCommitLog().deleteExpiredFile(1L, 100, 12000, true, maxBatchDeleteFilesNum);
        Assert.assertEquals(80, messageStore.getOffsetInQueueByTime(topic, 0, timeMid));
        Thread.sleep(70 * 1000);
        Assert.assertEquals(180, messageStore.getOffsetInQueueByTime(topic, 0, timeMid));
    }

    @Test
    public void testDispatchNormalConsumeQueue() throws Exception {
        String topic = "TestDispatchBuildConsumeQueue";
        createTopic(topic, CQType.SimpleCQ, messageStore);

        long timeStart = System.currentTimeMillis();
        long timeMid = -1;

        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage(topic, -1);
            PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());

            Thread.sleep(2);
            if (i == 49)
                timeMid = System.currentTimeMillis();
        }

        Thread.sleep(2 * 1000);

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, 0);
        Assert.assertEquals(CQType.SimpleCQ, consumeQueue.getCQType());
        //check the consume queue
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(0, consumeQueue.getOffsetInQueueByTime(0));
        Assert.assertEquals(50, consumeQueue.getOffsetInQueueByTime(timeMid));
        Assert.assertEquals(99, consumeQueue.getOffsetInQueueByTime(timeMid + Integer.MAX_VALUE));
        Assert.assertEquals(100, consumeQueue.getMaxOffsetInQueue());
        //check the messagestore
        Assert.assertEquals(100, messageStore.getMessageTotalInQueue(topic, 0));
        Assert.assertEquals(consumeQueue.getMinOffsetInQueue(), messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(consumeQueue.getMaxOffsetInQueue(), messageStore.getMaxOffsetInQueue(topic, 0));
        for (int i = -100; i < 100; i += 20) {
            Assert.assertEquals(consumeQueue.getOffsetInQueueByTime(timeMid + i), messageStore.getOffsetInQueueByTime(topic, 0, timeMid + i));
        }

        //check the message time
        long earlistMessageTime = messageStore.getEarliestMessageTime(topic, 0);
        Assert.assertTrue(earlistMessageTime > timeStart - 10);
        Assert.assertTrue(earlistMessageTime < timeStart + 10);
        long messageStoreTime = messageStore.getMessageStoreTimeStamp(topic, 0, 50);
        Assert.assertTrue(messageStoreTime > timeMid - 10);
        Assert.assertTrue(messageStoreTime < timeMid + 10);
        long commitLogOffset = messageStore.getCommitLogOffsetInQueue(topic, 0, 50);
        Assert.assertTrue(commitLogOffset >= messageStore.getMinPhyOffset());
        Assert.assertTrue(commitLogOffset <= messageStore.getMaxPhyOffset());

        Assert.assertFalse(messageStore.checkInDiskByConsumeOffset(topic, 0, 50));
    }

    @Test
    public void testDispatchBuildBatchConsumeQueue() throws Exception {
        String topic = "testDispatchBuildBatchConsumeQueue";
        int batchNum = 10;
        long timeStart = System.currentTimeMillis();
        long timeMid = -1;

        createTopic(topic, CQType.BatchCQ, messageStore);

        for (int i = 0; i < 100; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(topic, batchNum));
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Thread.sleep(2);
            if (i == 29)
                timeMid = System.currentTimeMillis();
        }

        Thread.sleep(2 * 1000);

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, 0);
        Assert.assertEquals(CQType.BatchCQ, consumeQueue.getCQType());

        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(1000, consumeQueue.getMaxOffsetInQueue());

        //check the message store
        Assert.assertEquals(1000, messageStore.getMessageTotalInQueue(topic, 0));
        Assert.assertEquals(consumeQueue.getMinOffsetInQueue(), messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(consumeQueue.getMaxOffsetInQueue(), messageStore.getMaxOffsetInQueue(topic, 0));
        for (int i = -100; i < 100; i += 20) {
            Assert.assertEquals(consumeQueue.getOffsetInQueueByTime(timeMid + i), messageStore.getOffsetInQueueByTime(topic, 0, timeMid + i));
        }

        //check the message time
        long earlistMessageTime = messageStore.getEarliestMessageTime(topic, 0);
        Assert.assertTrue(earlistMessageTime > timeStart - 20);
        Assert.assertTrue(earlistMessageTime < timeStart + 20);
        long messageStoreTime = messageStore.getMessageStoreTimeStamp(topic, 0, 300);
        Assert.assertTrue(messageStoreTime > timeMid - 20);
        Assert.assertTrue(messageStoreTime < timeMid + 20);
        long commitLogOffset = messageStore.getCommitLogOffsetInQueue(topic, 0, 300);
        Assert.assertTrue(commitLogOffset >= messageStore.getMinPhyOffset());
        Assert.assertTrue(commitLogOffset <= messageStore.getMaxPhyOffset());

        Assert.assertFalse(messageStore.checkInDiskByConsumeOffset(topic, 0, 300));

        //get the message Normally
        GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 0, 10 * batchNum, null);
        Assert.assertEquals(10, getMessageResult.getMessageMapedList().size());
        for (int i = 0; i < 10; i++) {
            SelectMappedBufferResult sbr = getMessageResult.getMessageMapedList().get(i);
            MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer());
            short tmpBatchNum = Short.parseShort(messageExt.getProperty(MessageConst.PROPERTY_INNER_NUM));
            Assert.assertEquals(i * batchNum, Long.parseLong(messageExt.getProperty(MessageConst.PROPERTY_INNER_BASE)));
            Assert.assertEquals(batchNum, tmpBatchNum);
        }
    }

    @Test
    public void testGetBatchMessageWithinNumber() throws Exception {
        String topic = UUID.randomUUID().toString();

        createTopic(topic, CQType.BatchCQ, messageStore);

        int batchNum = 20;
        for (int i = 0; i < 200; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(topic, batchNum));
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(i * batchNum, putMessageResult.getAppendMessageResult().getLogicsOffset());
            Assert.assertEquals(batchNum, putMessageResult.getAppendMessageResult().getMsgNum());
        }

        Thread.sleep(2 * 1000);

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, 0);
        Assert.assertEquals(CQType.BatchCQ, consumeQueue.getCQType());
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(200 * batchNum, consumeQueue.getMaxOffsetInQueue());

        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, 1, Integer.MAX_VALUE, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(batchNum, getMessageResult.getNextBeginOffset());
            Assert.assertEquals(1, getMessageResult.getMessageMapedList().size());
            Assert.assertEquals(batchNum, getMessageResult.getMessageCount());
            SelectMappedBufferResult sbr = getMessageResult.getMessageMapedList().get(0);
            MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer());
            short tmpBatchNum = Short.parseShort(messageExt.getProperty(MessageConst.PROPERTY_INNER_NUM));
            Assert.assertEquals(0, messageExt.getQueueOffset());
            Assert.assertEquals(batchNum, tmpBatchNum);
        }
        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, 39, Integer.MAX_VALUE, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(1, getMessageResult.getMessageMapedList().size());
            Assert.assertEquals(batchNum, getMessageResult.getNextBeginOffset());
            Assert.assertEquals(batchNum, getMessageResult.getMessageCount());

        }

        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, 60, Integer.MAX_VALUE, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(3, getMessageResult.getMessageMapedList().size());
            Assert.assertEquals(3 * batchNum, getMessageResult.getNextBeginOffset());
            Assert.assertEquals(3 * batchNum, getMessageResult.getMessageCount());
            for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
                Assert.assertFalse(getMessageResult.getMessageMapedList().get(i).hasReleased());
                SelectMappedBufferResult sbr = getMessageResult.getMessageMapedList().get(i);
                MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer());
                Assert.assertNotNull(messageExt);
                short innerBatchNum = Short.parseShort(messageExt.getProperty(MessageConst.PROPERTY_INNER_NUM));
                Assert.assertEquals(i * batchNum, Long.parseLong(messageExt.getProperty(MessageConst.PROPERTY_INNER_BASE)));
                Assert.assertEquals(batchNum, innerBatchNum);

            }
        }
    }

    @Test
    public void testGetBatchMessageWithinSize() throws Exception {
        String topic = UUID.randomUUID().toString();
        createTopic(topic, CQType.BatchCQ, messageStore);

        int batchNum = 10;
        for (int i = 0; i < 100; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(topic, batchNum));
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(i * 10, putMessageResult.getAppendMessageResult().getLogicsOffset());
            Assert.assertEquals(batchNum, putMessageResult.getAppendMessageResult().getMsgNum());
        }

        Thread.sleep(2 * 1000);

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, 0);
        Assert.assertEquals(CQType.BatchCQ, consumeQueue.getCQType());
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(1000, consumeQueue.getMaxOffsetInQueue());

        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, Integer.MAX_VALUE, 100, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(10, getMessageResult.getNextBeginOffset());
            Assert.assertEquals(1, getMessageResult.getMessageMapedList().size());
            SelectMappedBufferResult sbr = getMessageResult.getMessageMapedList().get(0);
            MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer());
            short tmpBatchNum = Short.valueOf(messageExt.getProperty(MessageConst.PROPERTY_INNER_NUM));
            Assert.assertEquals(0, messageExt.getQueueOffset());
            Assert.assertEquals(batchNum, tmpBatchNum);
        }
        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, Integer.MAX_VALUE, 2048, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(1, getMessageResult.getMessageMapedList().size());
            Assert.assertEquals(10, getMessageResult.getNextBeginOffset());

        }

        {
            GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 5, Integer.MAX_VALUE, 4096, null);
            Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
            Assert.assertEquals(3, getMessageResult.getMessageMapedList().size());
            Assert.assertEquals(30, getMessageResult.getNextBeginOffset());
            for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
                Assert.assertFalse(getMessageResult.getMessageMapedList().get(i).hasReleased());
                SelectMappedBufferResult sbr = getMessageResult.getMessageMapedList().get(i);
                MessageExt messageExt = MessageDecoder.decode(sbr.getByteBuffer());
                short tmpBatchNum = Short.valueOf(messageExt.getProperty(MessageConst.PROPERTY_INNER_NUM));
                Assert.assertEquals(i * batchNum, Long.parseLong(messageExt.getProperty(MessageConst.PROPERTY_INNER_BASE)));
                Assert.assertEquals(batchNum, tmpBatchNum);

            }
        }
    }

    private void createTopic(String topic, CQType cqType, MessageStore messageStore) {
        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        TopicConfig topicConfigToBeAdded = new TopicConfig();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(TopicAttributes.queueType.getName(), cqType.toString());
        topicConfigToBeAdded.setTopicName(topic);
        topicConfigToBeAdded.setAttributes(attributes);

        topicConfigTable.put(topic, topicConfigToBeAdded);
        ((DefaultMessageStore)messageStore).setTopicConfigTable(topicConfigTable);
    }

}
