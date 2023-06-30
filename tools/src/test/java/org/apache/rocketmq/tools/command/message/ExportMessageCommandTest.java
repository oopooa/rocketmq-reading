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
package org.apache.rocketmq.tools.command.message;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExportMessageCommandTest {
    private static ExportMessageCommand exportMessageCommand;

    private static final PullResult PULL_RESULT = mockPullResult();

    private static PullResult mockPullResult() {
        MessageExt msg = new MessageExt();
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic("topic1");
        messageExt.setBrokerName("broker-a");
        messageExt.setQueueId(1);
        msg.setBody("{'age':1}".getBytes(StandardCharsets.UTF_8));
        messageExt.setBornHost(new InetSocketAddress(10910));
        messageExt.setStoreHost(new InetSocketAddress(10910));
        messageExt.setDeliverTimeMs(1L);
        List<MessageExt> msgFoundList = new ArrayList<>();
        msgFoundList.add(msg);
        return new PullResult(PullStatus.FOUND, 1, 0, 1, msgFoundList);
    }

    @BeforeClass
    public static void init() throws MQClientException, RemotingException, MQBrokerException, InterruptedException,
        NoSuchFieldException, IllegalAccessException {
        exportMessageCommand = new ExportMessageCommand();
        DefaultMQPullConsumer defaultMQPullConsumer = mock(DefaultMQPullConsumer.class);

        assignPullResult(defaultMQPullConsumer);
        when(defaultMQPullConsumer.minOffset(any(MessageQueue.class))).thenReturn(Long.valueOf(0));
        when(defaultMQPullConsumer.maxOffset(any(MessageQueue.class))).thenReturn(Long.valueOf(1));

        final Set<MessageQueue> mqList = new HashSet<>();
        mqList.add(new MessageQueue("topic1", "broker-a", 1));
        when(defaultMQPullConsumer.fetchSubscribeMessageQueues(anyString())).thenReturn(mqList);

        Field producerField = ExportMessageCommand.class.getDeclaredField("defaultMQPullConsumer");
        producerField.setAccessible(true);
        producerField.set(exportMessageCommand, defaultMQPullConsumer);
    }

    @AfterClass
    public static void terminate() {
    }

    private static void assignPullResult() {
        assignPullResult(null);
    }

    private static void assignPullResult(DefaultMQPullConsumer defaultMQPullConsumer) {
        try {
            if (defaultMQPullConsumer == null) {
                Field producerField = ExportMessageCommand.class.getDeclaredField("defaultMQPullConsumer");
                producerField.setAccessible(true);
                defaultMQPullConsumer = (DefaultMQPullConsumer) producerField.get(exportMessageCommand);
            }
            when(defaultMQPullConsumer.pull(any(MessageQueue.class), anyString(), anyLong(), anyInt()))
                .thenReturn(PULL_RESULT);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testExecuteDefault() throws SubCommandException {
        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-t topic1", "-n localhost:9876"};
        assignPullResult();
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + exportMessageCommand.commandName(),
            subargs, exportMessageCommand.buildCommandlineOptions(options), new DefaultParser());
        exportMessageCommand.execute(commandLine, options, null);

        System.setOut(out);
        String s = new String(bos.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertTrue(s.contains("100%"));
    }

    @Test
    public void testExecuteJSON() throws SubCommandException {
        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-t topic1", "-n localhost:9876", "-f json"};
        assignPullResult();
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + exportMessageCommand.commandName(),
            subargs, exportMessageCommand.buildCommandlineOptions(options), new DefaultParser());
        exportMessageCommand.execute(commandLine, options, null);

        System.setOut(out);
        String s = new String(bos.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertTrue(s.contains("100%"));
    }
}