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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class QueryMsgTraceByIdSubCommandTest {

    private static final int NAME_SERVER_PORT = 45677;

    private static final int BROKER_PORT = 45676;

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    @Before
    public void before() {
        brokerMocker = startOneBroker();
        nameServerMocker = startNameServer();
    }

    @After
    public void after() {
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testExecute() throws SubCommandException {
        QueryMsgTraceByIdSubCommand cmd = new QueryMsgTraceByIdSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-i AC1FF54E81C418B4AAC24F92E1E00000"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }

    private ServerResponseMocker startNameServer() {
        int nameServerPort = NAME_SERVER_PORT;
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:" + nameServerPort);
        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> dataList = new ArrayList<>();
        HashMap<Long, String> brokerAddress = new HashMap<>();
        brokerAddress.put(1L, "127.0.0.1:" + BROKER_PORT);
        BrokerData brokerData = new BrokerData("mockCluster", "mockBrokerName", brokerAddress);
        brokerData.setBrokerName("mockBrokerName");
        dataList.add(brokerData);
        topicRouteData.setBrokerDatas(dataList);

        List<QueueData> queueDatas = new ArrayList<>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("mockBrokerName");
        queueData.setPerm(1);
        queueData.setReadQueueNums(1);
        queueData.setTopicSysFlag(1);
        queueData.setWriteQueueNums(1);
        queueDatas.add(queueData);
        topicRouteData.setQueueDatas(queueDatas);

        return ServerResponseMocker.startServer(nameServerPort, topicRouteData.encode());
    }

    private ServerResponseMocker startOneBroker() {
        try {
            MessageExt messageExt = new MessageExt();
            messageExt.setTopic("mockTopic");
            messageExt.setBody(new byte[100]);
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
            byte[] body = MessageDecoder.encode(messageExt, false);

            HashMap<String, String> extMap = new HashMap<>();
            extMap.put("indexLastUpdateTimestamp", String.valueOf(System.currentTimeMillis()));
            extMap.put("indexLastUpdatePhyoffset", String.valueOf(System.currentTimeMillis()));
            // start broker
            return ServerResponseMocker.startServer(BROKER_PORT, body, extMap);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}