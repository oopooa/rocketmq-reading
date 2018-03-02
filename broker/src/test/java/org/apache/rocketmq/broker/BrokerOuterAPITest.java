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

import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class BrokerOuterAPITest {
    @Mock
    private ChannelHandlerContext handlerContext;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private MessageStore messageStore;
    private String clusterName = "clusterName";
    private String brokerName = "brokerName";
    private String brokerAddr = "brokerAddr";
    private long brokerId = 0L;
    private String nameserver1 = "127.0.0.1";
    private String nameserver2 = "127.0.0.2";
    private String nameserver3 = "127.0.0.3";
    private int timeOut = 3000;

    @Mock
    private NettyRemotingClient nettyRemotingClient;

    private BrokerOuterAPI brokerOuterAPI;

    public void init() throws Exception {
        brokerOuterAPI = new BrokerOuterAPI(new NettyClientConfig(), null);
        Field field = BrokerOuterAPI.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(brokerOuterAPI, nettyRemotingClient);
    }

    @Test
    public void test_register_normal() throws Exception {
        init();
        brokerOuterAPI.start();

        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();

        when(nettyRemotingClient.getNameServerAddressList()).thenReturn(Lists.asList(nameserver1, nameserver2, new String[] {nameserver3}));
        when(nettyRemotingClient.invokeSync(anyString(), any(RemotingCommand.class), anyLong())).thenReturn(response);
        List<RegisterBrokerResult> registerBrokerResultList = brokerOuterAPI.registerBrokerAll(clusterName, brokerAddr, brokerName, brokerId, "hasServerAddr", topicConfigSerializeWrapper, Lists.<String>newArrayList(), false, timeOut, true);

        assertEquals(3, registerBrokerResultList.size());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void test_register_timeout() throws Exception {
        init();
        brokerOuterAPI.start();

        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();

        when(nettyRemotingClient.getNameServerAddressList()).thenReturn(Lists.asList(nameserver1, nameserver2, new String[] {nameserver3}));
        when(nettyRemotingClient.invokeSync(anyString(), any(RemotingCommand.class), anyLong())).thenAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock invocation) throws Throwable {
                if (invocation.getArgument(0) == nameserver1) {
                    return response;
                } else if (invocation.getArgument(0) == nameserver2) {
                    return response;
                } else if (invocation.getArgument(0) == nameserver3) {
                    TimeUnit.MILLISECONDS.sleep(timeOut + 20);
                    return response;
                }
                return response;
            }
        });
        List<RegisterBrokerResult> registerBrokerResultList = brokerOuterAPI.registerBrokerAll(clusterName, brokerAddr, brokerName, brokerId, "hasServerAddr", topicConfigSerializeWrapper, Lists.<String>newArrayList(), false, timeOut, true);

        assertEquals(2, registerBrokerResultList.size());
    }

    private RemotingCommand buildResponse(Boolean changed) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(QueryDataVersionResponseHeader.class);
        final QueryDataVersionResponseHeader responseHeader = (QueryDataVersionResponseHeader) response.readCustomHeader();
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        responseHeader.setChanged(changed);
        return response;
    }
}
