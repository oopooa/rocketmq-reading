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

package org.apache.rocketmq.proxy.grpc.service;

import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
import com.google.rpc.Code;
import io.grpc.Context;
import io.grpc.Metadata;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.proxy.configuration.InitConfigurationTest;
import org.apache.rocketmq.proxy.grpc.common.InterceptorConstants;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class LocalGrpcServiceTest extends InitConfigurationTest {
    private LocalGrpcService localGrpcService;
    @Mock
    SendMessageProcessor sendMessageProcessorMock;

    @Before
    public void setUp() throws Exception {
        super.before();
        BrokerController brokerControllerMock = Mockito.mock(BrokerController.class);
        Mockito.when(brokerControllerMock.getSendMessageProcessor()).thenReturn(sendMessageProcessorMock);
        localGrpcService = new LocalGrpcService(brokerControllerMock);
    }

    @Test
    public void testSendMessageError() throws RemotingCommandException {
        String remark = "store putMessage return null";
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, remark);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(response);
        Mockito.when(sendMessageProcessorMock.asyncProcessRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(future);
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .setMessage(Message.newBuilder()
                .setSystemAttribute(SystemAttribute.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .build();
        Metadata metadata = new Metadata();
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, "1.1.1.1");
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, "0.0.0.0");

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        grpcFuture.thenAccept(r -> {
            assertThat(r.getCommon().getStatus().getCode())
                .isEqualTo(Code.INTERNAL.getNumber());
            assertThat(r.getCommon().getStatus().getMessage())
                .isEqualTo(remark);
        });
    }

    @Test
    public void testSendMessageWriteAndFlush() throws RemotingCommandException {
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(null);
        Mockito.when(sendMessageProcessorMock.asyncProcessRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(future);
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .setMessage(Message.newBuilder()
                .setSystemAttribute(SystemAttribute.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .build();
        Metadata metadata = new Metadata();
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, "1.1.1.1");
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, "0.0.0.0");

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        grpcFuture.thenAccept(r -> assertThat(r).isNull());
    }

    @Test
    public void testSendMessageWithException() throws RemotingCommandException {
        Mockito.when(sendMessageProcessorMock.asyncProcessRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenThrow(new RemotingCommandException("test"));
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .setMessage(Message.newBuilder()
                .setSystemAttribute(SystemAttribute.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .build();
        Metadata metadata = new Metadata();
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, "1.1.1.1");
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, "0.0.0.0");

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        grpcFuture.thenAccept(r -> assertThat(r).isNull()).exceptionally(e -> {
            assertThat(e).isInstanceOf(RemotingCommandException.class);
            return null;
        });
    }
}