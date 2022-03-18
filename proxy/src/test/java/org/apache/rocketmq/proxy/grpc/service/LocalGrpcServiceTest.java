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

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.ChangeInvisibleDurationRequest;
import apache.rocketmq.v1.ChangeInvisibleDurationResponse;
import apache.rocketmq.v1.ConsumerData;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.ProducerData;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryOffsetPolicy;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import io.grpc.Context;
import io.grpc.Metadata;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.AckMessageProcessor;
import org.apache.rocketmq.broker.processor.ChangeInvisibleTimeProcessor;
import org.apache.rocketmq.broker.processor.ClientManageProcessor;
import org.apache.rocketmq.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.apache.rocketmq.proxy.connector.transaction.TransactionId;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(MockitoJUnitRunner.class)
public class LocalGrpcServiceTest extends InitConfigAndLoggerTest {
    private LocalGrpcService localGrpcService;
    @Mock
    private SendMessageProcessor sendMessageProcessorMock;
    @Mock
    private PopMessageProcessor popMessageProcessorMock;
    @Mock
    private PullMessageProcessor pullMessageProcessorMock;
    @Mock
    private BrokerController brokerControllerMock;

    private Metadata metadata;

    @Before
    public void setUp() throws Exception {
        super.before();
        Mockito.when(brokerControllerMock.getSendMessageProcessor()).thenReturn(sendMessageProcessorMock);
        Mockito.when(brokerControllerMock.getPopMessageProcessor()).thenReturn(popMessageProcessorMock);
        Mockito.when(brokerControllerMock.getPullMessageProcessor()).thenReturn(pullMessageProcessorMock);
        Mockito.when(brokerControllerMock.getBrokerConfig()).thenReturn(new BrokerConfig());
        localGrpcService = new LocalGrpcService(brokerControllerMock);
        metadata = new Metadata();
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, "1.1.1.1");
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, "0.0.0.0");
        metadata.put(InterceptorConstants.LANGUAGE, "JAVA");
    }

    @Test
    public void testHeartbeatProducerData() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        ClientManageProcessor clientManageProcessorMock = Mockito.mock(ClientManageProcessor.class);
        Mockito.when(clientManageProcessorMock.heartBeat(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        Mockito.when(brokerControllerMock.getClientManageProcessor()).thenReturn(clientManageProcessorMock);
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
            .setClientId("test-client")
            .setProducerData(ProducerData.newBuilder()
                .setGroup(Resource.newBuilder()
                    .setName("group")
                    .build())
                .build())
            .build();
        CompletableFuture<HeartbeatResponse> grpcFuture = localGrpcService.heartbeat(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        HeartbeatResponse r =  grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode())
            .isEqualTo(Code.OK.getNumber());
    }

    @Test
    public void testHeartbeatConsumerData() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        ClientManageProcessor clientManageProcessorMock = Mockito.mock(ClientManageProcessor.class);
        Mockito.when(clientManageProcessorMock.heartBeat(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        Mockito.when(brokerControllerMock.getClientManageProcessor()).thenReturn(clientManageProcessorMock);
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
            .setClientId("test-client")
            .setConsumerData(ConsumerData.newBuilder()
                .setGroup(Resource.newBuilder()
                    .setName("group")
                    .build())
                .build())
            .build();
        CompletableFuture<HeartbeatResponse> grpcFuture = localGrpcService.heartbeat(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        HeartbeatResponse r =  grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode())
            .isEqualTo(Code.OK.getNumber());
    }

    @Test
    public void testSendMessageError() throws Exception {
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

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        SendMessageResponse r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode())
            .isEqualTo(Code.INTERNAL.getNumber());
    }

    @Test
    public void testSendMessageWriteAndFlush() throws Exception {
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

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        assertThat(grpcFuture.isDone()).isFalse();
    }

    @Test
    public void testSendMessageWithException() throws Exception {
        Mockito.when(sendMessageProcessorMock.asyncProcessRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenThrow(new RemotingCommandException("test"));
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .setMessage(Message.newBuilder()
                .setSystemAttribute(SystemAttribute.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .build();

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        assertThatThrownBy(() -> {
            try {
                grpcFuture.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }).isInstanceOf(RemotingCommandException.class);
    }

    @Test
    public void testReceiveMessageSuccess() throws Exception {
        long invisibleTime = 1000L;
        String topic = "topic";
        byte[] body = "123".getBytes(StandardCharsets.UTF_8);
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(topic);
        messageExt.setQueueOffset(0L);
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setBody(body);
        messageExt.putUserProperty("key", "value");
        PopMessageResponseHeader responseHeader = new PopMessageResponseHeader();
        responseHeader.setInvisibleTime(invisibleTime);
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, responseHeader);
        remotingCommand.setBody(MessageDecoder.encode(messageExt, true));
        remotingCommand.makeCustomHeaderToNet();
        Mockito.when(popMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(remotingCommand);
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder()
            .setPartition(Partition.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .build())
            .build();
        CompletableFuture<ReceiveMessageResponse> grpcFuture = localGrpcService.receiveMessage(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .withDeadlineAfter(20, TimeUnit.SECONDS, Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryImpl("test")))
                .attach(), request);
        ReceiveMessageResponse r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
        assertThat(r.getMessagesCount()).isEqualTo(1);
        assertThat(Durations.toMillis(r.getInvisibleDuration())).isEqualTo(invisibleTime);
        assertThat(Converter.getResourceNameWithNamespace(r.getMessages(0).getTopic())).isEqualTo(topic);
        assertThat(r.getMessages(0).getBody().toByteArray()).isEqualTo(body);
    }

    @Test
    public void testReceiveMessageSuccessWriteAndFlush() throws Exception {
        Mockito.when(popMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(null);
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder().getDefaultInstanceForType();
        CompletableFuture<ReceiveMessageResponse> grpcFuture = localGrpcService.receiveMessage(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .withDeadlineAfter(20, TimeUnit.SECONDS, Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryImpl("test")))
                .attach(), request);
        assertThat(grpcFuture.isDone()).isFalse();
    }

    @Test
    public void testAckMessage() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        AckMessageProcessor ackMessageProcessorMock = Mockito.mock(AckMessageProcessor.class);
        Mockito.when(brokerControllerMock.getAckMessageProcessor()).thenReturn(ackMessageProcessorMock);
        Mockito.when(ackMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        AckMessageRequest request = AckMessageRequest.newBuilder().setReceiptHandle(
            ReceiptHandle.builder()
                .startOffset(0L)
                .retrieveTime(0L)
                .invisibleTime(1000L)
                .nextVisibleTime(1000L)
                .reviveQueueId(0)
                .topicType("topic")
                .brokerName("brokerName")
                .queueId(0)
                .offset(0L)
                .build().encode()
        ).build();
        CompletableFuture<AckMessageResponse> grpcFuture = localGrpcService.ackMessage(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        AckMessageResponse r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
    }

    @Test
    public void testNackMessage() throws Exception {
        ChangeInvisibleTimeResponseHeader responseHeader = new ChangeInvisibleTimeResponseHeader();
        responseHeader.setInvisibleTime(1000L);
        responseHeader.setPopTime(0L);
        responseHeader.setReviveQid(0);
        RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, responseHeader);

        ChangeInvisibleTimeProcessor changeInvisibleTimeProcessor = Mockito.mock(ChangeInvisibleTimeProcessor.class);
        Mockito.when(brokerControllerMock.getChangeInvisibleTimeProcessor()).thenReturn(changeInvisibleTimeProcessor);
        Mockito.when(changeInvisibleTimeProcessor.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        NackMessageRequest request = NackMessageRequest.newBuilder().setReceiptHandle(
            ReceiptHandle.builder()
                .startOffset(0L)
                .retrieveTime(0L)
                .invisibleTime(1000L)
                .nextVisibleTime(1000L)
                .reviveQueueId(0)
                .topicType("topic")
                .brokerName("brokerName")
                .queueId(0)
                .offset(0L)
                .build().encode()
        ).build();
        CompletableFuture<NackMessageResponse> grpcFuture = localGrpcService.nackMessage(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        NackMessageResponse r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
    }

    @Test
    public void testForwardMessageToDeadLetterQueue() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(response);
        Mockito.when(brokerControllerMock.getSendMessageProcessor()).thenReturn(sendMessageProcessorMock);
        Mockito.when(sendMessageProcessorMock.asyncProcessRequest(Mockito.any(ChannelHandlerContext.class),
            Mockito.argThat(argument -> argument.getCode() == RequestCode.CONSUMER_SEND_MSG_BACK)))
            .thenReturn(future);
        ForwardMessageToDeadLetterQueueRequest request = ForwardMessageToDeadLetterQueueRequest.newBuilder()
            .setReceiptHandle(ReceiptHandle.builder()
                .startOffset(0L)
                .retrieveTime(0L)
                .invisibleTime(1000L)
                .nextVisibleTime(1000L)
                .reviveQueueId(0)
                .topicType("topic")
                .brokerName("brokerName")
                .queueId(0)
                .offset(0L)
                .build().encode())
            .build();
        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> grpcFuture = localGrpcService.forwardMessageToDeadLetterQueue(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        ForwardMessageToDeadLetterQueueResponse r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
    }

    @Test
    public void testEndTransaction() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);

        EndTransactionProcessor endTransactionProcessor = Mockito.mock(EndTransactionProcessor.class);
        Mockito.when(brokerControllerMock.getEndTransactionProcessor()).thenReturn(endTransactionProcessor);
        Mockito.when(endTransactionProcessor.processRequest(Mockito.any(ChannelHandlerContext.class),
            Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        EndTransactionRequest request = EndTransactionRequest.newBuilder()
            .setMessageId("123")
            .setTransactionId(TransactionId.genFromBrokerTransactionId(
                new InetSocketAddress("0.0.0.0", 80), "123", 123, 123
                ).getProxyTransactionId()
            )
            .build();
        CompletableFuture<EndTransactionResponse> grpcFuture = localGrpcService.endTransaction(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        EndTransactionResponse r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
    }

    @Test
    public void testQueryOffset() throws Exception {
        String topic = "test-topic";
        int queueId = 1;

        long maxOffset = 10L;
        long timeOffset = 5L;
        MessageStore messageStore = Mockito.mock(MessageStore.class);
        Mockito.when(brokerControllerMock.getMessageStore()).thenReturn(messageStore);
        Mockito.when(messageStore.getMaxOffsetInQueue(Mockito.eq(topic), Mockito.eq(queueId))).thenReturn(maxOffset);
        Mockito.when(messageStore.getOffsetInQueueByTime(Mockito.eq(topic), Mockito.eq(queueId), Mockito.anyLong())).thenReturn(timeOffset);

        QueryOffsetRequest request = QueryOffsetRequest.newBuilder()
            .setPartition(Partition.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setId(queueId)
                .build())
            .setPolicy(QueryOffsetPolicy.BEGINNING)
            .build();
        CompletableFuture<QueryOffsetResponse> grpcFuture = localGrpcService.queryOffset(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        QueryOffsetResponse r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
        assertThat(r.getOffset()).isEqualTo(0);

        request = QueryOffsetRequest.newBuilder()
            .setPartition(Partition.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setId(queueId)
                .build())
            .setPolicy(QueryOffsetPolicy.END)
            .build();
        grpcFuture = localGrpcService.queryOffset(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
        assertThat(r.getOffset()).isEqualTo(maxOffset);

        request = QueryOffsetRequest.newBuilder()
            .setPartition(Partition.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setId(queueId)
                .build())
            .setTimePoint(Timestamp.newBuilder()
                .setSeconds(1000L)
                .build())
            .setPolicy(QueryOffsetPolicy.TIME_POINT)
            .build();
        grpcFuture = localGrpcService.queryOffset(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
        assertThat(r.getOffset()).isEqualTo(timeOffset);
    }

    @Test
    public void testReportThreadStackTrace() throws Exception {

    }

    @Test
    public void testNotifyClientTermination() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);

        ClientManageProcessor clientManageProcessor = Mockito.mock(ClientManageProcessor.class);
        Mockito.when(brokerControllerMock.getClientManageProcessor()).thenReturn(clientManageProcessor);
        Mockito.when(clientManageProcessor.unregisterClient(Mockito.any(ChannelHandlerContext.class),
            Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        NotifyClientTerminationRequest request = NotifyClientTerminationRequest.newBuilder()
            .setProducerGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .build();
        localGrpcService.notifyClientTermination(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        Mockito.verify(clientManageProcessor, Mockito.times(1))
            .unregisterClient(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class));
    }

    @Test
    public void testChangeInvisibleDuration() throws Exception {
        long invisibleTime = 1000L;
        int queueId = 1;
        long offset = 123L;
        ChangeInvisibleTimeResponseHeader responseHeader = new ChangeInvisibleTimeResponseHeader();
        responseHeader.setInvisibleTime(1000L);
        responseHeader.setPopTime(0L);
        responseHeader.setReviveQid(0);
        RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, responseHeader);

        ChangeInvisibleTimeProcessor changeInvisibleTimeProcessor = Mockito.mock(ChangeInvisibleTimeProcessor.class);
        Mockito.when(brokerControllerMock.getChangeInvisibleTimeProcessor()).thenReturn(changeInvisibleTimeProcessor);
        Mockito.when(changeInvisibleTimeProcessor.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        ChangeInvisibleDurationRequest request = ChangeInvisibleDurationRequest.newBuilder().setReceiptHandle(
            ReceiptHandle.builder()
                .startOffset(0L)
                .retrieveTime(0L)
                .invisibleTime(invisibleTime)
                .nextVisibleTime(1000L)
                .reviveQueueId(0)
                .topicType("topic")
                .brokerName("brokerName")
                .queueId(queueId)
                .offset(offset)
                .build().encode()
        ).build();
        CompletableFuture<ChangeInvisibleDurationResponse> grpcFuture = localGrpcService.changeInvisibleDuration(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        ChangeInvisibleDurationResponse r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
        ReceiptHandle handle = ReceiptHandle.decode(r.getReceiptHandle());
        assertThat(handle.getInvisibleTime()).isEqualTo(invisibleTime);
        assertThat(handle.getQueueId()).isEqualTo(queueId);
        assertThat(handle.getOffset()).isEqualTo(offset);
    }

    @Test
    public void testPullMessageSuccess() throws Exception {
        String topic = "topic";
        byte[] body = "123".getBytes(StandardCharsets.UTF_8);
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(topic);
        messageExt.setQueueOffset(0L);
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setBody(body);
        messageExt.putUserProperty("key", "value");

        long minOffset = 1L;
        long nextOffset = 3L;
        long maxOffset = 10L;
        PullMessageResponseHeader responseHeader = new PullMessageResponseHeader();
        responseHeader.setMinOffset(minOffset);
        responseHeader.setNextBeginOffset(nextOffset);
        responseHeader.setMaxOffset(maxOffset);
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, responseHeader);
        remotingCommand.setBody(MessageDecoder.encode(messageExt, true));
        remotingCommand.makeCustomHeaderToNet();
        Mockito.when(pullMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(remotingCommand);
        PullMessageRequest request = PullMessageRequest.newBuilder().getDefaultInstanceForType();
        CompletableFuture<PullMessageResponse> grpcFuture = localGrpcService.pullMessage(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .withDeadlineAfter(20, TimeUnit.SECONDS, Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryImpl("test")))
                .attach(), request);
        PullMessageResponse r = grpcFuture.get();
        assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
        assertThat(r.getMessagesCount()).isEqualTo(1);
        assertThat(Converter.getResourceNameWithNamespace(r.getMessages(0).getTopic())).isEqualTo(topic);
        assertThat(r.getMessages(0).getBody().toByteArray()).isEqualTo(body);
        assertThat(r.getMinOffset()).isEqualTo(minOffset);
        assertThat(r.getNextOffset()).isEqualTo(nextOffset);
        assertThat(r.getMaxOffset()).isEqualTo(maxOffset);
    }
}