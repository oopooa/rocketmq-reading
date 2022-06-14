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
package org.apache.rocketmq.proxy.processor;

import io.netty.channel.Channel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.ServiceManagerFactory;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.proxy.service.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.service.transaction.TransactionId;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultMessagingProcessor extends AbstractStartAndShutdown implements MessagingProcessor {

    protected final ServiceManager serviceManager;
    protected final ProducerProcessor producerProcessor;
    protected final ConsumerProcessor consumerProcessor;
    protected final TransactionProcessor transactionProcessor;
    protected final ClientProcessor clientProcessor;

    protected final ThreadPoolExecutor producerProcessorExecutor;
    protected final ThreadPoolExecutor consumerProcessorExecutor;

    protected DefaultMessagingProcessor(ServiceManager serviceManager) {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        this.producerProcessorExecutor = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getProducerProcessorThreadPoolNums(),
            proxyConfig.getProducerProcessorThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "ProducerProcessorExecutor",
            proxyConfig.getProducerProcessorThreadPoolQueueCapacity()
        );
        this.consumerProcessorExecutor = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getConsumerProcessorThreadPoolNums(),
            proxyConfig.getConsumerProcessorThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "ConsumerProcessorExecutor",
            proxyConfig.getConsumerProcessorThreadPoolQueueCapacity()
        );

        this.serviceManager = serviceManager;
        this.producerProcessor = new ProducerProcessor(this, serviceManager, this.producerProcessorExecutor);
        this.consumerProcessor = new ConsumerProcessor(this, serviceManager, this.consumerProcessorExecutor);
        this.transactionProcessor = new TransactionProcessor(this, serviceManager);
        this.clientProcessor = new ClientProcessor(this, serviceManager);

        this.init();
    }

    public static DefaultMessagingProcessor createForLocalMode(BrokerController brokerController) {
        return createForLocalMode(brokerController, null);
    }

    public static DefaultMessagingProcessor createForLocalMode(BrokerController brokerController, RPCHook rpcHook) {
        return new DefaultMessagingProcessor(ServiceManagerFactory.createForLocalMode(brokerController, rpcHook));
    }

    public static DefaultMessagingProcessor createForClusterMode() {
        return createForClusterMode(null);
    }

    public static DefaultMessagingProcessor createForClusterMode(RPCHook rpcHook) {
        return new DefaultMessagingProcessor(ServiceManagerFactory.createForClusterMode(rpcHook));
    }

    protected void init() {
        this.appendStartAndShutdown(this.serviceManager);
        this.appendShutdown(this.producerProcessorExecutor::shutdown);
        this.appendShutdown(this.consumerProcessorExecutor::shutdown);
    }

    @Override
    public SubscriptionGroupConfig getSubscriptionGroupConfig(ProxyContext ctx, String consumerGroupName) {
        return this.serviceManager.getMetadataService().getSubscriptionGroupConfig(consumerGroupName);
    }

    @Override
    public ProxyTopicRouteData getTopicRouteDataForProxy(ProxyContext ctx, List<Address> requestHostAndPortList,
        String topicName) throws Exception {
        return this.serviceManager.getTopicRouteService().getTopicRouteForProxy(requestHostAndPortList, topicName);
    }

    @Override
    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, QueueSelector queueSelector,
        String producerGroup, int sysFlag, List<Message> msg, long timeoutMillis) {
        return this.producerProcessor.sendMessage(ctx, queueSelector, producerGroup, sysFlag, msg, timeoutMillis);
    }

    @Override
    public CompletableFuture<RemotingCommand> forwardMessageToDeadLetterQueue(ProxyContext ctx, ReceiptHandle handle,
        String messageId, String groupName, String topicName, long timeoutMillis) {
        return this.producerProcessor.forwardMessageToDeadLetterQueue(ctx, handle, messageId, groupName, topicName, timeoutMillis);
    }

    @Override
    public void endTransaction(ProxyContext ctx, TransactionId transactionId, String messageId,
        String producerGroup, TransactionStatus transactionStatus, boolean fromTransactionCheck,
        long timeoutMillis) throws MQBrokerException, RemotingException, InterruptedException {
        this.transactionProcessor.endTransaction(ctx, transactionId, messageId, producerGroup, transactionStatus, fromTransactionCheck, timeoutMillis);
    }

    @Override
    public CompletableFuture<PopResult> popMessage(
        ProxyContext ctx,
        QueueSelector queueSelector,
        String consumerGroup,
        String topic,
        int maxMsgNums,
        long invisibleTime,
        long pollTime,
        int initMode,
        SubscriptionData subscriptionData,
        boolean fifo,
        PopMessageResultFilter popMessageResultFilter,
        long timeoutMillis
    ) {
        return this.consumerProcessor.popMessage(ctx, queueSelector, consumerGroup, topic, maxMsgNums,
            invisibleTime, pollTime, initMode, subscriptionData, fifo, popMessageResultFilter, timeoutMillis);
    }

    @Override
    public CompletableFuture<AckResult> ackMessage(ProxyContext ctx, ReceiptHandle handle, String messageId,
        String consumerGroup, String topic, long timeoutMillis) {
        return this.consumerProcessor.ackMessage(ctx, handle, messageId, consumerGroup, topic, timeoutMillis);
    }

    @Override
    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle, String messageId,
        String groupName, String topicName, long invisibleTime, long timeoutMillis) {
        return this.consumerProcessor.changeInvisibleTime(ctx, handle, messageId, groupName, topicName, invisibleTime, timeoutMillis);
    }

    @Override
    public CompletableFuture<PullResult> pullMessage(ProxyContext ctx, SelectableMessageQueue selectableMessageQueue,
        String consumerGroup, long queueOffset, int maxMsgNums, int sysFlag, long commitOffset,
        long suspendTimeoutMillis, SubscriptionData subscriptionData, long timeoutMillis) {
        return this.consumerProcessor.pullMessage(ctx, selectableMessageQueue, consumerGroup, queueOffset, maxMsgNums,
            sysFlag, commitOffset, suspendTimeoutMillis, subscriptionData, timeoutMillis);
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx, SelectableMessageQueue selectableMessageQueue,
        String consumerGroup, long commitOffset, long timeoutMillis) {
        return this.consumerProcessor.updateConsumerOffset(ctx, selectableMessageQueue, consumerGroup, commitOffset, timeoutMillis);
    }

    @Override
    public CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext ctx, Set<SelectableMessageQueue> mqSet,
        String consumerGroup, String clientId, long timeoutMillis) {
        return this.consumerProcessor.lockBatchMQ(ctx, mqSet, consumerGroup, clientId, timeoutMillis);
    }

    @Override
    public CompletableFuture<Void> unlockBatchMQ(ProxyContext ctx, Set<SelectableMessageQueue> mqSet, String consumerGroup,
        String clientId, long timeoutMillis) {
        return this.consumerProcessor.unlockBatchMQ(ctx, mqSet, consumerGroup, clientId, timeoutMillis);
    }

    @Override
    public void registerProducer(ProxyContext ctx, String producerGroup, ClientChannelInfo clientChannelInfo) {
        this.clientProcessor.registerProducer(ctx, producerGroup, clientChannelInfo);
    }

    @Override
    public void unRegisterProducer(ProxyContext ctx, String producerGroup, ClientChannelInfo clientChannelInfo) {
        this.clientProcessor.unRegisterProducer(ctx, producerGroup, clientChannelInfo);
    }

    @Override
    public Channel findProducerChannel(ProxyContext ctx, String producerGroup, String clientId) {
        return this.clientProcessor.findProducerChannel(ctx, producerGroup, clientId);
    }

    @Override
    public void registerProducerListener(ProducerChangeListener producerChangeListener) {
        this.clientProcessor.registerProducerChangeListener(producerChangeListener);
    }

    @Override
    public void registerConsumer(ProxyContext ctx, String consumerGroup, ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        Set<SubscriptionData> subList) {
        this.clientProcessor.registerConsumer(ctx, consumerGroup, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList);
    }

    @Override
    public ClientChannelInfo findConsumerChannel(ProxyContext ctx, String consumerGroup, String clientId) {
        return this.clientProcessor.findConsumerChannel(ctx, consumerGroup, clientId);
    }

    @Override
    public void unRegisterConsumer(ProxyContext ctx, String consumerGroup, ClientChannelInfo clientChannelInfo) {
        this.clientProcessor.unRegisterConsumer(ctx, consumerGroup, clientChannelInfo);
    }

    @Override
    public void registerConsumerListener(ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.clientProcessor.registerConsumerIdsChangeListener(consumerIdsChangeListener);
    }

    @Override
    public ConsumerGroupInfo getConsumerGroupInfo(String consumerGroup) {
        return this.clientProcessor.getConsumerGroupInfo(consumerGroup);
    }

    @Override
    public void addTransactionSubscription(ProxyContext ctx, String producerGroup, String topic) {
        this.transactionProcessor.addTransactionSubscription(ctx, producerGroup, topic);
    }

    @Override
    public ProxyRelayService getProxyRelayService() {
        return this.serviceManager.getProxyRelayService();
    }

    @Override
    public MetadataService getMetadataService() {
        return this.serviceManager.getMetadataService();
    }
}
