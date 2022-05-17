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
package org.apache.rocketmq.controller.impl;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.ControllerConfig;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;

public class DefaultBrokerHeartbeatManager implements BrokerHeartbeatManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private static final long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
    private final ScheduledExecutorService scheduledService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DefaultBrokerHeartbeatManager_scheduledService_"));
    private final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactoryImpl("DefaultBrokerHeartbeatManager_executorService_"));

    private final ControllerConfig controllerConfig;
    private final Map<BrokerAddrInfo/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
    private final List<BrokerLifecycleListener> brokerLifecycleListeners;

    public DefaultBrokerHeartbeatManager(final ControllerConfig controllerConfig) {
        this.controllerConfig = controllerConfig;
        this.brokerLiveTable = new ConcurrentHashMap<>(256);
        this.brokerLifecycleListeners = new ArrayList<>();
    }

    @Override
    public void start() {
        this.scheduledService.scheduleAtFixedRate(this::scanNotActiveBroker, 2000, this.controllerConfig.getScanNotActiveBrokerInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        this.scheduledService.shutdown();
        this.executor.shutdown();
    }

    public void scanNotActiveBroker() {
        try {
            log.info("start scanNotActiveBroker");
            final Iterator<Map.Entry<BrokerAddrInfo, BrokerLiveInfo>> iterator = this.brokerLiveTable.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<BrokerAddrInfo, BrokerLiveInfo> next = iterator.next();
                long last = next.getValue().lastUpdateTimestamp;
                long timeoutMillis = next.getValue().heartbeatTimeoutMillis;
                if ((last + timeoutMillis) < System.currentTimeMillis()) {
                    final Channel channel = next.getValue().channel;
                    if (channel != null) {
                        RemotingUtil.closeChannel(channel);
                    }
                    iterator.remove();
                    this.executor.submit(() ->
                        notifyBrokerInActive(next.getValue().brokerName, next.getKey().brokerAddr, next.getValue().brokerId));
                    log.warn("The broker channel expired, {} {}ms", next.getKey(), timeoutMillis);
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveBroker exception", e);
        }
    }

    private void notifyBrokerInActive(String brokerName, String brokerAddr, Long brokerId) {
        for (BrokerLifecycleListener listener : this.brokerLifecycleListeners) {
            listener.onBrokerInactive(brokerName, brokerAddr, brokerId);
        }
    }

    @Override
    public void addBrokerLifecycleListener(BrokerLifecycleListener listener) {
        this.brokerLifecycleListeners.add(listener);
    }

    @Override
    public void registerBroker(String clusterName, String brokerName, String brokerAddr,
        long brokerId, Long timeoutMillis, Channel channel) {
        final BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        final BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(addrInfo,
            new BrokerLiveInfo(brokerName,
                brokerId,
                System.currentTimeMillis(),
                timeoutMillis == null ? DEFAULT_BROKER_CHANNEL_EXPIRED_TIME : timeoutMillis,
                channel));
        if (prevBrokerLiveInfo == null) {
            log.info("new broker registered, {}, brokerId:{}", addrInfo, brokerId);
        }
    }

    @Override
    public void onBrokerHeartbeat(String clusterName, String brokerAddr) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            prev.lastUpdateTimestamp = System.currentTimeMillis();
        }
    }

    @Override
    public void onBrokerChannelClose(Channel channel) {
        synchronized (this) {
            for (Map.Entry<BrokerAddrInfo, BrokerLiveInfo> entry : this.brokerLiveTable.entrySet()) {
                if (entry.getValue().channel == channel) {
                    this.executor.submit(() ->
                        notifyBrokerInActive(entry.getValue().brokerName, entry.getKey().brokerAddr, entry.getValue().brokerId));
                    break;
                }
            }
        }
    }

    @Override
    public boolean isBrokerActive(String clusterName, String brokerAddr) {
        final BrokerLiveInfo info = this.brokerLiveTable.get(new BrokerAddrInfo(clusterName, brokerAddr));
        if (info != null) {
            long last = info.lastUpdateTimestamp;
            long timeoutMillis = info.heartbeatTimeoutMillis;
            return (last + timeoutMillis) >= System.currentTimeMillis();
        }
        return false;
    }

    static class BrokerLiveInfo {
        private final String brokerName;
        private final long brokerId;
        private final long heartbeatTimeoutMillis;
        private final Channel channel;
        private long lastUpdateTimestamp;

        public BrokerLiveInfo(String brokerName, long brokerId, long lastUpdateTimestamp, long heartbeatTimeoutMillis,
            Channel channel) {
            this.brokerName = brokerName;
            this.brokerId = brokerId;
            this.lastUpdateTimestamp = lastUpdateTimestamp;
            this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
            this.channel = channel;
        }

        @Override public String toString() {
            return "BrokerLiveInfo{" +
                "brokerName='" + brokerName + '\'' +
                ", brokerId=" + brokerId +
                ", lastUpdateTimestamp=" + lastUpdateTimestamp +
                ", heartbeatTimeoutMillis=" + heartbeatTimeoutMillis +
                ", channel=" + channel +
                '}';
        }
    }

    /**
     * broker address information
     */
    static class BrokerAddrInfo {
        private final String clusterName;
        private final String brokerAddr;

        private int hash;

        public BrokerAddrInfo(String clusterName, String brokerAddr) {
            this.clusterName = clusterName;
            this.brokerAddr = brokerAddr;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }

            if (obj instanceof BrokerAddrInfo) {
                BrokerAddrInfo addr = (BrokerAddrInfo) obj;
                return clusterName.equals(addr.clusterName) && brokerAddr.equals(addr.brokerAddr);
            }
            return false;
        }

        @Override
        public int hashCode() {
            int h = hash;
            if (h == 0 && clusterName.length() + brokerAddr.length() > 0) {
                for (int i = 0; i < clusterName.length(); i++) {
                    h = 31 * h + clusterName.charAt(i);
                }
                h = 31 * h + '_';
                for (int i = 0; i < brokerAddr.length(); i++) {
                    h = 31 * h + brokerAddr.charAt(i);
                }
                hash = h;
            }
            return h;
        }

        @Override
        public String toString() {
            return "BrokerAddrInfo [clusterName=" + clusterName + ", brokerAddr=" + brokerAddr + "]";
        }
    }
}
