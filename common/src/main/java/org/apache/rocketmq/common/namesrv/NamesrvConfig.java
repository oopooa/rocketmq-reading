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

/**
 * $Id: NamesrvConfig.java 1839 2013-05-16 02:12:02Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.namesrv;

import java.io.File;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class NamesrvConfig {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";
    private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";
    private String productEnvName = "center";
    private boolean clusterTest = false;
    private boolean orderMessageEnable = false;
    private boolean returnOrderTopicConfigToBroker = true;
    private volatile boolean remoteFaultTolerance = true;

    /**
     * Indicates the nums of thread to handle client requests, like GET_ROUTEINTO_BY_TOPIC.
     */
    private int clientRequestThreadPoolNums = 8;
    /**
     * Indicates the nums of thread to handle broker or operation requests, like REGISTER_BROKER.
     */
    private int defaultThreadPoolNums = 16;
    /**
     * Indicates the capacity of queue to hold client requests.
     */
    private int clientRequestThreadPoolQueueCapacity = 50000;
    /**
     * Indicates the capacity of queue to hold broker or operation requests.
     */
    private int defaultThreadPoolQueueCapacity = 10000;
    /**
     * Interval of periodic scanning for non-active broker;
     */
    private long scanNotActiveBrokerInterval = 5 * 1000;

    private int unRegisterBrokerQueueCapacity = 3000;

    /**
     * Support acting master or not.
     *
     * The slave can be an acting master when master node is down to support following operations:
     * 1. support lock/unlock message queue operation.
     * 2. support searchOffset, query maxOffset/minOffset operation.
     * 3. support query earliest msg store time.
     */
    private boolean supportActingMaster = false;

    private volatile boolean enableAllTopicList = false;

    /**
     * Dledger controller config
     */
    private boolean isStartupController = false;
    /**
     * Indicates the nums of thread to handle broker or operation requests, like REGISTER_BROKER.
     */
    private int controllerThreadPoolNums = 16;
    /**
     * Indicates the capacity of queue to hold client requests.
     */
    private int controllerRequestThreadPoolQueueCapacity = 50000;
    private String controllerDLegerGroup;
    private String controllerDLegerPeers;
    private String controllerDLegerSelfId;
    private String controllerStorePath = System.getProperty("user.home") + File.separator + "DledgerController";
    /**
     * Whether the controller can elect a master which is not in the syncStateSet.
     */
    private boolean enableElectUncleanMaster = false;

    public void setRemoteFaultTolerance(boolean remoteFaultTolerance) {
        this.remoteFaultTolerance = remoteFaultTolerance;
    }

    public boolean isRemoteFaultTolerance() {
        return remoteFaultTolerance;
    }

    public boolean isOrderMessageEnable() {
        return orderMessageEnable;
    }

    public void setOrderMessageEnable(boolean orderMessageEnable) {
        this.orderMessageEnable = orderMessageEnable;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getKvConfigPath() {
        return kvConfigPath;
    }

    public void setKvConfigPath(String kvConfigPath) {
        this.kvConfigPath = kvConfigPath;
    }

    public String getProductEnvName() {
        return productEnvName;
    }

    public void setProductEnvName(String productEnvName) {
        this.productEnvName = productEnvName;
    }

    public boolean isClusterTest() {
        return clusterTest;
    }

    public void setClusterTest(boolean clusterTest) {
        this.clusterTest = clusterTest;
    }

    public String getConfigStorePath() {
        return configStorePath;
    }

    public void setConfigStorePath(final String configStorePath) {
        this.configStorePath = configStorePath;
    }

    public boolean isReturnOrderTopicConfigToBroker() {
        return returnOrderTopicConfigToBroker;
    }

    public void setReturnOrderTopicConfigToBroker(boolean returnOrderTopicConfigToBroker) {
        this.returnOrderTopicConfigToBroker = returnOrderTopicConfigToBroker;
    }

    public int getClientRequestThreadPoolNums() {
        return clientRequestThreadPoolNums;
    }

    public void setClientRequestThreadPoolNums(final int clientRequestThreadPoolNums) {
        this.clientRequestThreadPoolNums = clientRequestThreadPoolNums;
    }

    public int getDefaultThreadPoolNums() {
        return defaultThreadPoolNums;
    }

    public void setDefaultThreadPoolNums(final int defaultThreadPoolNums) {
        this.defaultThreadPoolNums = defaultThreadPoolNums;
    }

    public int getClientRequestThreadPoolQueueCapacity() {
        return clientRequestThreadPoolQueueCapacity;
    }

    public void setClientRequestThreadPoolQueueCapacity(final int clientRequestThreadPoolQueueCapacity) {
        this.clientRequestThreadPoolQueueCapacity = clientRequestThreadPoolQueueCapacity;
    }

    public int getDefaultThreadPoolQueueCapacity() {
        return defaultThreadPoolQueueCapacity;
    }

    public void setDefaultThreadPoolQueueCapacity(final int defaultThreadPoolQueueCapacity) {
        this.defaultThreadPoolQueueCapacity = defaultThreadPoolQueueCapacity;
    }

    public long getScanNotActiveBrokerInterval() {
        return scanNotActiveBrokerInterval;
    }

    public void setScanNotActiveBrokerInterval(long scanNotActiveBrokerInterval) {
        this.scanNotActiveBrokerInterval = scanNotActiveBrokerInterval;
    }

    public int getUnRegisterBrokerQueueCapacity() {
        return unRegisterBrokerQueueCapacity;
    }

    public void setUnRegisterBrokerQueueCapacity(final int unRegisterBrokerQueueCapacity) {
        this.unRegisterBrokerQueueCapacity = unRegisterBrokerQueueCapacity;
    }

    public boolean isSupportActingMaster() {
        return supportActingMaster;
    }

    public void setSupportActingMaster(final boolean supportActingMaster) {
        this.supportActingMaster = supportActingMaster;
    }

    public boolean isEnableAllTopicList() {
        return enableAllTopicList;
    }

    public void setEnableAllTopicList(boolean enableAllTopicList) {
        this.enableAllTopicList = enableAllTopicList;
    }

    public boolean isStartupController() {
        return isStartupController;
    }

    public void setStartupController(boolean startupController) {
        isStartupController = startupController;
    }

    public int getControllerThreadPoolNums() {
        return controllerThreadPoolNums;
    }

    public void setControllerThreadPoolNums(int controllerThreadPoolNums) {
        this.controllerThreadPoolNums = controllerThreadPoolNums;
    }

    public int getControllerRequestThreadPoolQueueCapacity() {
        return controllerRequestThreadPoolQueueCapacity;
    }

    public void setControllerRequestThreadPoolQueueCapacity(int controllerRequestThreadPoolQueueCapacity) {
        this.controllerRequestThreadPoolQueueCapacity = controllerRequestThreadPoolQueueCapacity;
    }

    public String getControllerDLegerGroup() {
        return controllerDLegerGroup;
    }

    public void setControllerDLegerGroup(String controllerDLegerGroup) {
        this.controllerDLegerGroup = controllerDLegerGroup;
    }

    public String getControllerDLegerPeers() {
        return controllerDLegerPeers;
    }

    public void setControllerDLegerPeers(String controllerDLegerPeers) {
        this.controllerDLegerPeers = controllerDLegerPeers;
    }

    public String getControllerDLegerSelfId() {
        return controllerDLegerSelfId;
    }

    public void setControllerDLegerSelfId(String controllerDLegerSelfId) {
        this.controllerDLegerSelfId = controllerDLegerSelfId;
    }

    public String getControllerStorePath() {
        return controllerStorePath;
    }

    public void setControllerStorePath(String controllerStorePath) {
        this.controllerStorePath = controllerStorePath;
    }

    public boolean isEnableElectUncleanMaster() {
        return enableElectUncleanMaster;
    }

    public void setEnableElectUncleanMaster(boolean enableElectUncleanMaster) {
        this.enableElectUncleanMaster = enableElectUncleanMaster;
    }
}
