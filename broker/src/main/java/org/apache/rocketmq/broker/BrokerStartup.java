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

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerStartup {

    public static Logger log;
    public static final SystemConfigFileHelper CONFIG_FILE_HELPER = new SystemConfigFileHelper();

    // 🚩 Broker 服务入口
    public static void main(String[] args) {
        // 创建并启动一个 BrokerController 实例
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {
            controller.start();

            String tip = String.format("The broker[%s, %s] boot success. serializeType=%s",
                controller.getBrokerConfig().getBrokerName(), controller.getBrokerAddr(),
                RemotingCommand.getSerializeTypeConfigInThisServer());

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    public static BrokerController buildBrokerController(String[] args) throws Exception {
        // 设置 RocketMQ Remoting 版本信息, 属性名为 rocketmq.remoting.version
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        // 创建 Broker 配置类
        final BrokerConfig brokerConfig = new BrokerConfig();
        // 创建 Netty服务端 配置类
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        // 创建 Netty客户端 配置类
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();
        // 创建 Broker 消息存储配置类
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        // 设置 Netty 服务监听端口为 10911
        nettyServerConfig.setListenPort(10911);
        // 重置存储配置高可用端口为 0, 后续通过配置文件修改或者默认 listenPort + 1
        messageStoreConfig.setHaListenPort(0);

        // 构建基础命令行参数解析配置
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        // 解析命令行参数 并新增 -c -p -m 参数解析
        CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqbroker", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            // 异常退出
            System.exit(-1);
        }

        Properties properties = null;
        // 检查命令行参数是否包含 -c 或 --configFile
        if (commandLine.hasOption('c')) {
            // 获取配置文件路径
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                // 设置配置文件路径
                CONFIG_FILE_HELPER.setFile(file);
                BrokerPathConfigHelper.setBrokerConfigPath(file);
                // 加载配置文件到 properties
                properties = CONFIG_FILE_HELPER.loadConfig();
            }
        }

        if (properties != null) {
            // 把 rmqAddressServerDomain 和 rmqAddressServerSubGroup 设置为系统属性
            properties2SystemEnv(properties);
            // 把属性转换到 Broker 配置中
            MixAll.properties2Object(properties, brokerConfig);
            // 把属性转换到 NettyServer 配置中
            MixAll.properties2Object(properties, nettyServerConfig);
            // 把属性转换到 NettyClient 配置中
            MixAll.properties2Object(properties, nettyClientConfig);
            // 把属性转换到 Broker 消息存储配置中
            MixAll.properties2Object(properties, messageStoreConfig);
        }

        // 解析命令行参数, 并转换到 Broker 配置中
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
        // 如果 ROCKETMQ_HOME 配置不存在
        if (null == brokerConfig.getRocketmqHome()) {
            // 输出环境变量异常
            System.out.printf("Please set the %s variable in your environment " +
                "to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
            // 异常退出
            System.exit(-2);
        }

        // 校验 NameServer 地址
        String namesrvAddr = brokerConfig.getNamesrvAddr();
        if (StringUtils.isNotBlank(namesrvAddr)) {
            try {
                // 以 ; 为分隔符对 NameServer 地址进行拆分
                String[] addrArray = namesrvAddr.split(";");
                for (String addr : addrArray) {
                    NetworkUtil.string2SocketAddress(addr);
                }
            } catch (Exception e) {
                // 输出异常 NameServer 地址
                System.out.printf("The Name Server Address[%s] illegal, please set it as follows, " +
                        "\"127.0.0.1:9876;192.168.0.1:9876\"%n", namesrvAddr);
                // 异常退出
                System.exit(-3);
            }
        }

        // 如果当前 Broker 角色为 Slave
        if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
            // 默认 ratio = 40 - 10 = 30
            int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
            // 设置消息在内存中的最大比例
            messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
        }

        // 根据高可用配置设置 Broker 角色
        // 如果 Broker 没有开启 Controller 模式
        if (!brokerConfig.isEnableControllerMode()) {
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    // 设置 Master 的 BrokerId 为 0
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    // 如果 Slave 的 BrokerId 小于等于 0
                    if (brokerConfig.getBrokerId() <= MixAll.MASTER_ID) {
                        // 输出异常信息
                        System.out.printf("Slave's brokerId must be > 0%n");
                        // 异常退出
                        System.exit(-3);
                    }
                    break;
                default:
                    break;
            }
        }

        // 如果消息存储配置启用了 DLedger 日志
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            // 设置 BrokerId 为 -1, 因为 DLedger 模式下, BrokerId 会自动分配
            brokerConfig.setBrokerId(-1);
        }

        // 如果 Broker 启用了 Controller 模式并且消息存储配置启用了 DLedger CommitLog
        if (brokerConfig.isEnableControllerMode() && messageStoreConfig.isEnableDLegerCommitLog()) {
            // 输出冲突信息
            System.out.printf("The config enableControllerMode and enableDLegerCommitLog cannot both be true.%n");
            // 异常退出
            System.exit(-4);
        }

        // 如果高可用端口仍然为 0, 说明没有配置过, 在此处进行默认配置
        if (messageStoreConfig.getHaListenPort() <= 0) {
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
        }

        // 通过 BrokerStartup 启动时, 该值为 false
        brokerConfig.setInBrokerContainer(false);
        // 设置 Broker 日志路径
        System.setProperty("brokerLogDir", "");
        // 是否日志隔离: 在同一个机器上有多个 Broker 部署时, 是否区分日志路径
        if (brokerConfig.isIsolateLogEnable()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + brokerConfig.getBrokerId());
        }
        if (brokerConfig.isIsolateLogEnable() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + messageStoreConfig.getdLegerSelfId());
        }

        // 检查命令行参数是否包含 -p 或 --printConfigItem
        if (commandLine.hasOption('p')) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            // 在控制台输出 Broker 配置详情
            MixAll.printObjectProperties(console, brokerConfig);
            // 在控制台输出 NettyServer 配置详情
            MixAll.printObjectProperties(console, nettyServerConfig);
            // 在控制台输出 NettyClient 配置详情
            MixAll.printObjectProperties(console, nettyClientConfig);
            // 在控制台输出 Broker 消息存储配置详情
            MixAll.printObjectProperties(console, messageStoreConfig);
            // 正常退出
            System.exit(0);
            // 检查命令行参数是否包含 -m 或 --printImportantConfig
        } else if (commandLine.hasOption('m')) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            // 在控制台输出 Broker 重要配置详情
            MixAll.printObjectProperties(console, brokerConfig, true);
            // 在控制台输出 NettyServer 重要配置详情
            MixAll.printObjectProperties(console, nettyServerConfig, true);
            // 在控制台输出 NettyClient 重要配置详情
            MixAll.printObjectProperties(console, nettyClientConfig, true);
            // 在控制台输出 Broker 消息存储重要配置详情
            MixAll.printObjectProperties(console, messageStoreConfig, true);
            // 正常退出
            System.exit(0);
        }

        log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
        // 在日志文件中输出配置详情
        MixAll.printObjectProperties(log, brokerConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        MixAll.printObjectProperties(log, nettyClientConfig);
        MixAll.printObjectProperties(log, messageStoreConfig);

        // 创建 BrokerController 实例, 设置各种属性
        final BrokerController controller = new BrokerController(
            brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);

        // 从配置文件中加载的属性注册到 allConfigs 中
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    public static Runnable buildShutdownHook(BrokerController brokerController) {
        return new Runnable() {
            private volatile boolean hasShutdown = false;
            private final AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        brokerController.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        };
    }

    public static BrokerController createBrokerController(String[] args) {
        try {
            // 根据命令行参数和配置文件构建一个 BrokerController 实例
            BrokerController controller = buildBrokerController(args);
            // 初始化 BrokerController
            boolean initResult = controller.initialize();
            // 初始化是否完成
            if (!initResult) {
                // 停止 BrokerController
                controller.shutdown();
                // 异常退出
                System.exit(-3);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(controller)));
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        // 配置 -c 命令行参数解析 指定 Broker 配置文件
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        // 配置 -p 命令行参数解析 输出所有配置项
        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        /**
         * 配置 -m 命令行参数解析 输出重要配置项 (标记了{@link ImportantField}的字段)
         */
        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static class SystemConfigFileHelper {
        private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigFileHelper.class);

        private String file;

        public SystemConfigFileHelper() {
        }

        public Properties loadConfig() throws Exception {
            // 读取文件为缓存输入流
            InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
            Properties properties = new Properties();
            // 从流中加载配置属性
            properties.load(in);
            in.close();
            return properties;
        }

        public void update(Properties properties) throws Exception {
            LOGGER.error("[SystemConfigFileHelper] update no thing.");
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getFile() {
            return file;
        }
    }
}
