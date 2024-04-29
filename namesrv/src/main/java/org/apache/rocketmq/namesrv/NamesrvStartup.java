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
package org.apache.rocketmq.namesrv;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.JraftConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;

public class NamesrvStartup {

    private final static Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final static Logger logConsole = LoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);
    private static Properties properties = null;
    private static NamesrvConfig namesrvConfig = null;
    private static NettyServerConfig nettyServerConfig = null;
    private static NettyClientConfig nettyClientConfig = null;
    private static ControllerConfig controllerConfig = null;

    public static void main(String[] args) {
        main0(args);
        controllerManagerMain();
    }

    public static NamesrvController main0(String[] args) {
        try {
            // 解析命令行和配置文件
            parseCommandlineAndConfigFile(args);
            NamesrvController controller = createAndStartNamesrvController();
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static ControllerManager controllerManagerMain() {
        try {
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                return createAndStartControllerManager();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    public static void parseCommandlineAndConfigFile(String[] args) throws Exception {
        // 设置 RocketMQ Remoting 版本信息, 属性名为 rocketmq.remoting.version
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        // 构建基础命令行参数解析配置
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        // 解析命令行参数 并新增 -c -p 参数解析
        CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            // 异常退出
            System.exit(-1);
            return;
        }

        // 创建 Namesrv 配置类
        namesrvConfig = new NamesrvConfig();
        // 创建 Netty服务端 配置类
        nettyServerConfig = new NettyServerConfig();
        // 创建 Netty客户端 配置类
        nettyClientConfig = new NettyClientConfig();
        // 设置 Netty 服务监听端口为 9876
        nettyServerConfig.setListenPort(9876);
        // 检查命令行参数是否包含 -c 或 --configFile
        if (commandLine.hasOption('c')) {
            // 获取配置文件路径
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                // 读取文件为缓存输入流
                InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
                properties = new Properties();
                // 从流中加载配置属性
                properties.load(in);
                // 把属性转换到 Namesrv 配置中
                MixAll.properties2Object(properties, namesrvConfig);
                // 把属性转换到 NettyServer 配置中
                MixAll.properties2Object(properties, nettyServerConfig);
                // 把属性转换到 NettyClient 配置中
                MixAll.properties2Object(properties, nettyClientConfig);
                // 是否在当前 Namesrv 开启 controller
                if (namesrvConfig.isEnableControllerInNamesrv()) {
                    // 创建 Controller 配置类
                    controllerConfig = new ControllerConfig();
                    // 创建 Jraft 配置类
                    JraftConfig jraftConfig = new JraftConfig();
                    controllerConfig.setJraftConfig(jraftConfig);
                    // 把属性转换到 Controller 配置中
                    MixAll.properties2Object(properties, controllerConfig);
                    // 把属性转换到 Jraft 配置中
                    MixAll.properties2Object(properties, jraftConfig);
                }
                // 设置配置文件路径
                namesrvConfig.setConfigStorePath(file);

                // 输出配置加载信息
                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        // 解析命令行参数, 并转换到 Namesrv 配置中
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        // 检查命令行参数是否包含 -p 或 --printConfigItem
        if (commandLine.hasOption('p')) {
            // 输出 Namesrv 配置详情
            MixAll.printObjectProperties(logConsole, namesrvConfig);
            // 输出 NettyServer 配置详情
            MixAll.printObjectProperties(logConsole, nettyServerConfig);
            // 输出 NettyClient 配置详情
            MixAll.printObjectProperties(logConsole, nettyClientConfig);
            // 是否在当前 Namesrv 开启 controller
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                // 输出 Controller 配置详情
                MixAll.printObjectProperties(logConsole, controllerConfig);
            }
            // 正常退出
            System.exit(0);
        }

        // 如果 ROCKETMQ_HOME 配置不存在
        if (null == namesrvConfig.getRocketmqHome()) {
            // 输出环境变量异常
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            // 异常退出
            System.exit(-2);
        }
        // 输出 Namesrv 配置详情到日志文件
        MixAll.printObjectProperties(log, namesrvConfig);
        // 输出 NettyServer 配置详情到日志文件
        MixAll.printObjectProperties(log, nettyServerConfig);

    }

    public static NamesrvController createAndStartNamesrvController() throws Exception {

        // 创建 NamesrvController 实例
        NamesrvController controller = createNamesrvController();
        // 启动 NamesrvController
        start(controller);
        NettyServerConfig serverConfig = controller.getNettyServerConfig();
        String tip = String.format("The Name Server boot success. serializeType=%s, address %s:%d", RemotingCommand.getSerializeTypeConfigInThisServer(), serverConfig.getBindAddress(), serverConfig.getListenPort());
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controller;
    }

    public static NamesrvController createNamesrvController() {

        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        controller.start();

        return controller;
    }

    public static ControllerManager createAndStartControllerManager() throws Exception {
        ControllerManager controllerManager = createControllerManager();
        start(controllerManager);
        String tip = "The ControllerManager boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controllerManager;
    }

    public static ControllerManager createControllerManager() throws Exception {
        NettyServerConfig controllerNettyServerConfig = (NettyServerConfig) nettyServerConfig.clone();
        ControllerManager controllerManager = new ControllerManager(controllerConfig, controllerNettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        controllerManager.getConfiguration().registerConfig(properties);
        return controllerManager;
    }

    public static ControllerManager start(final ControllerManager controllerManager) throws Exception {

        if (null == controllerManager) {
            throw new IllegalArgumentException("ControllerManager is null");
        }

        boolean initResult = controllerManager.initialize();
        if (!initResult) {
            controllerManager.shutdown();
            System.exit(-3);
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controllerManager.shutdown();
            return null;
        }));

        controllerManager.start();

        return controllerManager;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static void shutdown(final ControllerManager controllerManager) {
        controllerManager.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        // 配置 -c 命令行参数解析 指定 namesrv 配置文件
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        // 配置 -p 命令行参数解析 输出所有配置项
        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
