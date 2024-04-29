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
package org.apache.rocketmq.srvutil;

import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ServerUtil {

    public static Options buildCommandlineOptions(final Options options) {
        // 配置 -h 命令行参数解析 输出帮助信息
        Option opt = new Option("h", "help", false, "Print help");
        // 该参数不是必须的
        opt.setRequired(false);
        options.addOption(opt);

        // 配置 -n 命令行参数解析 配置 namesrv 地址列表
        opt =
            new Option("n", "namesrvAddr", true,
                "Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'");
        // 该参数不是必须的
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static CommandLine parseCmdLine(final String appName, String[] args, Options options,
        CommandLineParser parser) {
        HelpFormatter hf = new HelpFormatter();
        // 设置帮助信息每行的宽度为 110 个字符 (默认为 74, 超出的部分会换行显示)
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            // 解析命令行参数, 如果参数不能被解析或者缺失, 则抛出异常
            commandLine = parser.parse(options, args);
            // 检查命令行参数是否包含 -h 或 --help
            if (commandLine.hasOption('h')) {
                // 输出命令行选项帮助信息, 并打印基于 options 的用法说明 比如 [-c <arg>] [-h] [-n <arg>] [-p]
                hf.printHelp(appName, options, true);
                // 正常退出
                System.exit(0);
            }
        } catch (ParseException e) {
            // 输出异常信息
            System.err.println(e.getMessage());
            // 输出正确的命令行选项帮助信息
            hf.printHelp(appName, options, true);
            // 异常退出
            System.exit(1);
        }

        return commandLine;
    }

    public static void printCommandLineHelp(final String appName, final Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(appName, options, true);
    }

    public static Properties commandLine2Properties(final CommandLine commandLine) {
        Properties properties = new Properties();
        // 获取命令行传入的参数数组
        Option[] opts = commandLine.getOptions();

        if (opts != null) {
            for (Option opt : opts) {
                // 获取完整属性名
                String name = opt.getLongOpt();
                // 获取属性值
                String value = commandLine.getOptionValue(name);
                if (value != null) {
                    properties.setProperty(name, value);
                }
            }
        }

        return properties;
    }

}
