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
package org.apache.rocketmq.tools.command.offset;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.NameServerMocker;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GetConsumerStatusCommandTest {

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    @Rule
    public TemporaryFolder temporaryFolder = TemporaryFolder.builder().build();

    @Before
    public void before() {
        System.setProperty("rocketmq.client.logRoot", temporaryFolder.getRoot().getAbsolutePath());
        brokerMocker = startOneBroker();
        nameServerMocker = NameServerMocker.startByDefaultConf(0, brokerMocker.listenPort());
    }

    @After
    public void after() {
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testExecute() throws SubCommandException {
        GetConsumerStatusCommand cmd = new GetConsumerStatusCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-g default-group", "-t unit-test", "-i clientid",
            String.format("-n localhost:%d", nameServerMocker.listenPort())};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }

    private ServerResponseMocker startOneBroker() {
        GetConsumerStatusBody getConsumerStatusBody = new GetConsumerStatusBody();
        // start broker
        return ServerResponseMocker.startServer(0, getConsumerStatusBody.encode());
    }
}
