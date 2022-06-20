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
package org.apache.rocketmq.remoting.netty;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.rocketmq.remoting.RPCHookContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultRPCHookContext implements RPCHookContext {

    private Decision decision;

    //Make it compatible in 1.6
    private Future<RemotingCommand> responseFuture;

    public DefaultRPCHookContext() {
        this.decision = Decision.CONTINUE;
    }


    public void setResponseFuture(Future<RemotingCommand> responseFuture) {
        this.responseFuture = responseFuture;
    }

    @Override
    public Decision getDecision() {
        return decision;
    }

    @Override public void setDecision(Decision decision) {
        this.decision = decision;
    }

    @Override
    public Future<RemotingCommand> getResponseFuture() {
        return responseFuture;
    }

    @Override public void clear() {
        this.decision = Decision.CONTINUE;
        this.responseFuture = null;
    }
}
