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
package org.apache.rocketmq.common.protocol.header.namesrv.controller;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class AlterSyncStateSetRequestHeader implements CommandCustomHeader {
    private String brokerName;
    private String masterIdentity;
    private int masterEpoch;

    public AlterSyncStateSetRequestHeader() {
    }

    public AlterSyncStateSetRequestHeader(String brokerName, String masterIdentity, int masterEpoch) {
        this.brokerName = brokerName;
        this.masterIdentity = masterIdentity;
        this.masterEpoch = masterEpoch;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getMasterIdentity() {
        return masterIdentity;
    }

    public void setMasterIdentity(String masterIdentity) {
        this.masterIdentity = masterIdentity;
    }

    public int getMasterEpoch() {
        return masterEpoch;
    }

    public void setMasterEpoch(int masterEpoch) {
        this.masterEpoch = masterEpoch;
    }

    @Override public String toString() {
        return "AlterSyncStateSetRequestHeader{" +
            "brokerName='" + brokerName + '\'' +
            ", masterAddress='" + masterIdentity + '\'' +
            ", masterEpoch=" + masterEpoch +
            '}';
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
