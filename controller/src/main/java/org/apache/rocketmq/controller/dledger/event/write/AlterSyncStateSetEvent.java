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

package org.apache.rocketmq.controller.dledger.event.write;

import java.util.Set;

public class AlterSyncStateSetEvent implements WriteEventMessage {

    private final String clusterName;

    private final String brokerName;

    private final Long masterBrokerId;

    private final Integer masterEpoch;

    private final Integer newSyncStateSetEpoch;

    private final Set<Long/*BrokerId*/> newSyncStateSet;

    private final Set<Long/*BrokerId*/> aliveBrokerSet;

    public AlterSyncStateSetEvent(String clusterName, String brokerName,
        Long masterBrokerId, Integer masterEpoch, Set<Long> newSyncStateSet, Integer newSyncStateSetEpoch, Set<Long> aliveBrokerSet) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.masterBrokerId = masterBrokerId;
        this.masterEpoch = masterEpoch;
        this.newSyncStateSet = newSyncStateSet;
        this.newSyncStateSetEpoch = newSyncStateSetEpoch;
        this.aliveBrokerSet = aliveBrokerSet;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Long getMasterBrokerId() {
        return masterBrokerId;
    }

    public Integer getMasterEpoch() {
        return masterEpoch;
    }

    public Integer getNewSyncStateSetEpoch() {
        return newSyncStateSetEpoch;
    }

    public Set<Long> getNewSyncStateSet() {
        return newSyncStateSet;
    }

    public Set<Long> getAliveBrokerSet() {
        return aliveBrokerSet;
    }

    @Override
    public WriteEventType getEventType() {
        return WriteEventType.ALTER_SYNC_STATE_SET;
    }

    @Override
    public String toString() {
        return "AlterSyncStateSetEvent{" +
            "brokerName='" + brokerName + '\'' +
            ", masterBrokerId=" + masterBrokerId +
            ", masterEpoch=" + masterEpoch +
            '}';
    }
}
