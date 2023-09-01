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

public class ApplyBrokerIdEvent implements WriteEventMessage {

    private final String clusterName;
    private final String brokerName;
    private final Long appliedBrokerId;
    private final String registerCheckCode;

    public ApplyBrokerIdEvent(String clusterName, String brokerName, Long appliedBrokerId, String registerCheckCode) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.appliedBrokerId = appliedBrokerId;
        this.registerCheckCode = registerCheckCode;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Long getAppliedBrokerId() {
        return appliedBrokerId;
    }

    public String getRegisterCheckCode() {
        return registerCheckCode;
    }

    @Override
    public WriteEventType getEventType() {
        return WriteEventType.APPLY_BROKER_ID;
    }
}
