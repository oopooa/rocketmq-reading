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
package org.apache.rocketmq.common.attribute;

public class LongRangeAttribute extends AbstractRangeAttribute<Long> {

    public LongRangeAttribute(String name, boolean changeable, long min, long max, long defaultValue) {
        super(name, changeable, min, max, defaultValue);
    }

    @Override
    protected Long parse(String value) {
        return Long.parseLong(value);
    }
}
