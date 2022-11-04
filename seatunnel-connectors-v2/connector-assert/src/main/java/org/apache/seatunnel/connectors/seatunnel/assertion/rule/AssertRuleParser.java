/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.assertion.rule;

import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.Config.FIELD_NAME;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.Config.FIELD_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.Config.FIELD_VALUE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.Config.RULE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.Config.RULE_VALUE;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AssertRuleParser {

    public List<AssertFieldRule.AssertRule> parseRowRules(List<? extends Config> rowRuleList){

        return assembleFieldValueRules(rowRuleList);
    }

    public List<AssertFieldRule> parseRules(List<? extends Config> ruleConfigList) {
        return ruleConfigList.stream()
            .map(config -> {
                AssertFieldRule fieldRule = new AssertFieldRule();
                fieldRule.setFieldName(config.getString(FIELD_NAME.key()));
                if (config.hasPath(FIELD_TYPE.key())) {
                    fieldRule.setFieldType(getFieldType(config.getString(FIELD_TYPE.key())));
                }

                if (config.hasPath(FIELD_VALUE.key())) {
                    List<AssertFieldRule.AssertRule> fieldValueRules = assembleFieldValueRules(config.getConfigList(FIELD_VALUE.key()));
                    fieldRule.setFieldRules(fieldValueRules);
                }
                return fieldRule;
            })
            .collect(Collectors.toList());
    }

    private List<AssertFieldRule.AssertRule> assembleFieldValueRules(List<? extends Config> fieldValueConfigList) {
        return fieldValueConfigList.stream()
            .map(config -> {
                AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
                if (config.hasPath(RULE_TYPE.key())) {
                    valueRule.setRuleType(AssertFieldRule.AssertRuleType.valueOf(config.getString(RULE_TYPE.key())));
                }
                if (config.hasPath(RULE_VALUE.key())) {
                    valueRule.setRuleValue(config.getDouble(RULE_VALUE.key()));
                }
                return valueRule;
            })
            .collect(Collectors.toList());
    }

    private SeaTunnelDataType<?> getFieldType(String fieldTypeStr) {
        return TYPES.get(fieldTypeStr.toLowerCase());
    }

    private static final Map<String, SeaTunnelDataType<?>> TYPES = Maps.newHashMap();

    static {
        TYPES.put("string", BasicType.STRING_TYPE);
        TYPES.put("boolean", BasicType.BOOLEAN_TYPE);
        TYPES.put("byte", BasicType.BYTE_TYPE);
        TYPES.put("short", BasicType.SHORT_TYPE);
        TYPES.put("int", BasicType.INT_TYPE);
        TYPES.put("long", BasicType.LONG_TYPE);
        TYPES.put("float", BasicType.FLOAT_TYPE);
        TYPES.put("double", BasicType.DOUBLE_TYPE);
        TYPES.put("void", BasicType.VOID_TYPE);
    }
}
