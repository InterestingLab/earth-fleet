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
package org.apache.seatunnel.connectors.seatunnel.prometheus.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Setter
@Getter
@ToString
public class PrometheusSinkConfig extends HttpConfig {

    private static final int DEFAULT_BATCH_SIZE = 1024;

    public static final Option<String> KEY_TIMESTAMP =
            Options.key("key_timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("key timestamp");

    public static final Option<String> KEY_LABEL =
            Options.key("key_label").stringType().noDefaultValue().withDescription("key label");

    public static final Option<String> KEY_VALUE =
            Options.key("key_value").stringType().noDefaultValue().withDescription("key value");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(DEFAULT_BATCH_SIZE)
                    .withDescription("the batch size writer to prometheus");

    private String keyTimestamp;

    private String keyValue;

    private String keyLabel;

    private int batchSize = BATCH_SIZE.defaultValue();

    public static PrometheusSinkConfig loadConfig(Config pluginConfig) {
        PrometheusSinkConfig sinkConfig = new PrometheusSinkConfig();
        if (pluginConfig.hasPath(KEY_VALUE.key())) {
            sinkConfig.setKeyValue(pluginConfig.getString(KEY_VALUE.key()));
        }
        if (pluginConfig.hasPath(KEY_LABEL.key())) {
            sinkConfig.setKeyLabel(pluginConfig.getString(KEY_LABEL.key()));
        }
        if (pluginConfig.hasPath(BATCH_SIZE.key())) {
            int batchSize = checkIntArgument(pluginConfig.getInt(BATCH_SIZE.key()));
            sinkConfig.setBatchSize(batchSize);
        }
        return sinkConfig;
    }

    private static int checkIntArgument(int args) {
        checkArgument(args > 0);
        return args;
    }
}
