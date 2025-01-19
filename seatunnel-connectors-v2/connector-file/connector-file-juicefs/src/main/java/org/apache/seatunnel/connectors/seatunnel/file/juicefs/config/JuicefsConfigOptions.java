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

package org.apache.seatunnel.connectors.seatunnel.file.juicefs.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;

import java.util.HashMap;
import java.util.Map;

public class JuicefsConfigOptions extends BaseSourceConfigOptions {

    public static final Option<String> JFS_NAME =
            Options.key("jfs_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Juicefs volume name. eg: jfs://seatunnel");

    public static final Option<String> META_URL =
            Options.key("meta_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Juicefs meta url");
    /**
     * The current key for that config option. if you need to add a new option, you can add it here
     * and refer to this:
     *
     * <p>https://juicefs.com/docs/community/hadoop_java_sdk/
     *
     * <p>such as: key = "juicefs.cache-size" value = "0"
     */
    public static final Option<Map<String, String>> HADOOP_PROPERTIES =
            Options.key("hadoop_properties")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("Juicefs hadoop properties");
}
