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

package org.apache.seatunnel.core.starter.enums;

import com.beust.jcommander.IStringConverter;

/**
 * Driver submitted mode, only works with Spark engine and Flink engine
 */
public enum DeployModeType {
    /**
     * Spark
     */
    CLIENT("client"),
    CLUSTER("cluster"),

    /**
     * Flink
     */
    RUN("run"),
    RUN_APPLICATION("run-application");

    private final String deployMode;

    DeployModeType(String deployMode) {
        this.deployMode = deployMode;
    }

    public static class DeployModeConverter implements IStringConverter<DeployModeType> {
        @Override
        public DeployModeType convert(String value) {
            return DeployModeType.valueOf(value.toUpperCase());
        }
    }

    public String getDeployMode() {
        return deployMode;
    }
}
