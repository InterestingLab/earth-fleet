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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalTableFilters;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Describes the connection information of the Oracle database and the configuration information for
 * performing snapshotting and streaming reading, such as splitSize.
 */
@Getter
public class OracleSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    private final Boolean useSelectCount;
    private final Boolean skipAnalyze;

    public OracleSourceConfig(
            Boolean useSelectCount,
            Boolean skipAnalyze,
            StartupConfig startupConfig,
            StopConfig stopConfig,
            List<String> databaseList,
            List<String> tableList,
            int splitSize,
            Map<String, String> splitColumn,
            double distributionFactorUpper,
            double distributionFactorLower,
            int sampleShardingThreshold,
            int inverseSamplingRate,
            Properties dbzProperties,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            String originUrl,
            int fetchSize,
            String serverTimeZone,
            long connectTimeoutMillis,
            int connectMaxRetries,
            int connectionPoolSize,
            boolean exactlyOnce) {
        super(
                startupConfig,
                stopConfig,
                databaseList,
                tableList,
                splitSize,
                splitColumn,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold,
                inverseSamplingRate,
                dbzProperties,
                driverClassName,
                hostname,
                port,
                username,
                password,
                originUrl,
                fetchSize,
                serverTimeZone,
                connectTimeoutMillis,
                connectMaxRetries,
                connectionPoolSize,
                exactlyOnce);
        this.useSelectCount = useSelectCount;
        this.skipAnalyze = skipAnalyze;
    }

    @Override
    public OracleConnectorConfig getDbzConnectorConfig() {
        return new OracleConnectorConfig(getDbzConfiguration());
    }

    public Configuration getOriginDbzConnectorConfig() {
        return super.getDbzConfiguration();
    }

    public RelationalTableFilters getTableFilters() {
        return getDbzConnectorConfig().getTableFilters();
    }
}
