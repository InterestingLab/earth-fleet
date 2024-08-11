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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.timeplus.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.timeplus.exception.TimeplusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.timeplus.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.timeplus.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.TimeplusProxy;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.TimeplusSink;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.file.TimeplusTable;
import org.apache.seatunnel.connectors.seatunnel.timeplus.util.TimeplusUtil;

import com.google.auto.service.AutoService;
import com.timeplus.proton.client.ProtonNode;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.*;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.*;

import static org.icecream.IceCream.ic;

@AutoService(Factory.class)
public class TimeplusSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Timeplus";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(TABLE)
                .optional(
                        HOST,
                        DATABASE,
                        TIMEPLUS_CONFIG,
                        BULK_SIZE,
                        SPLIT_MODE,
                        SHARDING_KEY,
                        PRIMARY_KEY,
                        SUPPORT_UPSERT,
                        SCHEMA_SAVE_MODE,
                        SAVE_MODE_CREATE_TEMPLATE,
                        DATA_SAVE_MODE,
                        ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE)
                .bundled(USERNAME, PASSWORD)
                .build();
    }

    /*
        public void prepare(Config config) throws PrepareFailException {
            Map<String, Object> defaultConfig =
                    ImmutableMap.<String, Object>builder()
                            .put(HOST.key(), HOST.defaultValue())
                            .put(DATABASE.key(), DATABASE.defaultValue())
                            .put(USERNAME.key(), USERNAME.defaultValue())
                            .put(PASSWORD.key(), PASSWORD.defaultValue())
                            .put(TABLE.key(), TABLE.defaultValue())
                            .put(BULK_SIZE.key(), BULK_SIZE.defaultValue())
                            .put(SPLIT_MODE.key(), SPLIT_MODE.defaultValue())
                            .put(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue())
                            .build();

            config = config.withFallback(ConfigFactory.parseMap(defaultConfig));

            CheckResult result = CheckConfigUtil.checkAllExists(config, HOST.key());

            boolean isCredential = config.hasPath(USERNAME.key()) || config.hasPath(PASSWORD.key());

            if (isCredential) {
                result = CheckConfigUtil.checkAllExists(config, USERNAME.key(), PASSWORD.key());
            }

            if (!result.isSuccess()) {
                throw new TimeplusConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "PluginName: %s, PluginType: %s, Message: %s",
                                getPluginName(), PluginType.SINK, result.getMsg()));
            }

            List<ProtonNode> nodes;
            if (!isCredential) {
                nodes =
                        TimeplusUtil.createNodes(
                                config.getString(HOST.key()),
                                config.getString(DATABASE.key()),
                                config.getString(SERVER_TIME_ZONE.key()),
                                null,
                                null,
                                null);
            } else {
                nodes =
                        TimeplusUtil.createNodes(
                                config.getString(HOST.key()),
                                config.getString(DATABASE.key()),
                                config.getString(SERVER_TIME_ZONE.key()),
                                config.getString(USERNAME.key()),
                                config.getString(PASSWORD.key()),
                                null);
            }

            Properties tpProperties = new Properties();
            if (CheckConfigUtil.isValidParam(config, TIMEPLUS_CONFIG.key())) {
                config.getObject(TIMEPLUS_CONFIG.key())
                        .forEach(
                                (key, value) ->
                                        tpProperties.put(key, String.valueOf(value.unwrapped())));
            }

            if (isCredential) {
                tpProperties.put("user", config.getString(USERNAME.key()));
                tpProperties.put("password", config.getString(PASSWORD.key()));
            }

            TimeplusProxy proxy = new TimeplusProxy(nodes.get(0));
            // TODO: there could be no table setting to sync all tables
            Map<String, String> tableSchema =
                    proxy.getTimeplusTableSchema(config.getString(TABLE.key()));
            String shardKey = null;
            String shardKeyType = null;
            TimeplusTable table =
                    proxy.getTimeplusTable(
                            config.getString(DATABASE.key()), config.getString(TABLE.key()));
            if (config.getBoolean(SPLIT_MODE.key())) {
                if (!"Distributed".equals(table.getEngine())) {
                    throw new TimeplusConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "split mode only support table which engine is "
                                    + "'Distributed' engine at now");
                }
                if (config.hasPath(SHARDING_KEY.key())) {
                    shardKey = config.getString(SHARDING_KEY.key());
                    shardKeyType = tableSchema.get(shardKey);
                }
            }
            ShardMetadata metadata;

            if (isCredential) {
                metadata =
                        new ShardMetadata(
                                shardKey,
                                shardKeyType,
                                table.getSortingKey(),
                                config.getString(DATABASE.key()),
                                config.getString(TABLE.key()),
                                table.getEngine(),
                                config.getBoolean(SPLIT_MODE.key()),
                                new Shard(1, 1, nodes.get(0)),
                                config.getString(USERNAME.key()),
                                config.getString(PASSWORD.key()));
            } else {
                metadata =
                        new ShardMetadata(
                                shardKey,
                                shardKeyType,
                                table.getSortingKey(),
                                config.getString(DATABASE.key()),
                                config.getString(TABLE.key()),
                                table.getEngine(),
                                config.getBoolean(SPLIT_MODE.key()),
                                new Shard(1, 1, nodes.get(0)));
            }

            proxy.close();

            String[] primaryKeys = null;
            if (config.hasPath(PRIMARY_KEY.key())) {
                String primaryKey = config.getString(PRIMARY_KEY.key());
                if (shardKey != null && !Objects.equals(primaryKey, shardKey)) {
                    throw new TimeplusConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "sharding_key and primary_key must be consistent to ensure correct processing of cdc events");
                }
                primaryKeys = new String[] {primaryKey};
            }
            boolean supportUpsert = SUPPORT_UPSERT.defaultValue();
            if (config.hasPath(SUPPORT_UPSERT.key())) {
                supportUpsert = config.getBoolean(SUPPORT_UPSERT.key());
            }
            boolean allowExperimentalLightweightDelete =
                    ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE.defaultValue();
            if (config.hasPath(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE.key())) {
                allowExperimentalLightweightDelete =
                        config.getBoolean(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE.key());
            }
            this.option =
                    ReaderOption.builder()
                            .shardMetadata(metadata)
                            .properties(tpProperties)
                            .tableEngine(table.getEngine())
                            .tableSchema(tableSchema)
                            .bulkSize(config.getInt(BULK_SIZE.key()))
                            .primaryKeys(primaryKeys)
                            .supportUpsert(supportUpsert)
                            .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
                            .build();
        }
    */
    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();

        String sinkTableName = config.get(TABLE);

        if (isBlank(sinkTableName)) {
            sinkTableName = catalogTable.getTableId().getTableName();
        }

        ic("sinkTableName",sinkTableName);

        // get source table relevant information
        TableIdentifier tableId = catalogTable.getTableId();
        String sourceDatabaseName = tableId.getDatabaseName();
        //String sourceSchemaName = tableId.getSchemaName();
        String sourceTableName = tableId.getTableName();
        // get sink table relevant information
        String sinkDatabaseName = config.get(DATABASE);

        ic("sourceTableName",sourceTableName);
        // to replace
        sinkDatabaseName =
                sinkDatabaseName.replace(
                        REPLACE_DATABASE_NAME_KEY,
                        sourceDatabaseName != null ? sourceDatabaseName : "");
        String finalTableName = this.replaceFullTableName(sinkTableName, tableId);
        ic("finalTableName",finalTableName);

        // rebuild TableIdentifier and catalogTable
        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(), sinkDatabaseName, null, finalTableName);
        catalogTable =
                CatalogTable.of(
                        newTableId,
                        catalogTable.getTableSchema(),
                        catalogTable.getOptions(),
                        catalogTable.getPartitionKeys(),
                        catalogTable.getCatalogName());

        Properties tpProperties = new Properties();
        if (config.getOptional(TIMEPLUS_CONFIG).isPresent()) {
            tpProperties.putAll(config.get(TIMEPLUS_CONFIG));
        }

        boolean supportUpsert = config.get(SUPPORT_UPSERT);
        boolean allowExperimentalLightweightDelete =
                config.get(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE);
        ReaderOption readerOption =
                ReaderOption.builder()
                        .tableName(finalTableName)
                        .properties(tpProperties)
                        .bulkSize(config.get(BULK_SIZE))
                        .supportUpsert(supportUpsert)
                        .schemaSaveMode(config.get(SCHEMA_SAVE_MODE))
                        .dataSaveMode(config.get(DATA_SAVE_MODE))
                        .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
                        .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                        .build();

        CatalogTable finalCatalogTable = catalogTable;
        ic("SAVE_MODE_CREATE_TEMPLATE",config.get(SAVE_MODE_CREATE_TEMPLATE));
        return () -> new TimeplusSink(finalCatalogTable, readerOption, config);
    }

    private String replaceFullTableName(String original, TableIdentifier tableId) {
        if (!isBlank(tableId.getDatabaseName())) {
            original = original.replace(REPLACE_DATABASE_NAME_KEY, tableId.getDatabaseName());
        }
        if (!isBlank(tableId.getSchemaName())) {
            original = original.replace(REPLACE_SCHEMA_NAME_KEY, tableId.getSchemaName());
        }
        if (!isBlank(tableId.getTableName())) {
            original = original.replace(REPLACE_TABLE_NAME_KEY, tableId.getTableName());
        }
        return original;
    }

    public static boolean isBlank(final CharSequence cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(cs.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }
}