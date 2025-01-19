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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.IndexInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.source.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.source.ElasticsearchRecord;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.source.SeaTunnelRowDeserializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE;

public class ElasticsearchRowSerializerTest {
    private static final String INDEX = "st_index";
    private static final String PRIMARY_KEY = "id";
    private static final String NAME_FIELD = "name";
    private static final String DATE_FIELD = "date";

    private ElasticsearchClusterInfo clusterInfo;

    @BeforeEach
    public void setUp() {
        clusterInfo = ElasticsearchClusterInfo.builder().clusterVersion("8.0.0").build();
    }

    private IndexInfo createIndexInfo(List<String> primaryKeys) {
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(SinkConfig.INDEX.key(), INDEX);
        if (primaryKeys != null) {
            confMap.put(SinkConfig.PRIMARY_KEYS.key(), primaryKeys);
        }
        return new IndexInfo(INDEX, ReadonlyConfig.fromMap(confMap));
    }

    private ElasticsearchRowSerializer createSerializer(
            IndexInfo indexInfo, SeaTunnelRowType schema) {
        return new ElasticsearchRowSerializer(clusterInfo, indexInfo, schema);
    }

    private SeaTunnelRowType createBasicSchema() {
        return new SeaTunnelRowType(
                new String[] {PRIMARY_KEY, NAME_FIELD},
                new SeaTunnelDataType[] {STRING_TYPE, STRING_TYPE});
    }

    @Test
    public void testSerializeUpsert() {
        IndexInfo indexInfo = createIndexInfo(Collections.singletonList(PRIMARY_KEY));
        SeaTunnelRowType schema = createBasicSchema();
        ElasticsearchRowSerializer serializer = createSerializer(indexInfo, schema);

        String id = "0001";
        String name = "jack";
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setRowKind(RowKind.UPDATE_AFTER);

        String expected =
                String.format(
                        "{ \"update\" :{\"_index\":\"%s\",\"_id\":\"%s\"} }\n"
                                + "{ \"doc\" :{\"name\":\"%s\",\"id\":\"%s\"}, \"doc_as_upsert\" : true }",
                        INDEX, id, name, id);

        Assertions.assertEquals(expected, serializer.serializeRow(row));
    }

    @Test
    public void testSerializeUpsertWithoutKey() {
        IndexInfo indexInfo = createIndexInfo(null);
        SeaTunnelRowType schema = createBasicSchema();
        ElasticsearchRowSerializer serializer = createSerializer(indexInfo, schema);

        String id = "0001";
        String name = "jack";
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setRowKind(RowKind.UPDATE_AFTER);

        String expected =
                String.format(
                        "{ \"index\" :{\"_index\":\"%s\"} }\n{\"name\":\"%s\",\"id\":\"%s\"}",
                        INDEX, name, id);

        Assertions.assertEquals(expected, serializer.serializeRow(row));
    }

    @Test
    public void testSerializeDelete() {
        IndexInfo indexInfo = createIndexInfo(Collections.singletonList(PRIMARY_KEY));
        SeaTunnelRowType schema = createBasicSchema();
        ElasticsearchRowSerializer serializer = createSerializer(indexInfo, schema);

        String id = "0001";
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, "jack"});
        row.setRowKind(RowKind.DELETE);

        String expected =
                String.format("{ \"delete\" :{\"_index\":\"%s\",\"_id\":\"%s\"} }", INDEX, id);

        Assertions.assertEquals(expected, serializer.serializeRow(row));
    }

    private CatalogTable createCatalogTable(String format) {
        SeaTunnelDataType<?>[] fieldTypes = {STRING_TYPE, LOCAL_DATE_TIME_TYPE};
        HashMap<String, Object> options = new HashMap<>();
        options.put("format", format);

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        schemaBuilder.column(
                PhysicalColumn.of(PRIMARY_KEY, fieldTypes[0], 10L, true, null, null, null, null));
        schemaBuilder.column(
                PhysicalColumn.of(DATE_FIELD, fieldTypes[1], 10L, true, null, null, null, options));

        return CatalogTable.of(
                TableIdentifier.of(null, null, null, INDEX),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "It is converted from RowType and only has column information.");
    }

    private SeaTunnelRowDeserializer createDeserializer(String format) {
        CatalogTable catalogTable = createCatalogTable(format);
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setCatalogTable(catalogTable);

        return new DefaultSeaTunnelRowDeserializer(
                new SeaTunnelRowType(
                        new String[] {PRIMARY_KEY, DATE_FIELD},
                        new SeaTunnelDataType[] {STRING_TYPE, LOCAL_DATE_TIME_TYPE}),
                sourceConfig);
    }

    @Test
    public void testDateConversions() {
        String index = "st_index";
        SeaTunnelRowDeserializer millisDeserializer =
                createDeserializer("epoch_millis || epoch_second");
        SeaTunnelRowDeserializer secondsDeserializer =
                createDeserializer("epoch_second || epoch_millis");

        // Test with seconds
        testDateConversion(millisDeserializer, index, 1689571957, true);
        testDateConversion(secondsDeserializer, index, 1689571957, false);

        // Test with milliseconds
        testDateConversion(millisDeserializer, index, 1689571957000L, true);
        testDateConversion(secondsDeserializer, index, 1689571957000L, false);
    }

    private void testDateConversion(
            SeaTunnelRowDeserializer deserializer,
            String index,
            Number dateValue,
            boolean isMillis) {
        Map<String, Object> doc =
                Stream.of(
                                new AbstractMap.SimpleEntry<>("id", 1),
                                new AbstractMap.SimpleEntry<>("date", dateValue))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        LocalDateTime field =
                (LocalDateTime)
                        deserializer
                                .deserialize(
                                        new ElasticsearchRecord(
                                                doc, Arrays.asList("id", "date"), index))
                                .getField(1);

        LocalDateTime expected =
                isMillis
                        ? LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(dateValue.longValue()), ZoneOffset.UTC)
                        : LocalDateTime.ofEpochSecond(dateValue.longValue(), 0, ZoneOffset.UTC);

        Assertions.assertEquals(expected, field);
    }
}
