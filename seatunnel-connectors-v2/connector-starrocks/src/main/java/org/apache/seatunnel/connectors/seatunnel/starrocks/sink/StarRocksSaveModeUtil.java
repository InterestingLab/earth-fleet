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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.sink.SaveModeConstants;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.util.CreateTableParser;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class StarRocksSaveModeUtil {

    public static String fillingCreateSql(
            String template, String database, String table, TableSchema tableSchema) {
        String primaryKey =
                tableSchema.getPrimaryKey().getColumnNames().stream()
                        .map(r -> "`" + r + "`")
                        .collect(Collectors.joining(","));

        Map<String, CreateTableParser.ColumnInfo> columnInTemplate =
                CreateTableParser.getColumnList(template);
        template = mergeColumnInTemplate(columnInTemplate, tableSchema, template);

        String rowTypeFields =
                tableSchema.getColumns().stream()
                        .filter(column -> !columnInTemplate.containsKey(column.getName()))
                        .map(StarRocksSaveModeUtil::columnToStarrocksType)
                        .collect(Collectors.joining(",\n"));
        return template.replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.DATABASE), database)
                .replaceAll(String.format("\\$\\{%s\\}", SaveModeConstants.TABLE_NAME), table)
                .replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.ROWTYPE_FIELDS),
                        rowTypeFields)
                .replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.ROWTYPE_PRIMARY_KEY),
                        primaryKey);
    }

    private static String columnToStarrocksType(Column column) {
        checkNotNull(column, "The column is required.");
        return String.format(
                "`%s` %s %s ",
                column.getName(),
                dataTypeToStarrocksType(column.getDataType()),
                column.isNullable() ? "NULL" : "NOT NULL");
    }

    private static String mergeColumnInTemplate(
            Map<String, CreateTableParser.ColumnInfo> columnInTemplate,
            TableSchema tableSchema,
            String template) {
        int offset = 0;
        Map<String, Column> columnMap =
                tableSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        for (String col : columnInTemplate.keySet()) {
            CreateTableParser.ColumnInfo columnInfo = columnInTemplate.get(col);
            if (StringUtils.isEmpty(columnInfo.getInfo())) {
                if (columnMap.containsKey(col)) {
                    Column column = columnMap.get(col);
                    String newCol = columnToStarrocksType(column);
                    template =
                            template.substring(
                                            0,
                                            columnInfo.getIndex()
                                                    + offset
                                                    - columnInfo.getName().length())
                                    + newCol
                                    + template.substring(offset + columnInfo.getIndex());
                    offset += newCol.length() - columnInfo.getName().length();
                } else {
                    throw new IllegalArgumentException("Can't find column " + col + " in table.");
                }
            }
        }
        return template;
    }

    private static String dataTypeToStarrocksType(SeaTunnelDataType<?> dataType) {
        checkNotNull(dataType, "The SeaTunnel's data type is required.");
        switch (dataType.getSqlType()) {
            case NULL:
            case TIME:
                throw new IllegalArgumentException(
                        "Unsupported SeaTunnel's data type: " + dataType);
            case STRING:
            case BYTES:
                return "STRING";
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DATE:
                return "DATE";
            case TIMESTAMP:
                return "DATETIME";
            case ARRAY:
                return "ARRAY<"
                        + dataTypeToStarrocksType(((ArrayType<?, ?>) dataType).getElementType())
                        + ">";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return String.format(
                        "Decimal(%d, %d)", decimalType.getPrecision(), decimalType.getScale());
            case MAP:
            case ROW:
                return "JSON";
            default:
        }
        throw new IllegalArgumentException("Unsupported SeaTunnel's data type: " + dataType);
    }
}
