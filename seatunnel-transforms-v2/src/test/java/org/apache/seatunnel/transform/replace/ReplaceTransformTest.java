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

package org.apache.seatunnel.transform.replace;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ReplaceTransformTest {

    private static final CatalogTable DEFAULT_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("test", "Database-x", "Schema-x", "Table-x"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "f1",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            false,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f2",
                                            BasicType.STRING_TYPE,
                                            null,
                                            null,
                                            true,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f3",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            true,
                                            null,
                                            null))
                            .primaryKey(PrimaryKey.of("pk1", Arrays.asList("f1")))
                            .constraintKey(
                                    ConstraintKey.of(
                                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                                            "uk1",
                                            Arrays.asList(
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f2", ConstraintKey.ColumnSortType.ASC),
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f3",
                                                            ConstraintKey.ColumnSortType.ASC))))
                            .build(),
                    Collections.emptyMap(),
                    Collections.singletonList("f2"),
                    null);

    @Test
    public void testReplaceTransformSchemaChange() {

        // before schema change
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, "old string", 1L});
        inputRow.setTableId(DEFAULT_TABLE.getTablePath().getFullName());

        Map map = new HashMap<>();
        map.put("replace_field", "f2");
        map.put("pattern", "old string");
        map.put("replacement", "new string");

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(map);
        ReplaceTransform replaceTransform = new ReplaceTransform(readonlyConfig, DEFAULT_TABLE);

        replaceTransform.initRowContainerGenerator();

        Assertions.assertEquals(1, replaceTransform.getFieldIndex());
        SeaTunnelRow outputRow = replaceTransform.transform(inputRow);

        Assertions.assertEquals(
                "SeaTunnelRow{tableId=Database-x.Schema-x.Table-x, kind=+I, fields=[1, new string, 1]}",
                outputRow.toString());

        // after schema change
        AlterTableAddColumnEvent addColumnEvent =
                AlterTableAddColumnEvent.addAfter(
                        DEFAULT_TABLE.getTableId(),
                        PhysicalColumn.of("f4", BasicType.LONG_TYPE, null, null, true, null, null),
                        "f1");

        replaceTransform.mapSchemaChangeEvent(addColumnEvent);

        SeaTunnelRow inputRowTwo = new SeaTunnelRow(new Object[] {2L, 2L, "old string", 2L});
        inputRowTwo.setTableId(DEFAULT_TABLE.getTablePath().getFullName());
        SeaTunnelRow outputRowTwo = replaceTransform.transform(inputRowTwo);

        Assertions.assertEquals(
                "SeaTunnelRow{tableId=Database-x.Schema-x.Table-x, kind=+I, fields=[2, 2, new string, 2]}",
                outputRowTwo.toString());
    }
}
