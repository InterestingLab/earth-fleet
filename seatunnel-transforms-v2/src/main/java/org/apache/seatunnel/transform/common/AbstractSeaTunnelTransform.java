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
package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;

import org.apache.groovy.parser.antlr4.util.StringUtils;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public abstract class AbstractSeaTunnelTransform<T, R> implements SeaTunnelTransform<T> {

    protected final ErrorHandleWay rowErrorHandleWay;
    protected CatalogTable inputCatalogTable;

    protected volatile CatalogTable outputCatalogTable;

    protected String inputTableName;

    protected String outTableName;

    @Override
    public void open() {}

    public AbstractSeaTunnelTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        this(
                config,
                inputCatalogTable,
                TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.defaultValue());
    }

    public AbstractSeaTunnelTransform(
            @NonNull ReadonlyConfig config,
            @NonNull CatalogTable catalogTable,
            ErrorHandleWay rowErrorHandleWay) {
        this.inputCatalogTable = catalogTable;
        this.rowErrorHandleWay = rowErrorHandleWay;
        List<String> pluginInputIdentifiers = config.get(CommonOptions.PLUGIN_INPUT);
        String pluginOutIdentifiers = config.get(CommonOptions.PLUGIN_OUTPUT);
        if (pluginInputIdentifiers != null && !pluginInputIdentifiers.isEmpty()) {
            this.inputTableName = pluginInputIdentifiers.get(0);
        } else {
            this.inputTableName = catalogTable.getTableId().getTableName();
        }
        if (!StringUtils.isEmpty(pluginOutIdentifiers)) {
            this.outTableName = pluginOutIdentifiers;
        } else {
            this.outTableName = catalogTable.getTableId().getTableName();
        }
    }

    public CatalogTable getProducedCatalogTable() {
        if (outputCatalogTable == null) {
            synchronized (this) {
                if (outputCatalogTable == null) {
                    outputCatalogTable = transformCatalogTable();
                }
            }
        }

        return outputCatalogTable;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(getProducedCatalogTable());
    }

    private CatalogTable transformCatalogTable() {
        TableIdentifier tableIdentifier = transformTableIdentifier();
        TableSchema tableSchema = transformTableSchema();
        return CatalogTable.of(
                tableIdentifier,
                tableSchema,
                inputCatalogTable.getOptions(),
                inputCatalogTable.getPartitionKeys(),
                inputCatalogTable.getComment());
    }

    public R transform(SeaTunnelRow row) {
        try {
            return transformRow(row);
        } catch (ErrorDataTransformException e) {
            if (e.getErrorHandleWay() != null) {
                ErrorHandleWay errorHandleWay = e.getErrorHandleWay();
                if (errorHandleWay.allowSkipThisRow()) {
                    log.debug("Skip row due to error", e);
                    return null;
                }
                throw e;
            }
            if (rowErrorHandleWay.allowSkip()) {
                log.debug("Skip row due to error", e);
                return null;
            }
            throw e;
        }
    }

    /**
     * Outputs transformed row data.
     *
     * @param inputRow upstream input row data
     */
    protected abstract R transformRow(SeaTunnelRow inputRow);

    protected abstract TableSchema transformTableSchema();

    protected abstract TableIdentifier transformTableIdentifier();
}
