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

package org.apache.seatunnel.engine.core.parse;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.Data;
import lombok.NonNull;
import scala.Serializable;
import scala.Tuple2;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser.checkProducedTypeEquals;

@Data
public class JobConfigParser {
    private static final ILogger LOGGER = Logger.getLogger(JobConfigParser.class);
    private IdGenerator idGenerator;
    private boolean isStartWithSavePoint;
    private MultipleTableJobConfigParser multipleTableJobConfigParser;
    private List<URL> commonPluginJars;

    public JobConfigParser(
            @NonNull IdGenerator idGenerator,
            @NonNull List<URL> commonPluginJars,
            MultipleTableJobConfigParser multipleTableJobConfigParser,
            boolean isStartWithSavePoint) {
        this.idGenerator = idGenerator;
        this.commonPluginJars = commonPluginJars;
        this.multipleTableJobConfigParser = multipleTableJobConfigParser;
        this.isStartWithSavePoint = isStartWithSavePoint;
    }

    public Tuple2<CatalogTable, Action> parseSource(
            Config config, JobConfig jobConfig, String tableId, int parallelism) {
        ImmutablePair<SeaTunnelSource, Set<URL>> tuple =
                ConnectorInstanceLoader.loadSourceInstance(
                        config, jobConfig.getJobContext(), commonPluginJars);
        final SeaTunnelSource source = tuple.getLeft();
        // old logic: prepare(initialization) -> set job context
        source.prepare(config);
        source.setJobContext(jobConfig.getJobContext());
        PluginUtil.ensureJobModeMatch(jobConfig.getJobContext(), source);
        String actionName =
                createSourceActionName(0, config.getString(CollectionConstants.PLUGIN_NAME));
        SourceAction action =
                new SourceAction(
                        idGenerator.getNextId(),
                        actionName,
                        tuple.getLeft(),
                        tuple.getRight(),
                        new HashSet<>());
        action.setParallelism(parallelism);
        SeaTunnelRowType producedType = (SeaTunnelRowType) tuple.getLeft().getProducedType();
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable(tableId, producedType);
        return new Tuple2<>(catalogTable, action);
    }

    public List<SinkAction<?, ?, ?, ?>> parseSinks(
            int configIndex,
            List<List<Tuple2<CatalogTable, Action>>> inputVertices,
            Config sinkConfig,
            JobConfig jobConfig) {
        List<SinkAction<?, ?, ?, ?>> sinkActions = new ArrayList<>();
        int spareParallelism = inputVertices.get(0).get(0)._2().getParallelism();
        if (inputVertices.size() > 1) {
            // union
            Set<Action> inputActions =
                    inputVertices.stream()
                            .flatMap(Collection::stream)
                            .map(Tuple2::_2)
                            .collect(Collectors.toCollection(LinkedHashSet::new));
            checkProducedTypeEquals(inputActions);
            SinkAction<?, ?, ?, ?> sinkAction =
                    parseSink(
                            configIndex,
                            sinkConfig,
                            jobConfig,
                            spareParallelism,
                            inputVertices
                                    .get(0)
                                    .get(0)
                                    ._1()
                                    .getTableSchema()
                                    .toPhysicalRowDataType(),
                            inputActions);
            sinkActions.add(sinkAction);
        } else {
            // sink template
            for (Tuple2<CatalogTable, Action> tableTuple : inputVertices.get(0)) {
                CatalogTable catalogTable = tableTuple._1();
                Action inputAction = tableTuple._2();
                int parallelism = inputAction.getParallelism();
                SinkAction<?, ?, ?, ?> sinkAction =
                        parseSink(
                                configIndex,
                                sinkConfig,
                                jobConfig,
                                parallelism,
                                catalogTable.getTableSchema().toPhysicalRowDataType(),
                                Collections.singleton(inputAction));
                sinkActions.add(sinkAction);
            }
        }
        return sinkActions;
    }

    private SinkAction<?, ?, ?, ?> parseSink(
            int configIndex,
            Config config,
            JobConfig jobConfig,
            int parallelism,
            SeaTunnelRowType rowType,
            Set<Action> inputActions) {
        final ImmutablePair<
                        SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>,
                        Set<URL>>
                tuple =
                        ConnectorInstanceLoader.loadSinkInstance(
                                config, jobConfig.getJobContext(), commonPluginJars);
        final SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> sink =
                tuple.getLeft();
        // old logic: prepare(initialization) -> set job context -> set row type (There is a logical
        // judgment that depends on before and after, not a simple set)
        sink.prepare(config);
        sink.setJobConfigContext(jobConfig.getJobContext(), jobConfig.getEnvConfig());
        sink.setTypeInfo(rowType);
        if (!isStartWithSavePoint) {
            multipleTableJobConfigParser.handleSaveMode(sink);
        }
        final String actionName =
                createSinkActionName(configIndex, tuple.getLeft().getPluginName());
        final SinkAction action =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        actionName,
                        new ArrayList<>(inputActions),
                        sink,
                        tuple.getRight(),
                        new HashSet<>());
        action.setParallelism(parallelism);
        return action;
    }

    static String createSourceActionName(int configIndex, String pluginName) {
        return String.format("Source[%s]-%s", configIndex, pluginName);
    }

    static String createSinkActionName(int configIndex, String pluginName) {
        return String.format("Sink[%s]-%s", configIndex, pluginName);
    }

    static String createSinkActionName(int configIndex, String pluginName, String table) {
        return String.format("Sink[%s]-%s-%s", configIndex, pluginName, table);
    }

    static String createTransformActionName(int configIndex, String pluginName) {
        return String.format("Transform[%s]-%s", configIndex, pluginName);
    }
}
