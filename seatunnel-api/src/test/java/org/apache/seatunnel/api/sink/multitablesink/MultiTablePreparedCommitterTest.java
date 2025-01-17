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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
public class MultiTablePreparedCommitterTest {
    @Test
    public void testMultiTablePreparedCommitter() throws IOException {
        int threads = 3;
        Map<String, MultiTablePreparedSinkAggregatedCommitter> preparedAggCommitters =
                new HashMap<>();
        for (int i = 0; i < threads; i++) {
            TestSink testSink = new TestSink("test_table" + i);
            preparedAggCommitters.put(
                    "test_table" + i, new MultiTablePreparedSinkAggregatedCommitter(testSink));
        }

        Map<String, SinkAggregatedCommitter<?, ?>> stringSinkAggregatedCommitterMap =
                preparedAggCommitters.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        MultiTableSinkAggregatedCommitter multiTableSinkAggregatedCommitter =
                new MultiTableSinkAggregatedCommitter(stringSinkAggregatedCommitterMap);

        multiTableSinkAggregatedCommitter.init();

        Assertions.assertTrue(preparedAggCommitters.get("test_table0").isPrepared());
        Assertions.assertFalse(preparedAggCommitters.get("test_table1").isPrepared());
        Assertions.assertFalse(preparedAggCommitters.get("test_table2").isPrepared());

        {
            List<MultiTableCommitInfo> commitInfos = new ArrayList<>();
            ConcurrentMap<SinkIdentifier, Object> commitInfo = new ConcurrentHashMap<>();
            commitInfo.put(SinkIdentifier.of("test_table0", 0), "test");
            commitInfos.add(new MultiTableCommitInfo(commitInfo));
            MultiTableAggregatedCommitInfo multiTableAggregatedCommitInfo =
                    multiTableSinkAggregatedCommitter.combine(commitInfos);
            List<MultiTableAggregatedCommitInfo> aggregatedCommitInfo = new ArrayList<>();
            aggregatedCommitInfo.add(multiTableAggregatedCommitInfo);
            multiTableSinkAggregatedCommitter.commit(aggregatedCommitInfo);

            Assertions.assertTrue(preparedAggCommitters.get("test_table0").isPrepared());
            Assertions.assertFalse(preparedAggCommitters.get("test_table1").isPrepared());
            Assertions.assertFalse(preparedAggCommitters.get("test_table2").isPrepared());
        }

        {
            List<MultiTableCommitInfo> commitInfos = new ArrayList<>();
            ConcurrentMap<SinkIdentifier, Object> commitInfo = new ConcurrentHashMap<>();
            commitInfo.put(SinkIdentifier.of("test_table1", 0), "test1");
            commitInfos.add(new MultiTableCommitInfo(commitInfo));
            MultiTableAggregatedCommitInfo multiTableAggregatedCommitInfo =
                    multiTableSinkAggregatedCommitter.combine(commitInfos);
            List<MultiTableAggregatedCommitInfo> aggregatedCommitInfo = new ArrayList<>();
            aggregatedCommitInfo.add(multiTableAggregatedCommitInfo);
            multiTableSinkAggregatedCommitter.commit(aggregatedCommitInfo);

            Assertions.assertTrue(preparedAggCommitters.get("test_table0").isPrepared());
            Assertions.assertTrue(preparedAggCommitters.get("test_table1").isPrepared());
            Assertions.assertFalse(preparedAggCommitters.get("test_table2").isPrepared());
        }

        {
            List<MultiTableCommitInfo> commitInfos = new ArrayList<>();
            ConcurrentMap<SinkIdentifier, Object> commitInfo = new ConcurrentHashMap<>();
            commitInfo.put(SinkIdentifier.of("test_table2", 0), "test2");
            commitInfos.add(new MultiTableCommitInfo(commitInfo));
            MultiTableAggregatedCommitInfo multiTableAggregatedCommitInfo =
                    multiTableSinkAggregatedCommitter.combine(commitInfos);
            List<MultiTableAggregatedCommitInfo> aggregatedCommitInfo = new ArrayList<>();
            aggregatedCommitInfo.add(multiTableAggregatedCommitInfo);
            multiTableSinkAggregatedCommitter.commit(aggregatedCommitInfo);

            Assertions.assertTrue(preparedAggCommitters.get("test_table0").isPrepared());
            Assertions.assertTrue(preparedAggCommitters.get("test_table1").isPrepared());
            Assertions.assertTrue(preparedAggCommitters.get("test_table2").isPrepared());
        }

        multiTableSinkAggregatedCommitter.close();

        Assertions.assertFalse(preparedAggCommitters.get("test_table0").isPrepared());
        Assertions.assertFalse(preparedAggCommitters.get("test_table1").isPrepared());
        Assertions.assertFalse(preparedAggCommitters.get("test_table2").isPrepared());
    }

    static class TestSink implements SeaTunnelSink {
        String tableId;

        public TestSink(String tableId) {
            this.tableId = tableId;
        }

        @Override
        public String getPluginName() {
            return "test_table_sink_test";
        }

        @Override
        public SinkWriter createWriter(SinkWriter.Context context) throws IOException {
            System.out.println(LocalDateTime.now() + ": create writer of " + tableId);
            return new TestSinkWriter(tableId);
        }

        @Override
        public Optional<TestCommitter> createAggregatedCommitter() throws IOException {
            return Optional.of(new TestCommitter(tableId));
        }
    }

    static class TestCommitter implements SinkAggregatedCommitter {
        String tableId;

        public TestCommitter(String tableId) {
            this.tableId = tableId;
        }

        @Override
        public List commit(List aggregatedCommitInfo) throws IOException {
            System.out.println("commit aggregated commit info for: " + tableId);
            return new ArrayList();
        }

        @Override
        public TestCommitInfo combine(List commitInfos) {
            System.out.println("combine commit info for: " + tableId);
            return new TestCommitInfo(commitInfos);
        }

        @Override
        public void abort(List aggregatedCommitInfo) throws Exception {}

        @Override
        public void close() throws IOException {
            System.out.println("close aggregated committer of: " + tableId);
        }
    }

    @Data
    @AllArgsConstructor
    static class TestCommitInfo implements Serializable {
        private final List<String> commitInfos;
    }

    static class TestSinkWriter
            implements SinkWriter<SeaTunnelRow, MultiTableSinkWriterTest.TestSinkState, Object>,
                    SupportMultiTableSinkWriter {
        String tableId;

        public TestSinkWriter(String tableId) {
            this.tableId = tableId;
        }

        @Override
        public void write(SeaTunnelRow seaTunnelRow) {
            System.out.println(tableId + ": " + Arrays.toString(seaTunnelRow.getFields()));
        }

        @Override
        public Optional<MultiTableSinkWriterTest.TestSinkState> prepareCommit() throws IOException {
            return Optional.of(new MultiTableSinkWriterTest.TestSinkState("test"));
        }

        @Override
        public List<Object> snapshotState(long checkpointId) throws IOException {
            return SinkWriter.super.snapshotState(checkpointId);
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() throws IOException {
            System.out.println(LocalDateTime.now() + ": close writer of " + tableId);
        }
    }
}
