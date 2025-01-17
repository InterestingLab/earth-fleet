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

import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MultiTableTtlWriterTest {
    ExecutorService checkPointThread = Executors.newSingleThreadExecutor();

    @Test
    public void testMultiTableTtlWriter() throws IOException {

        int multiTableWriterTtl = 5;
        int threads = 2;
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> ttlWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        DefaultSinkWriterContext defaultSinkWriterContext = new DefaultSinkWriterContext(1, 1);
        for (int i = 0; i < threads; i++) {
            TestSink testSink = new TestSink("test_table" + i);
            ttlWriters.put(
                    SinkIdentifier.of("test_table" + i, 0),
                    new MultiTableTtlWriter(
                            ttlWriters,
                            "test_table" + i,
                            0,
                            1,
                            testSink,
                            null,
                            multiTableWriterTtl));
            sinkWritersContext.put(
                    SinkIdentifier.of("test_table" + i, 0), new DefaultSinkWriterContext(1, 1));
        }
        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(ttlWriters, 1, sinkWritersContext);

        List<ConcurrentMap<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>>> ttlWritersWithIndex =
                multiTableSinkWriter.getSinkWritersWithIndex();

        Assertions.assertEquals(1, ttlWritersWithIndex.size());
        Assertions.assertEquals(threads, ttlWritersWithIndex.get(0).size());

        Map<String, MultiTableTtlWriter> multiTableTtlWritersWithTableId = new HashMap<>();

        AtomicInteger idx = new AtomicInteger(0);
        ttlWritersWithIndex
                .get(0)
                .values()
                .forEach(
                        writer -> {
                            MultiTableTtlWriter multiTableTtlWriter = (MultiTableTtlWriter) writer;
                            multiTableTtlWritersWithTableId.put(
                                    multiTableTtlWriter.getTableIdentifier(), multiTableTtlWriter);
                            if (idx.getAndIncrement() == 0) {
                                // first writer should be prepared for resourceManager
                                Assertions.assertFalse(multiTableTtlWriter.isClosed());
                            } else {
                                // the other writers should not be prepared
                                Assertions.assertTrue(multiTableTtlWriter.isClosed());
                            }
                        });

        {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "test"});
            row.setTableId("test_table0");
            multiTableSinkWriter.write(row);

            SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {2, "test2"});
            row2.setTableId("test_table0");
            multiTableSinkWriter.write(row2);

            // simulate checkpoint
            checkpoint(multiTableSinkWriter);
        }
        sleep(6000);
        {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "test"});
            row.setTableId("test_table1");
            multiTableSinkWriter.write(row);

            sleep(500);
            // the writer of `test_table1` is prepared
            Assertions.assertFalse(multiTableTtlWritersWithTableId.get("test_table1").isClosed());

            SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {2, "test2"});
            row2.setTableId("test_table1");
            multiTableSinkWriter.write(row2);

            // simulate checkpoint
            checkpoint(multiTableSinkWriter);

            sleep(500);

            // the writer of `test_table0` is closed
            Assertions.assertTrue(multiTableTtlWritersWithTableId.get("test_table0").isClosed());
        }
        sleep(6000);
        {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {3, "test3"});
            row.setTableId("test_table0");
            multiTableSinkWriter.write(row);

            sleep(500);
            Assertions.assertFalse(multiTableTtlWritersWithTableId.get("test_table0").isClosed());

            SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {4, "test4"});
            row2.setTableId("test_table0");
            multiTableSinkWriter.write(row2);

            // simulate checkpoint
            checkpoint(multiTableSinkWriter);

            sleep(500);

            // the writer of `test_table1` is closed
            Assertions.assertTrue(multiTableTtlWritersWithTableId.get("test_table1").isClosed());
        }
        sleep(2000);
        {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {3, "test3"});
            row.setTableId("test_table1");
            multiTableSinkWriter.write(row);

            sleep(500);
            // the writer of `test_table1` is prepared
            Assertions.assertFalse(multiTableTtlWritersWithTableId.get("test_table1").isClosed());

            SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {4, "test4"});
            row2.setTableId("test_table1");
            multiTableSinkWriter.write(row2);

            // simulate checkpoint
            checkpoint(multiTableSinkWriter);

            sleep(500);

            // the writer of `test_table0` is not closed
            Assertions.assertFalse(multiTableTtlWritersWithTableId.get("test_table0").isClosed());
        }

        multiTableSinkWriter.close();

        Assertions.assertTrue(multiTableTtlWritersWithTableId.get("test_table0").isClosed());
        Assertions.assertTrue(multiTableTtlWritersWithTableId.get("test_table1").isClosed());
    }

    private void checkpoint(MultiTableSinkWriter multiTableSinkWriter) {
        checkPointThread.submit(
                () -> {
                    try {
                        System.out.println("simulate checkpoint");
                        multiTableSinkWriter.snapshotState(0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    static class TestSink implements SeaTunnelSink {
        String tableId;

        public TestSink(String tableId) {
            this.tableId = tableId;
        }

        @Override
        public String getPluginName() {
            return "multi_table_sink_test";
        }

        @Override
        public SinkWriter createWriter(SinkWriter.Context context) throws IOException {
            System.out.println(LocalDateTime.now() + ": create writer of " + tableId);
            return new TestSinkWriter(tableId);
        }
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
