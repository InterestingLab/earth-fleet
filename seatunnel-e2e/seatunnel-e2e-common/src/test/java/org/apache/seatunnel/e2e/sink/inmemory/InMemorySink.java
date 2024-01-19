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

package org.apache.seatunnel.e2e.sink.inmemory;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.Optional;

public class InMemorySink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        InMemoryState,
                        InMemoryCommitInfo,
                        InMemoryAggregatedCommitInfo>,
                SupportMultiTableSink {
    @Override
    public String getPluginName() {
        return "InMemorySink";
    }

    @Override
    public SinkWriter<SeaTunnelRow, InMemoryCommitInfo, InMemoryState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new InMemorySinkWriter();
    }

    @Override
    public Optional<Serializer<InMemoryCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<InMemoryCommitInfo, InMemoryAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(new InMemoryAggregatedCommitter());
    }

    @Override
    public Optional<Serializer<InMemoryAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }
}
