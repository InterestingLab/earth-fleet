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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.serialization.FlinkRowSerialization;
import org.apache.seatunnel.translation.flink.serialization.WrappedRow;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.InvalidClassException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class FlinkSinkWriter<InputT, CommT, WriterStateT> implements SinkWriter<InputT, CommT, FlinkWriterState<WriterStateT>> {

    private final org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter;
    private final FlinkRowSerialization rowSerialization = new FlinkRowSerialization();
    private long checkpointId;

    FlinkSinkWriter(org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter,
                    long checkpointId) {
        this.sinkWriter = sinkWriter;
        this.checkpointId = checkpointId;
    }

    @Override
    public void write(InputT element, org.apache.flink.api.connector.sink.SinkWriter.Context context) throws IOException {
        if (element instanceof Row) {
            sinkWriter.write(rowSerialization.deserialize((Row) element));
        } else if (element instanceof WrappedRow) {
            sinkWriter.write(rowSerialization.deserialize(((WrappedRow) element).getRow()));
        } else {
            throw new InvalidClassException("only support Flink Row at now, the element Class is " + element.getClass());
        }
    }

    @Override
    public List<CommT> prepareCommit(boolean flush) throws IOException {
        Optional<CommT> commTOptional = sinkWriter.prepareCommit();
        return commTOptional.map(Collections::singletonList).orElse(Collections.emptyList());
    }

    @Override
    public List<FlinkWriterState<WriterStateT>> snapshotState() throws IOException {
        List<FlinkWriterState<WriterStateT>> states = sinkWriter.snapshotState(this.checkpointId)
                .stream().map(state -> new FlinkWriterState<>(this.checkpointId, state)).collect(Collectors.toList());
        this.checkpointId++;
        return states;
    }

    @Override
    public void close() throws Exception {
        sinkWriter.close();
    }
}
