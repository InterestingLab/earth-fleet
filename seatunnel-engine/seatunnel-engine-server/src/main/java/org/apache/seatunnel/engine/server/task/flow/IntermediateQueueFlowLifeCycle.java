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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.ClosedSign;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

public class IntermediateQueueFlowLifeCycle extends AbstractFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>>,
        OneOutputFlowLifeCycle<Record<?>> {

    private final BlockingQueue<Record<?>> queue;

    public IntermediateQueueFlowLifeCycle(SeaTunnelTask runningTask,
                                          CompletableFuture<Void> completableFuture,
                                          BlockingQueue<Record<?>> queue) {
        super(runningTask, completableFuture);
        this.queue = queue;
    }

    @Override
    public void received(Record<?> record) {
        try {
            // TODO support batch put
            queue.put(record);
            if (record.getData() instanceof ClosedSign) {
                this.close();
            } else if (record.getData() instanceof CheckpointBarrier) {
                CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
                runningTask.ack(barrier.getId());
            }
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void collect(Collector<Record<?>> collector) throws Exception {
        while (true) {
            Record<?> record = queue.poll();
            if (record != null) {
                collector.collect(record);
                if (record.getData() instanceof ClosedSign) {
                    this.close();
                } else if (record.getData() instanceof CheckpointBarrier) {
                    CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
                    runningTask.ack(barrier.getId());
                }
            } else {
                break;
            }
        }
    }
}
