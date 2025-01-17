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

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportResourceShare;

import lombok.Getter;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class MultiTablePreparedSinkAggregatedCommitter
        implements SinkAggregatedCommitter, SupportResourceShare {
    private SeaTunnelSink sink;
    private int queueIndex;
    @Getter private SinkAggregatedCommitter<?, ?> committer;
    private volatile MultiTableResourceManager resourceManager;
    @Getter private volatile boolean isPrepared = false;
    private volatile boolean isInitialized = false;

    public MultiTablePreparedSinkAggregatedCommitter(SeaTunnelSink sink) {
        this.sink = sink;
    }

    public SinkAggregatedCommitter<?, ?> prepare() {
        if (committer == null) {
            try {
                Optional<SinkAggregatedCommitter<?, ?>> sinkOptional =
                        sink.createAggregatedCommitter();
                if (sinkOptional.isPresent()) {
                    committer = sinkOptional.get();
                    isPrepared = true;
                    if (committer instanceof SupportMultiTableSinkAggregatedCommitter
                            && resourceManager != null) {
                        ((SupportMultiTableSinkAggregatedCommitter<?>) committer)
                                .setMultiTableResourceManager(resourceManager, queueIndex);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return this.committer;
    }

    @Override
    public void init() {
        if (isInitialized) {
            return;
        }
        prepare();
        if (committer != null) {
            committer.init();
        }
        isInitialized = true;
    }

    @Override
    public MultiTableResourceManager<?> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        if (resourceManager != null) {
            return resourceManager;
        }
        prepare();
        if (committer instanceof SupportMultiTableSinkAggregatedCommitter) {
            resourceManager =
                    ((SupportMultiTableSinkAggregatedCommitter<?>) committer)
                            .initMultiTableResourceManager(tableSize, queueSize);
            return resourceManager;
        } else {
            return null;
        }
    }

    public void setMultiTableResourceManager(
            MultiTableResourceManager resourceManager, int queueIndex) {
        if (this.resourceManager == null) {
            this.resourceManager = resourceManager;
            this.queueIndex = queueIndex;
            if (committer != null
                    && committer instanceof SupportMultiTableSinkAggregatedCommitter) {
                ((SupportMultiTableSinkAggregatedCommitter<?>) committer)
                        .setMultiTableResourceManager(resourceManager, queueIndex);
            }
        }
    }

    @Override
    public List<?> commit(List aggregatedCommitInfo) throws IOException {
        prepare();
        init();
        return committer.commit(aggregatedCommitInfo);
    }

    @Override
    public Object combine(List commitInfos) {
        prepare();
        init();
        return committer.combine(commitInfos);
    }

    @Override
    public void abort(List aggregatedCommitInfo) throws Exception {
        prepare();
        init();
        committer.abort(aggregatedCommitInfo);
    }

    @Override
    public void close() throws IOException {
        if (committer != null) {
            committer.close();
            isInitialized = false;
            isPrepared = false;
        }
    }
}
