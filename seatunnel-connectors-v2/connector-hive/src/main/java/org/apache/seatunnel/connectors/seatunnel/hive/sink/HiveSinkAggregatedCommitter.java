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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer.HdfsUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveSinkAggregatedCommitter implements SinkAggregatedCommitter<HiveCommitInfo, HiveAggregatedCommitInfo> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSinkAggregatedCommitter.class);

    @Override
    public List<HiveAggregatedCommitInfo> commit(List<HiveAggregatedCommitInfo> aggregatedCommitInfoList) throws IOException {
        if (aggregatedCommitInfoList == null || aggregatedCommitInfoList.size() == 0) {
            return null;
        }
        List errorAggregatedCommitInfoList = new ArrayList();
        aggregatedCommitInfoList.stream().forEach(aggregateCommitInfo -> {
            try {
                Map<String, String> needMoveFiles = aggregateCommitInfo.getNeedMoveFiles();
                for (Map.Entry<String, String> entry : needMoveFiles.entrySet()) {
                    HdfsUtils.renameFile(entry.getKey(), entry.getValue(), true);
                }
            } catch (IOException e) {
                LOGGER.error("commit aggregateCommitInfo error ", e);
                errorAggregatedCommitInfoList.add(aggregateCommitInfo);
            }
        });

        return errorAggregatedCommitInfoList;
    }

    @Override
    public HiveAggregatedCommitInfo combine(List<HiveCommitInfo> commitInfos) {
        if (commitInfos == null || commitInfos.size() == 0) {
            return null;
        }
        Map<String, String> aggregateCommitInfo = new HashMap<>();
        commitInfos.stream().forEach(commitInfo -> {
            aggregateCommitInfo.putAll(commitInfo.getNeedMoveFiles());
        });
        return new HiveAggregatedCommitInfo(aggregateCommitInfo);
    }

    @Override
    public void abort(List<HiveAggregatedCommitInfo> aggregatedCommitInfoList) throws Exception {
        if (aggregatedCommitInfoList == null || aggregatedCommitInfoList.size() == 0) {
            return;
        }
        aggregatedCommitInfoList.stream().forEach(aggregateCommitInfo -> {
            try {
                Map<String, String> needMoveFiles = aggregateCommitInfo.getNeedMoveFiles();
                for (Map.Entry<String, String> entry : needMoveFiles.entrySet()) {
                    HdfsUtils.renameFile(entry.getValue(), entry.getKey(), true);
                }
            } catch (IOException e) {
                LOGGER.error("abort aggregateCommitInfo error ", e);
            }
        });
    }

    @Override
    public void close() throws IOException {
    }
}
