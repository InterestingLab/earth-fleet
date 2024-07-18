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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink.file;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.timeplus.config.FileReaderOption;
import org.apache.seatunnel.connectors.seatunnel.timeplus.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.TimeplusProxy;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TPFileAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TPFileCommitInfo;

import com.timeplus.proton.client.ProtonException;
import com.timeplus.proton.client.ProtonRequest;
import com.timeplus.proton.client.ProtonResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimeplusFileSinkAggCommitter
        implements SinkAggregatedCommitter<TPFileCommitInfo, TPFileAggCommitInfo> {

    private transient TimeplusProxy proxy;
    private final TimeplusTable clickhouseTable;

    private final FileReaderOption fileReaderOption;

    public TimeplusFileSinkAggCommitter(FileReaderOption readerOption) {
        fileReaderOption = readerOption;
        proxy = new TimeplusProxy(readerOption.getShardMetadata().getDefaultShard().getNode());
        clickhouseTable =
                proxy.getClickhouseTable(
                        readerOption.getShardMetadata().getDatabase(),
                        readerOption.getShardMetadata().getTable());
    }

    @Override
    public List<TPFileAggCommitInfo> commit(List<TPFileAggCommitInfo> aggregatedCommitInfo)
            throws IOException {
        aggregatedCommitInfo.forEach(
                commitInfo ->
                        commitInfo
                                .getDetachedFiles()
                                .forEach(
                                        (shard, files) -> {
                                            try {
                                                this.attachFileToClickhouse(shard, files);
                                            } catch (ProtonException e) {
                                                throw new SeaTunnelException(
                                                        "failed commit file to clickhouse", e);
                                            }
                                        }));
        return new ArrayList<>();
    }

    @Override
    public TPFileAggCommitInfo combine(List<TPFileCommitInfo> commitInfos) {
        Map<Shard, List<String>> files = new HashMap<>();
        commitInfos.forEach(
                infos ->
                        infos.getDetachedFiles()
                                .forEach(
                                        (shard, file) -> {
                                            if (files.containsKey(shard)) {
                                                files.get(shard).addAll(file);
                                            } else {
                                                files.put(shard, file);
                                            }
                                        }));
        return new TPFileAggCommitInfo(files);
    }

    @Override
    public void abort(List<TPFileAggCommitInfo> aggregatedCommitInfo) throws Exception {}

    private TimeplusProxy getProxy() {
        if (proxy != null) {
            return proxy;
        }
        synchronized (this) {
            if (proxy != null) {
                return proxy;
            }
            proxy =
                    new TimeplusProxy(
                            fileReaderOption.getShardMetadata().getDefaultShard().getNode());
            return proxy;
        }
    }

    @Override
    public void close() throws IOException {
        if (proxy != null) {
            proxy.close();
        }
    }

    private void attachFileToClickhouse(Shard shard, List<String> clickhouseLocalFiles)
            throws ProtonException {
        ProtonRequest<?> request = getProxy().getProtonConnection(shard);
        for (String clickhouseLocalFile : clickhouseLocalFiles) {
            ProtonResponse response =
                    request.query(
                                    String.format(
                                            "ALTER TABLE %s ATTACH PART '%s'",
                                            clickhouseTable.getLocalTableName(),
                                            clickhouseLocalFile.substring(
                                                    clickhouseLocalFile.lastIndexOf("/") + 1)))
                            .executeAndWait();
            response.close();
        }
    }
}
