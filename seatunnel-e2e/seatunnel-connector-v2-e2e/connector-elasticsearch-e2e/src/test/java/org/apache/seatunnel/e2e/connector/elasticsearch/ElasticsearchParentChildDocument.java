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

package org.apache.seatunnel.e2e.connector.elasticsearch;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.commons.io.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

@Slf4j
public class ElasticsearchParentChildDocument extends TestSuiteBase implements TestResource {

    private Map<String, Object> testDataset1;

    private Map<String, Object> testDataset2;

    private ElasticsearchContainer container;

    private EsRestClient esRestClient;

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        container =
                new ElasticsearchContainer(
                                DockerImageName.parse("elasticsearch:5.6.16")
                                        .asCompatibleSubstituteFor(
                                                "docker.elastic.co/elasticsearch/elasticsearch"))
                        .withNetwork(NETWORK)
                        .withEnv("cluster.routing.allocation.disk.threshold_enabled", "false")
                        .withNetworkAliases("elasticsearch")
                        .withPassword("elasticsearch")
                        .withStartupAttempts(5)
                        .withStartupTimeout(Duration.ofMinutes(5))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("elasticsearch:5.6.16")));
        Startables.deepStart(Stream.of(container)).join();
        log.info("Elasticsearch container started");
        esRestClient =
                EsRestClient.createInstance(
                        Lists.newArrayList("http://" + container.getHttpHostAddress()),
                        Optional.of("elastic"),
                        Optional.of("elasticsearch"),
                        false,
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty());

        testDataset1 = generateTestDataSet1();
        testDataset2 = generateTestDataSet2();
        createIndexForResource("test1", "/elasticsearch/parent_mapping.json");
        createIndexForResource("test2", "/elasticsearch/child_mapping.json");
        createIndexDocsByName("test1", "main", testDataset1);
        createIndexDocsByName("test2", "weixin", testDataset2);
        createIndexForResource("test3", "/elasticsearch/parent_child_mapping.json");
    }

    private void createIndexForResource(String indexName, String mappingPath) throws IOException {
        String mapping =
                IOUtils.toString(
                        ContainerUtil.getResourcesFile(mappingPath).toURI(),
                        StandardCharsets.UTF_8);
        esRestClient.createIndex(indexName, mapping);
    }

    @TestTemplate
    public void testElasticsearch(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/elasticsearch/elasticsearch_source_and_sink_parent.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Container.ExecResult execResult2 =
                container.executeJob("/elasticsearch/elasticsearch_source_and_sink_child.conf");
        Assertions.assertEquals(0, execResult2.getExitCode());
    }

    private void createIndexDocsByName(String indexName, String type, Map<String, Object> testData)
            throws JsonProcessingException {
        StringBuilder requestBody = new StringBuilder();
        ObjectMapper objectMapper = new ObjectMapper();
        for (String key : testData.keySet()) {
            String indexHeader =
                    String.format(
                            "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}\n",
                            indexName, type, key);
            Object value = testData.get(key);
            String row = objectMapper.writeValueAsString(value);
            requestBody.append(indexHeader);
            requestBody.append(row);
            requestBody.append("\n");
        }
        esRestClient.bulk(requestBody.toString());
    }

    private Map<String, Object> generateTestDataSet1() {
        Map<String, Object> doc = new HashMap<>();
        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("ent_name", "企业1");
        doc1.put("ent_type", 1);
        doc.put("1001", doc1);
        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("ent_name", "企业2");
        doc2.put("ent_type", 2);
        doc.put("1002", doc2);
        return doc;
    }

    private Map<String, Object> generateTestDataSet2() {
        Map<String, Object> doc = new HashMap<>();
        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("ent_id", "1001");
        doc1.put("weixin_name", "公众号1");
        doc1.put("article_num", 35);
        doc.put("2001", doc1);
        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("ent_id", "1002");
        doc2.put("weixin_name", "公众号2");
        doc2.put("article_num", 69);
        doc.put("2002", doc2);
        return doc;
    }

    @AfterEach
    @Override
    public void tearDown() {
        if (Objects.nonNull(esRestClient)) {
            esRestClient.close();
        }
        container.close();
    }
}
