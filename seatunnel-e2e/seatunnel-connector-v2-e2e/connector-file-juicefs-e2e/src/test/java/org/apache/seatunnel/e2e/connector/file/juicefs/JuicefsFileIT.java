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

package org.apache.seatunnel.e2e.connector.file.juicefs;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.Container;

import java.io.IOException;

@Disabled("Please testing it in your local environment with juicefs conf")
public class JuicefsFileIT extends TestSuiteBase  implements TestResource {

    public static final String HADOOP_JUICEFS_DOWNLOAD =
            "https://repo1.maven.org/maven2/io/juicefs/juicefs-hadoop/1.2.1/juicefs-hadoop-1.2.1.jar";

    @BeforeAll
    @Override
    public void startUp() {
        JuicefsUtils juicefsUtils = new JuicefsUtils();
        juicefsUtils.uploadTestFiles("/excel/e2e.xlsx", "/read/excel/e2e.xlsx", true);
        juicefsUtils.uploadTestFiles("/json/e2e.json", "/read/json/e2e.json", true);
        juicefsUtils.uploadTestFiles("/orc/e2e.orc", "/read/orc/e2e.orc", true);
        juicefsUtils.uploadTestFiles("/parquet/e2e.parquet", "/read/parquet/e2e.parquet", true);
        juicefsUtils.uploadTestFiles("/text/e2e.txt", "/read/text/e2e.txt", true);
        juicefsUtils.close();
    }

    @AfterAll
    @Override
    public void tearDown() {
    }

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/juicefs/lib && cd /tmp/seatunnel/plugins/juicefs/lib && curl -O "
                                        + HADOOP_JUICEFS_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    @TestTemplate
    public void testJuicefsFileWriteAndRead(TestContainer container)
            throws IOException, InterruptedException {
        // test juicefs excel file
        Container.ExecResult excelWriteResult =
                container.executeJob("/excel/fake_to_juicefs_excel.conf");
        Assertions.assertEquals(0, excelWriteResult.getExitCode(), excelWriteResult.getStderr());
        Container.ExecResult excelReadResult =
                container.executeJob("/excel/juicefs_excel_to_assert.conf");
        Assertions.assertEquals(0, excelReadResult.getExitCode(), excelReadResult.getStderr());

        // test juicefs text file
        Container.ExecResult textWriteResult =
                container.executeJob("/text/fake_to_juicefs_file_text.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Container.ExecResult textReadResult =
                container.executeJob("/text/juicefs_file_text_to_assert.conf");
        Assertions.assertEquals(0, textReadResult.getExitCode());

        // test juicefs json file
        Container.ExecResult jsonWriteResult =
                container.executeJob("/json/fake_to_juicefs_file_json.conf");
        Assertions.assertEquals(0, jsonWriteResult.getExitCode());
        Container.ExecResult jsonReadResult =
                container.executeJob("/json/juicefs_file_json_to_assert.conf");
        Assertions.assertEquals(0, jsonReadResult.getExitCode());

        // test juicefs orc file
        Container.ExecResult orcWriteResult =
                container.executeJob("/orc/fake_to_juicefs_file_orc.conf");
        Assertions.assertEquals(0, orcWriteResult.getExitCode());
        Container.ExecResult orcReadResult =
                container.executeJob("/orc/juicefs_file_orc_to_assert.conf");
        Assertions.assertEquals(0, orcReadResult.getExitCode());

        // test juicefs parquet file
        Container.ExecResult parquetWriteResult =
                container.executeJob("/parquet/fake_to_juicefs_file_parquet.conf");
        Assertions.assertEquals(0, parquetWriteResult.getExitCode());
        Container.ExecResult parquetReadResult =
                container.executeJob("/parquet/juicefs_file_parquet_to_assert.conf");
        Assertions.assertEquals(0, parquetReadResult.getExitCode());
    }
}
