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

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import java.io.File;

public class JuicefsUtils {

    private static final String metaUrl = "mysql://xxxx:xxx@(127.0.0.1:3306)/juicefs";
    private static final String jfsName = "jfs://sealtunneltest";

    private final FileSystem jfs;

    @SneakyThrows
    public JuicefsUtils() {
        Configuration conf = new Configuration();
        conf.set("fs.jfs.impl", "io.juicefs.JuiceFileSystem");
        conf.set("juicefs.meta", metaUrl);

        Path p = new Path(jfsName);
        jfs = p.getFileSystem(conf);
    }

    @SneakyThrows
    public void uploadTestFiles(
            String filePath, String targetFilePath, boolean isFindFromResource) {
        File resourcesFile;
        if (isFindFromResource) {
            resourcesFile = ContainerUtil.getResourcesFile(filePath);
        } else {
            resourcesFile = new File(filePath);
        }
        jfs.copyFromLocalFile(new Path(resourcesFile.getAbsolutePath()), new Path(targetFilePath));
    }

    @SneakyThrows
    public void close() {
        if (jfs != null) {
            jfs.close();
        }
    }
}
