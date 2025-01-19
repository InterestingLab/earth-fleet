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

package org.apache.seatunnel.connectors.seatunnel.file.juicefs.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.commons.lang3.StringUtils;

import lombok.Setter;

import java.util.HashMap;

@Setter
public class JuicefsHadoopConf extends HadoopConf {

    private static final String HDFS_IMPL = "io.juicefs.JuiceFileSystem";
    private static final String HDFS_ABSTRACT_IMPL = "io.juicefs.JuiceFS";

    private static final String JUICEFS_META_KEY = "juicefs.meta";
    private static final String DEFAULT_SCHEMA = "jfs";

    private String schema = DEFAULT_SCHEMA;

    @Override
    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    @Override
    public String getSchema() {
        return this.schema;
    }

    public JuicefsHadoopConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithConfig(ReadonlyConfig config) {
        String jfsName = config.get(JuicefsConfigOptions.JFS_NAME);
        JuicefsHadoopConf hadoopConf = new JuicefsHadoopConf(jfsName);
        String schema = jfsName.split("://")[0];
        hadoopConf.setSchema(schema);
        String remoteUser = config.get(BaseSourceConfigOptions.REMOTE_USER);
        if (StringUtils.isNotEmpty(remoteUser)) {
            hadoopConf.setRemoteUser(remoteUser);
        }
        String hdfsAbstractImplKey =
                String.format("fs.AbstractFileSystem.%s.impl", hadoopConf.getSchema());
        HashMap<String, String> juicefsOptions = new HashMap<>();
        juicefsOptions.put(hdfsAbstractImplKey, HDFS_ABSTRACT_IMPL);
        juicefsOptions.put(JUICEFS_META_KEY, config.get(JuicefsConfigOptions.META_URL));
        juicefsOptions.putAll(config.get(JuicefsConfigOptions.HADOOP_PROPERTIES));

        hadoopConf.setExtraOptions(juicefsOptions);

        return hadoopConf;
    }
}
