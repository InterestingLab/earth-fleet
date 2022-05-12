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

package org.apache.seatunnel.translation.spark.sink;

import org.apache.seatunnel.api.sink.Sink;
import org.apache.seatunnel.common.utils.SerializationUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;

import java.util.HashMap;

public class SparkSinkInjector {

    private static final String SPARK_SINK_CLASS_NAME = "org.apache.seatunnel.translation.spark.sink.SparkSink";

    public static DataStreamWriter<Row> inject(Dataset<Row> dataset, Sink<?, ?, ?, ?> sink,
                                               HashMap<String, String> configuration) {
        return dataset.writeStream().format(SPARK_SINK_CLASS_NAME).outputMode(OutputMode.Append())
                .option("configuration", SerializationUtils.objectToString(configuration)).option("sink",
                        SerializationUtils.objectToString(sink));
    }

}
