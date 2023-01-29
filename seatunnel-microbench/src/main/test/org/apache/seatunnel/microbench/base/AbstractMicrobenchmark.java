/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.seatunnel.microbench.base;

import static org.apache.seatunnel.microbench.base.AbstractMicrobenchmark.DEFAULT_TIME;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * All JMH tests need to extend this class to make it easier for you to complete JMHTest, you can also choose to
 * customize runtime conditions (Measurement, Fork, Warmup, etc.)
 * <p>
 * You can run any of the JMH tests as a normal UT, or you can package it and get all the reported results via `java
 * -jar benchmark.jar`, or get the results of a particular Test via `java -jar /benchmarks.jar exampleClassName`.
 */
@Warmup(iterations = AbstractMicrobenchmark.DEFAULT_WARMUP_ITERATIONS, time = DEFAULT_TIME)
@Measurement(iterations = AbstractMicrobenchmark.DEFAULT_MEASURE_ITERATIONS, time = DEFAULT_TIME)
@Fork(AbstractMicrobenchmark.DEFAULT_FORKS)
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@Slf4j
public class AbstractMicrobenchmark {

    static final int DEFAULT_WARMUP_ITERATIONS = 5;

    static final int DEFAULT_MEASURE_ITERATIONS = 5;

    static final int DEFAULT_TIME = 5;

    static final int DEFAULT_FORKS = 2;

    private ChainedOptionsBuilder newOptionsBuilder() {

        String className = getClass().getSimpleName();

        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
            // set benchmark class name
            .include(".*" + className + ".*")
            // add GC profiler
            .addProfiler(GCProfiler.class)
            //set jvm args
            .jvmArgsAppend("-Xmx512m", "-Xms512m", "-XX:MaxDirectMemorySize=512m",
                "-XX:BiasedLockingStartupDelay=0",
                "-Djmh.executor=CUSTOM",
                "-Djmh.executor.class=org.apache.seatunnel.microbench.base.AbstractMicrobenchmark$JmhThreadExecutor"
            );

        String output = getReportDir();
        if (output != null) {
            boolean writeFileStatus;
            String filePath = getReportDir() + className + ".json";
            File file = new File(filePath);

            if (file.exists()) {
                writeFileStatus = file.delete();
            } else {
                writeFileStatus = file.getParentFile().mkdirs();
                try {
                    writeFileStatus = file.createNewFile();
                } catch (IOException e) {
                    log.warn("jmh test create file error", e);
                }
            }
            if (writeFileStatus) {
                optBuilder.resultFormat(ResultFormatType.JSON)
                    .result(filePath);
            }
        }
        return optBuilder;
    }

    @Test
    public void run() throws Exception {
        new Runner(newOptionsBuilder().build()).run();
    }

    public static String getReportDir() {
        return System.getProperty("perfReportDir");
    }

    public static class JmhThreadExecutor extends ThreadPoolExecutor {
        @SuppressWarnings("checkstyle:MagicNumber")
        public JmhThreadExecutor(int size, String name) {
            super(size, size, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), Executors.defaultThreadFactory());
        }
    }
}
