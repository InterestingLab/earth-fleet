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

package org.apache.seatunnel.core.starter.seatunnel.command;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.StringFormatUtils;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.JobMetricsRunner;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This command is used to execute the SeaTunnel engine job by SeaTunnel API.
 */
@Slf4j
public class ClientExecuteCommand implements Command<ClientCommandArgs> {

    private final ClientCommandArgs clientCommandArgs;

    private JobStatus jobStatus;
    private SeaTunnelClient engineClient;
    private HazelcastInstance instance;
    private ScheduledExecutorService executorService;
    private CompletableFuture<SeaTunnelClient> clientFuture;

    public ClientExecuteCommand(ClientCommandArgs clientCommandArgs) {
        this.clientCommandArgs = clientCommandArgs;
    }

    @SuppressWarnings({"checkstyle:RegexpSingleline", "checkstyle:MagicNumber"})
    @Override
    public void execute() throws CommandExecuteException {
        JobMetricsRunner.JobMetricsSummary jobMetricsSummary = null;
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        Thread hook = addShutdownHook();
        try {
            String clusterName = clientCommandArgs.getClusterName();
            if (clientCommandArgs.getMasterType().equals(MasterType.LOCAL)) {
                clusterName = creatRandomClusterName(clusterName);
                instance = createServerInLocal(clusterName);
            }
            seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            // SeaTunnel Client may start failed cause network problem, we should shut down it
            clientFuture = CompletableFuture.supplyAsync(() -> new SeaTunnelClient(clientConfig));
            engineClient = clientFuture.get(15, TimeUnit.SECONDS);
            if (clientCommandArgs.isListJob()) {
                String jobStatus = engineClient.listJobStatus();
                System.out.println(jobStatus);
            } else if (null != clientCommandArgs.getJobId()) {
                String jobState = engineClient.getJobDetailStatus(Long.parseLong(clientCommandArgs.getJobId()));
                System.out.println(jobState);
            } else if (null != clientCommandArgs.getCancelJobId()) {
                engineClient.cancelJob(Long.parseLong(clientCommandArgs.getCancelJobId()));
            } else if (null != clientCommandArgs.getMetricsJobId()) {
                String jobMetrics = engineClient.getJobMetrics(Long.parseLong(clientCommandArgs.getMetricsJobId()));
                System.out.println(jobMetrics);
            } else if (null != clientCommandArgs.getSavePointJobId()) {
                engineClient.savePointJob(Long.parseLong(clientCommandArgs.getSavePointJobId()));
            } else {
                Path configFile = FileUtils.getConfigPath(clientCommandArgs);
                checkConfigExist(configFile);
                JobConfig jobConfig = new JobConfig();
                JobExecutionEnvironment jobExecutionEnv;
                jobConfig.setName(clientCommandArgs.getJobName());
                if (null != clientCommandArgs.getRestoreJobId()) {
                    jobExecutionEnv = engineClient.restoreExecutionContext(configFile.toString(), jobConfig,
                        Long.parseLong(clientCommandArgs.getRestoreJobId()));
                } else {
                    jobExecutionEnv = engineClient.createExecutionContext(configFile.toString(), jobConfig);
                }

                // get job start time
                startTime = LocalDateTime.now();
                // create job proxy
                ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
                // register cancelJob hook
                Runtime.getRuntime().removeShutdownHook(hook);
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        log.info("run shutdown hook because get close signal");
                        shutdownHook(clientJobProxy);
                    });
                    try {
                        future.get(15, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        log.error("Cancel job failed.", e);
                    }
                    closeClient();
                    System.exit(1);
                }));
                // get job id
                long jobId = clientJobProxy.getJobId();
                JobMetricsRunner jobMetricsRunner = new JobMetricsRunner(engineClient, jobId);
                executorService = Executors.newSingleThreadScheduledExecutor();
                executorService.scheduleAtFixedRate(jobMetricsRunner, 0,
                    seaTunnelConfig.getEngineConfig().getPrintJobMetricsInfoInterval(), TimeUnit.SECONDS);
                // wait for job complete
                jobStatus = clientJobProxy.waitForJobComplete();
                // get job end time
                endTime = LocalDateTime.now();
                // get job statistic information when job finished
                jobMetricsSummary = engineClient.getJobMetricsSummary(jobId);
            }
        } catch (Exception e) {
            throw new CommandExecuteException("SeaTunnel job executed failed", e);
        } finally {
            closeClient();
            if (jobMetricsSummary != null) {
                // print job statistics information when job finished
                log.info(StringFormatUtils.formatTable(
                    "Job Statistic Information",
                    "Start Time",
                    DateTimeUtils.toString(startTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),

                    "End Time",
                    DateTimeUtils.toString(endTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),

                    "Total Time(s)",
                    Duration.between(startTime, endTime).getSeconds(),

                    "Total Read Count",
                    jobMetricsSummary.getSourceReadCount(),

                    "Total Write Count",
                    jobMetricsSummary.getSinkWriteCount(),

                    "Total Failed Count",
                    jobMetricsSummary.getSourceReadCount() - jobMetricsSummary.getSinkWriteCount()));
            }
        }
    }

    private void closeClient() {
        if (!clientFuture.isDone()) {
            clientFuture.cancel(true);
        }
        if (engineClient != null) {
            engineClient.close();
        }
        if (instance != null) {
            instance.shutdown();
        }
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private Thread addShutdownHook() {
        Thread hook = new Thread(() -> {
            log.info("run shutdown hook because get close signal");
            closeClient();
            System.exit(1);
        });
        Runtime.getRuntime().addShutdownHook(hook);
        return hook;
    }

    private HazelcastInstance createServerInLocal(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        return HazelcastInstanceFactory.newHazelcastInstance(seaTunnelConfig.getHazelcastConfig(),
            Thread.currentThread().getName(),
            new SeaTunnelNodeContext(seaTunnelConfig));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private String creatRandomClusterName(String namePrefix) {
        Random random = new Random();
        return namePrefix + "-" + random.nextInt(1000000);
    }

    private void shutdownHook(ClientJobProxy clientJobProxy) {
        if (clientCommandArgs.isCloseJob()) {
            if (jobStatus == null || !jobStatus.isEndState()) {
                log.warn("Task will be closed due to client shutdown.");
                clientJobProxy.cancelJob();
            }
        }
    }

}
