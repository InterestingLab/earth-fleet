/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.logicaldag.LogicalDagGenerator;

import java.util.ArrayList;
import java.util.List;

public class JobExecutionEnvironment {

    private static String DEFAULT_JOB_NAME = "test_st_job";

    private JobConfig jobConfig;

    private int maxParallelism = 1;

    private List<Action> actions = new ArrayList<>();

    private String jobFilePath;

    private IdGenerator idGenerator;

    public JobExecutionEnvironment(JobConfig jobConfig, String jobFilePath) {
        this.jobConfig = jobConfig;
        this.jobFilePath = jobFilePath;
        this.idGenerator = new IdGenerator();
    }

    public JobConfigParser getJobConfigParser() {
        return new JobConfigParser(jobFilePath, idGenerator);
    }

    public void addAction(List<Action> actions) {
        this.actions.addAll(actions);
    }

    public LogicalDagGenerator getLogicalDagGenerator() {
        return new LogicalDagGenerator(actions, jobConfig, idGenerator);
    }

    public List<Action> getActions() {
        return actions;
    }
}

