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

package org.apache.seatunnel.core.starter.seatunnel.args;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.command.AbstractCommandArgs;
import org.apache.seatunnel.core.starter.config.EngineType;

import com.beust.jcommander.Parameter;

import java.util.List;

public class SeaTunnelCommandArgs extends AbstractCommandArgs {

    /**
     * Undefined parameters parsed will be stored here as seatunnel engine command parameters.
     */
    private List<String> seatunnelParams;

    @Parameter(names = {"-n", "--name"},
        description = "The name of job")
    private String name = "seatunnel_job";

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public EngineType getEngineType() {
        return EngineType.SEATUNNEL;
    }

    @Override
    public DeployMode getDeployMode() {
        return DeployMode.CLIENT;
    }

    public List<String> getSeatunnelParams() {
        return seatunnelParams;
    }

    public void setSeatunnelParams(List<String> seatunnelParams) {
        this.seatunnelParams = seatunnelParams;
    }
}
