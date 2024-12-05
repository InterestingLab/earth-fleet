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

package org.apache.seatunnel.engine.server.resourcemanager.opeartion;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoad;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import lombok.SneakyThrows;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedHashMap;

/** Different from WorkerHeartbeatOperation, SystemLoad does not require frequent retrieval */
public class WorkerSystemLoadOperation extends Operation implements IdentifiedDataSerializable {

    private SystemLoad workerSystemLoad;

    public WorkerSystemLoadOperation() {
        this.workerSystemLoad = new SystemLoad();
    }

    @Override
    public void run() throws Exception {
        String currentTime =
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        SystemLoad.SystemLoadInfo systemLoadInfo = new SystemLoad.SystemLoadInfo();
        systemLoadInfo.setCpuPercentage( getCpuPercentage());
        systemLoadInfo.setMemPercentage( getMemPercentage());
        System.out.println("1111:"+systemLoadInfo);
        workerSystemLoad.setMetrics(new LinkedHashMap<>(Collections.singletonMap(currentTime,systemLoadInfo)));
    }

    public double getMemPercentage() {
        MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryMxBean.getHeapMemoryUsage();
        return (heapMemoryUsage.getUsed() / (double) heapMemoryUsage.getMax());
    }

    @SneakyThrows
    public double getCpuPercentage() {
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        CentralProcessor processor = hal.getProcessor();
        long[] prevTicks = processor.getSystemCpuLoadTicks();
        Thread.sleep(1000);
        long[] ticks = processor.getSystemCpuLoadTicks();
        long user =
                ticks[CentralProcessor.TickType.USER.getIndex()]
                        - prevTicks[CentralProcessor.TickType.USER.getIndex()];
        long nice =
                ticks[CentralProcessor.TickType.NICE.getIndex()]
                        - prevTicks[CentralProcessor.TickType.NICE.getIndex()];
        long sys =
                ticks[CentralProcessor.TickType.SYSTEM.getIndex()]
                        - prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
        long idle =
                ticks[CentralProcessor.TickType.IDLE.getIndex()]
                        - prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
        long totalCpu = user + nice + sys + idle;
        return ((totalCpu - idle) / (double) totalCpu);
    }

    @Override
    public Object getResponse() {
        return workerSystemLoad;
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ResourceDataSerializerHook.SYSTEM_LOAD_TYPE;
    }
}