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

package org.apache.seatunnel.engine.server.execution;

import java.util.concurrent.CompletableFuture;

public class TaskExecutionContext {

    // future which is Task submit
    public volatile CompletableFuture<Void> executionFuture;

    // future which can only be used to cancel the local execution.
    private volatile CompletableFuture<Void> cancellationFuture;

    public TaskExecutionContext(
        CompletableFuture<Void> executionFuture,
        CompletableFuture<Void> cancellationFuture
    ) {
        this.executionFuture = executionFuture;
        this.cancellationFuture = cancellationFuture;
    }

    public CompletableFuture<Void> cancel() {
        cancellationFuture.cancel(true);
        return executionFuture;
    }

}
