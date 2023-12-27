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

package org.apache.seatunnel.connectors.seatunnel.file.ftp.system;

/** Ftp connection mode enum. href="http://commons.apache.org/net/">Apache Commons Net</a>. */
public enum FtpConnectionModeEnum {

    /** DEFAULT */
    DEFAULT("default"),
    /** ACTIVE_LOCAL_DATA_CONNECTION_MODE */
    ACTIVE_LOCAL_DATA_CONNECTION_MODE("active_local"),

    /** PASSIVE_LOCAL_DATA_CONNECTION_MODE */
    PASSIVE_LOCAL_DATA_CONNECTION_MODE("passive_local");

    private final String mode;

    FtpConnectionModeEnum(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static FtpConnectionModeEnum fromMode(String mode) {
        for (FtpConnectionModeEnum ftpConnectionModeEnum : FtpConnectionModeEnum.values()) {
            if (ftpConnectionModeEnum.getMode().equals(mode)) {
                return ftpConnectionModeEnum;
            }
        }
        throw new IllegalArgumentException("Unknown ftp connection mode: " + mode);
    }
}
